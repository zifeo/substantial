import process from "node:process";
import { ConfigError, messageOf } from "../errors.ts";
import { Refused, Transient } from "./retry.ts";

export interface CredentialsInit {
  readonly accessKeyId: string;
  readonly secretAccessKey: string;
  /** For temporary credentials. */
  readonly sessionToken?: string;
}

/** Explicit S3 credentials; the secrets stay out of logs and JSON. */
export class Credentials {
  readonly accessKeyId: string;
  readonly #secretAccessKey: string;
  readonly #sessionToken: string | undefined;

  constructor(init: CredentialsInit) {
    if (typeof init?.accessKeyId !== "string" || typeof init.secretAccessKey !== "string") {
      throw new ConfigError("Credentials need an access key id and a secret access key");
    }
    if (init.accessKeyId === "" || init.secretAccessKey === "") {
      throw new ConfigError("Credentials need an access key id and a secret access key");
    }
    if (init.sessionToken !== undefined && typeof init.sessionToken !== "string") {
      throw new ConfigError("a session token is a string");
    }
    this.accessKeyId = init.accessKeyId;
    this.#secretAccessKey = init.secretAccessKey;
    this.#sessionToken = init.sessionToken === "" ? undefined : init.sessionToken;
  }

  get secretAccessKey(): string {
    return this.#secretAccessKey;
  }

  get sessionToken(): string | undefined {
    return this.#sessionToken;
  }

  toJSON(): { accessKeyId: string } {
    return { accessKeyId: this.accessKeyId };
  }
}

/** What a credentials provider returns. */
export interface ProvidedCredentials extends CredentialsInit {
  /** From 5 minutes before it, the provider is called again; keys without one never are. */
  readonly expiresAt?: Date;
}

/** Called before the first request and again before any request made less than 5 minutes
 * before the keys expire; a provider that throws fails that request only, and one that has not
 * answered after 60 s fails that attempt, which is retried. */
export type CredentialsProvider = () => ProvidedCredentials | Promise<ProvidedCredentials>;

export interface PasswordInit {
  readonly password: string;
  /** The ACL user; the default user without one. */
  readonly username?: string;
}

/** A Redis password; it stays out of logs and JSON. */
export class Password {
  readonly username: string | undefined;
  readonly #password: string;

  constructor(init: PasswordInit) {
    if (typeof init?.password !== "string" || init.password === "") {
      throw new ConfigError("a Password needs a password");
    }
    const { username } = init;
    if (username !== undefined && (typeof username !== "string" || username === "")) {
      throw new ConfigError(`a username is a non-empty string, not ${username}`);
    }
    if ("expiresAt" in init) {
      throw new ConfigError("expiresAt is for the password a provider returns");
    }
    this.username = username;
    this.#password = init.password;
  }

  get password(): string {
    return this.#password;
  }

  toJSON(): { username: string | undefined } {
    return { username: this.username };
  }
}

/** What a password provider returns. */
export interface ProvidedPassword extends PasswordInit {
  /** From 5 minutes before it, the provider is called again; a password without one never is. */
  readonly expiresAt?: Date;
}

/** Called at the first connection and again by any connection opened less than 5 minutes before
 * the password expires; connections already open keep theirs. */
export type PasswordProvider = () => ProvidedPassword | Promise<ProvidedPassword>;

/** `"env"` reads the standard variables; a store that takes none is given `undefined`. */
export type CredentialsArg =
  | Credentials
  | Password
  | "env"
  | CredentialsProvider
  | PasswordProvider
  | undefined;

const REFRESH_MS = 5 * 60_000;

export function refuse(url: string, credentials: CredentialsArg): void {
  if (credentials !== undefined) {
    throw new ConfigError(`${url} takes no credentials`);
  }
}

function required(name: string): string {
  const value = process.env[name];
  if (value === undefined || value === "") {
    throw new ConfigError(`${name} is not set`);
  }
  return value;
}

function fixed(credentials: CredentialsArg): Credentials | undefined {
  if (credentials === undefined) {
    throw new ConfigError(
      'an S3 store needs credentials, "env", new Credentials(...) or a provider',
    );
  }
  if (credentials instanceof Credentials) return credentials;
  if (typeof credentials === "function") return undefined;
  if (credentials !== "env") {
    throw new ConfigError(
      `credentials are "env", new Credentials(...) or a provider, not ${credentials}`,
    );
  }
  const sessionToken = process.env.AWS_SESSION_TOKEN;
  return new Credentials({
    accessKeyId: required("AWS_ACCESS_KEY_ID"),
    secretAccessKey: required("AWS_SECRET_ACCESS_KEY"),
    ...(sessionToken ? { sessionToken } : {}),
  });
}

/** A Redis store's password given once, explicit or read from `REDIS_PASSWORD` and
 * `REDIS_USERNAME`, the provider to ask for it, or `undefined` for a server without AUTH. */
export function resolvePassword(
  credentials: CredentialsArg,
): Password | PasswordProvider | undefined {
  if (credentials === undefined || credentials instanceof Password) return credentials;
  if (typeof credentials === "function") return credentials as PasswordProvider;
  if (credentials !== "env") {
    throw new ConfigError(
      `a Redis store takes "env", new Password(...), a provider or undefined, not ${credentials}`,
    );
  }
  const username = process.env.REDIS_USERNAME;
  return new Password({
    password: required("REDIS_PASSWORD"),
    ...(username ? { username } : {}),
  });
}

/** What a provider returned last, asked again from 5 minutes before it expires: one call at a
 * time, failing as transient after `timeoutMs`, the store's attempt timeout. */
class Refreshed<T> {
  readonly #provide: () => Promise<{ value: T; expiresAt: number | undefined }>;
  readonly #what: string;
  readonly #timeoutMs: number;
  #current: { value: T; expiresAt: number | undefined } | undefined;
  #pending: Promise<T> | undefined;

  constructor(
    provide: () => Promise<{ value: T; expiresAt: number | undefined }>,
    what: string,
    timeoutMs: number,
    given: T | undefined,
  ) {
    this.#provide = provide;
    this.#what = what;
    this.#timeoutMs = timeoutMs;
    if (given !== undefined) this.#current = { value: given, expiresAt: undefined };
  }

  /** The value `get` resolved last, and when it expires. */
  get current(): { value: T; expiresAt: number | undefined } {
    if (this.#current === undefined) throw new Error("nothing provided yet");
    return this.#current;
  }

  get(): Promise<T> {
    const c = this.#current;
    if (c !== undefined && (c.expiresAt === undefined || c.expiresAt - Date.now() >= REFRESH_MS)) {
      return Promise.resolve(c.value);
    }
    // Bounded, so that a call that never returns fails the requests waiting on it and the next
    // one calls the provider again.
    this.#pending ??= this.#bounded().finally(() => {
      this.#pending = undefined;
    });
    return this.#pending;
  }

  async #bounded(): Promise<T> {
    let timer: ReturnType<typeof setTimeout> | undefined;
    const late = new Promise<never>((_, reject) => {
      timer = setTimeout(
        () =>
          reject(
            new Transient(`credentials provider: no ${this.#what} within ${this.#timeoutMs} ms`),
          ),
        this.#timeoutMs,
      );
    });
    try {
      this.#current = await Promise.race([this.#provide(), late]);
      return this.#current.value;
    } finally {
      clearTimeout(timer);
    }
  }
}

async function ask<T>(provider: () => T | Promise<T>): Promise<T> {
  try {
    return await provider();
  } catch (e) {
    throw new Refused(`credentials provider: ${messageOf(e)}`);
  }
}

function expiry(expiresAt: unknown): number | undefined {
  if (expiresAt === undefined) return undefined;
  if (!(expiresAt instanceof Date && !Number.isNaN(expiresAt.getTime()))) {
    throw new Refused(`the credentials provider returned an invalid expiresAt: ${expiresAt}`);
  }
  return expiresAt.getTime();
}

/** The keys an S3 store signs with: explicit, read from the standard variables, or from a
 * provider. */
export class Keys extends Refreshed<Credentials> {
  constructor(credentials: CredentialsArg, timeoutMs: number) {
    const given = fixed(credentials);
    const provider = credentials as CredentialsProvider;
    super(
      async () => {
        const provided = await ask(provider);
        let value: Credentials;
        try {
          value = new Credentials(provided);
        } catch (e) {
          throw new Refused(`the credentials provider returned no keys: ${messageOf(e)}`);
        }
        return { value, expiresAt: expiry(provided.expiresAt) };
      },
      "keys",
      timeoutMs,
      given,
    );
  }
}

/** The password of a Redis store's connections, explicit or from a provider. */
export class Passwords extends Refreshed<Password> {
  constructor(credentials: Password | PasswordProvider, timeoutMs: number) {
    const provider = credentials as PasswordProvider;
    super(
      async () => {
        const provided = await ask(provider);
        let value: Password;
        try {
          const { password, username } = provided;
          value = new Password({ password, ...(username === undefined ? {} : { username }) });
        } catch (e) {
          throw new Refused(`the credentials provider returned no password: ${messageOf(e)}`);
        }
        return { value, expiresAt: expiry(provided.expiresAt) };
      },
      "password",
      timeoutMs,
      credentials instanceof Password ? credentials : undefined,
    );
  }
}
