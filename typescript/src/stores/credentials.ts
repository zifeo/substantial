import process from "node:process";
import { ConfigError, messageOf } from "../errors.ts";
import { Refused } from "./retry.ts";

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
 * before the keys expire; a provider that throws fails that request only. */
export type CredentialsProvider = () => ProvidedCredentials | Promise<ProvidedCredentials>;

/** `"env"` reads the standard variables; a store that takes none is given `undefined`. */
export type CredentialsArg = Credentials | "env" | CredentialsProvider | undefined;

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

/** The keys an S3 store signs with: explicit, read from the standard variables, or from a
 * provider. */
export class Keys {
  readonly #provider: CredentialsProvider | undefined;
  #current: { credentials: Credentials; expiresAt: number | undefined } | undefined;
  #pending: Promise<Credentials> | undefined;

  constructor(credentials: CredentialsArg) {
    const given = fixed(credentials);
    if (given === undefined) {
      this.#provider = credentials as CredentialsProvider;
    } else {
      this.#current = { credentials: given, expiresAt: undefined };
    }
  }

  /** The keys for a request about to be made. */
  get(): Promise<Credentials> {
    const c = this.#current;
    if (c !== undefined && (c.expiresAt === undefined || c.expiresAt - Date.now() >= REFRESH_MS)) {
      return Promise.resolve(c.credentials);
    }
    this.#pending ??= this.#provide().finally(() => {
      this.#pending = undefined;
    });
    return this.#pending;
  }

  async #provide(): Promise<Credentials> {
    let given: ProvidedCredentials;
    try {
      given = await (this.#provider as CredentialsProvider)();
    } catch (e) {
      throw new Refused(`credentials provider: ${messageOf(e)}`);
    }
    let credentials: Credentials;
    try {
      credentials = new Credentials(given);
    } catch (e) {
      throw new Refused(`the credentials provider returned no keys: ${messageOf(e)}`);
    }
    const { expiresAt } = given;
    if (
      expiresAt !== undefined &&
      !(expiresAt instanceof Date && !Number.isNaN(expiresAt.getTime()))
    ) {
      throw new Refused(`the credentials provider returned an invalid expiresAt: ${expiresAt}`);
    }
    this.#current = { credentials, expiresAt: expiresAt?.getTime() };
    return credentials;
  }
}
