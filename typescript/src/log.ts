// Logs of workflow code (I-14): the console, muted while a replay walks the recorded history.
import type { Shared } from "./scheduler.ts";

/** The logger of a run, `c.log`: the console's levels. */
export interface Log {
  debug(...data: unknown[]): void;
  info(...data: unknown[]): void;
  warn(...data: unknown[]): void;
  error(...data: unknown[]): void;
}

export function runLog(shared: Shared): Log {
  const level =
    (name: keyof Log) =>
    (...data: unknown[]): void => {
      if (shared.live) console[name](...data);
    };
  return { debug: level("debug"), info: level("info"), warn: level("warn"), error: level("error") };
}
