// The replays of a worker, each talking to the agent loop through its channel.
import type { Clock } from "./clock.ts";
import type { Core } from "./core.ts";
import type { agent, layout, run } from "./proto.ts";
import { Channel, dropped, type ReplayEvent, type ReplayHost, replay } from "./replay.ts";
import type { Workflows } from "./workflow.ts";

/** The name of a replay's task, before its run. */
export const REPLAY = "replay of ";

/** Where a replay's events go: the agent loop's inbox. */
interface Events {
  push(event: ReplayEvent): void;
}

/** The replays of a worker, each talking to the agent loop through its channel. */
export class Replays {
  readonly running: Map<string, Channel> = new Map();
  readonly #core: Core;
  readonly #workflows: Workflows;
  readonly #events: Events;
  readonly #host: ReplayHost;
  readonly #clock: Clock;

  constructor(core: Core, workflows: Workflows, events: Events, host: ReplayHost, clock: Clock) {
    this.#core = core;
    this.#workflows = workflows;
    this.#events = events;
    this.#host = host;
    this.#clock = clock;
  }

  /** Starts the replay of `chain`, whose current run is `input.run`, which logs and observers
   * name; it serves a due entry of `dueAt` if any. */
  spawn(chain: string, input: run.RunInput, dueAt: layout.Time | undefined): void {
    const start = input.start;
    // § one pjrbw31h4@32nvw
    const workflow = start && this.#workflows.get(start.workflow, start.version);
    if (workflow === undefined) {
      console.error(`substantial: replay of ${input.run}: this worker does not serve its workflow`);
      this.#events.push({ kind: "ended", chain, end: dropped() });
      return;
    }
    const second = this.running.get(chain);
    if (second !== undefined) {
      console.error(`substantial: a second replay of chain ${chain}, of run ${input.run}`);
      second.stop();
    }
    const channel = new Channel();
    this.running.set(chain, channel);
    const emit = (event: ReplayEvent) => this.#events.push(event);
    const clock = this.#clock;
    clock.spawn(
      () => replay(this.#core, workflow, chain, input, dueAt, clock, emit, channel, this.#host),
      `${REPLAY}${input.run}`,
    );
  }

  committed(c: agent.AgentCommand_Committed): void {
    const channel = this.running.get(c.run);
    if (channel === undefined)
      console.warn(`substantial: committed for chain ${c.run}, not in replay`);
    channel?.committed(c);
  }

  claimed(chain: string): void {
    const channel = this.running.get(chain);
    if (channel === undefined)
      console.warn(`substantial: claimed for chain ${chain}, not in replay`);
    channel?.claimed();
  }

  stop(chain: string): void {
    const channel = this.running.get(chain);
    if (channel === undefined) console.warn(`substantial: stop for chain ${chain}, not in replay`);
    channel?.stop();
  }

  stopAll(): void {
    for (const channel of this.running.values()) channel.stop();
  }
}
