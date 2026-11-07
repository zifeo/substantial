// The replays of a worker, each talking to the agent loop through its channel.
import type { Clock } from "./clock.ts";
import type { Core } from "./core.ts";
import type { agent, run } from "./proto.ts";
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

  spawn(runId: string, input: run.RunInput): void {
    const start = input.start;
    const workflow = start && this.#workflows.get(start.workflow, start.version);
    if (workflow === undefined) {
      console.error(`substantial: replay of ${runId}: this worker does not serve its workflow`);
      this.#events.push({ kind: "ended", run: runId, end: dropped() });
      return;
    }
    if (this.running.has(runId)) console.error(`substantial: a second replay of ${runId}`);
    const channel = new Channel();
    this.running.set(runId, channel);
    const emit = (event: ReplayEvent) => this.#events.push(event);
    const clock = this.#clock;
    clock.spawn(
      () => replay(this.#core, workflow, input, clock, emit, channel, this.#host),
      `${REPLAY}${runId}`,
    );
  }

  committed(c: agent.AgentCommand_Committed): void {
    const channel = this.running.get(c.run);
    if (channel === undefined) console.warn(`substantial: committed for ${c.run}, not in replay`);
    channel?.committed(c);
  }

  stop(runId: string): void {
    const channel = this.running.get(runId);
    if (channel === undefined) console.warn(`substantial: stop for ${runId}, not in replay`);
    channel?.stop();
  }

  stopAll(): void {
    for (const channel of this.running.values()) channel.stop();
  }
}
