# AGENTS, the Rust guest SDK

<silcrow decisions="docs/decisions/**/*.md" published="main" code="**/*" docs="">
This repo follows [Silcrow](https://github.com/zifeo/silcrow).
Decisions: `docs/decisions/**/*.md`. Usage: `silcrow help`.
Engineering principles:
- Locality: keep behavior, data, and decisions close to where they are used, and fix a shared defect once at its source.
- Explicitness: make inputs, outputs, dependencies, and failure modes visible.
- Economy: use the smallest mechanism that completely solves the problem, reusing existing code before adding any, and finish every caller, test, and fixture it touches.
- Stability: preserve observable behavior, including error handling and validation, across refactors unless a law changes it.
- Decisions: state each law as a business decision, in plain and abstract words a person can reason about and steer behavior with, never as a restatement of the code, and leave out any value or detail with no reason behind it.
- Verifiability: prefer deterministic behavior that can be checked directly, leave a test for new non-trivial logic, and name what was skipped or left unchecked.
- Order: make it work, then make it right, then make it fast only when a measured need or a law demands it.
</silcrow>

The crate `substantial-guest`, which writes a Substantial guest workflow in Rust. Its notes hold the SDK's own laws. The world it binds and the host that runs it are the laws of the repository's root project, which its code marks qualified with `.:`.
