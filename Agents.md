# Agents Notes: cadenza-service

## What I have learned

- `CadenzaService` wraps core primitives and adds service-level concerns (service registry, transport, database task/deputy abstractions, distributed inquiry behavior).
- The runtime ecosystem still depends on primitive flow:
  - signals trigger tasks
  - tasks execute work
  - intents/inquiries call responders
  - actors should provide formalized state access for tasks
- Actor API is exposed at service layer through:
  - `CadenzaService.createActor(...)`
  - `CadenzaService.createActorFromDefinition(...)`

## SocketController findings (critical for refactor direction)

- The pre-migration version (HEAD) uses pure meta-task setup flow:
  - `Setup SocketServer` (inside `SocketServer` meta routine) triggered by `global.meta.rest.network_configured`
  - `Connect to socket server` triggered by `meta.fetch.handshake_complete`
  - dynamic per-connection tasks created inside setup (`handshake`, `delegate`, `transmit`, `shutdown`)
- The current migration introduced actors but still executes many actor-task wrappers directly (manual invocation helpers), which bypasses normal graph registration/discovery semantics.
- Current file also contains migration artifacts, e.g. invalid identifier `socketCli.entSessionActor`, and expanded complexity that obscures the primitive mapping.
- The desired model from discussion is:
  - actor replaces old setup-state container
  - durable actor bootstrap is declarative (`initState`) while runtime setup is done by normal write tasks triggered by signals/flows
  - actor tasks are ordinary tasks in GraphRegistry (no new discoverability convention)

## Design constraints confirmed in discussion

- Actors are not graph nodes; actor tasks are.
- Runtime objects (socket instances, clients, handles) belong in runtime state, not durable persisted state.
- Durable state should be explicit and serializable.
- For now, runtime state persistence is strict no-write.
- Primitive descriptions should be used heavily (`description` on actors/tasks/intents).
- Runtime-created tasks are valid and intentional for complex workflows.
- Ephemeral tasks as promise resolvers are part of the intended primitive-native orchestration style.

## Latest lessons (DB-native shift)

- Patterns that feel convenient in file-based code can become counterproductive for DB-native primitive generation.
- Keep logic inside the primitive ecosystem: state reads/writes should happen through actor-bound tasks, not ad-hoc controller state access.
- Initialization should be modeled as graph behavior (signals -> tasks), not hidden lifecycle hooks.
- Runtime state should be explicitly task-managed and recreated as needed; durable state should remain explicit and serializable.
- Designing this way now reduces migration friction when actors/tasks/signals move from repo-defined code to DB-defined structures.

## Long-term direction (recorded)

- Business logic should eventually live primarily as DB-stored primitives (tasks/signals/intents/actors/agents), generated and managed through UI + AI-assisted flows.
- Runtime engines are intended to be generalized executors that materialize DB definitions into active graphs and continuously sync runtime metrics/state.

## What I will keep learning in this discussion

- Exact 1:1 mapping from each old socket setup step to actor durable bootstrap and runtime task responsibilities.
- Minimal actor task surface needed for server/client without duplicating legacy patterns.
- How to keep dynamic signal subscriptions compatible with actor-centered state ownership.
