# Cadenza Service Architecture

## Scope

`@cadenza.io/service` extends core primitives into a distributed service runtime.

It adds:

- service lifecycle and registry
- REST/socket transport
- remote task delegation (`DeputyTask`)
- graph metadata fan-out for persistence/observability
- database task abstractions, schema-driven Postgres actors, and database-service wrappers

## Layered Architecture

- Core bridge: [`src/Cadenza.ts`](/Users/emilforsvall/WebstormProjects/cadenza-workspace/cadenza-service/src/Cadenza.ts) wraps and forwards `@cadenza.io/core` APIs.
- Service registry: [`src/registry/ServiceRegistry.ts`](/Users/emilforsvall/WebstormProjects/cadenza-workspace/cadenza-service/src/registry/ServiceRegistry.ts) tracks instances, deputies, remote intents/signals, readiness/runtime state.
- Network controllers:
  - [`src/network/RestController.ts`](/Users/emilforsvall/WebstormProjects/cadenza-workspace/cadenza-service/src/network/RestController.ts)
  - [`src/network/SocketController.ts`](/Users/emilforsvall/WebstormProjects/cadenza-workspace/cadenza-service/src/network/SocketController.ts)
- Metadata controllers:
  - [`src/graph/controllers/GraphMetadataController.ts`](/Users/emilforsvall/WebstormProjects/cadenza-workspace/cadenza-service/src/graph/controllers/GraphMetadataController.ts)
  - [`src/graph/controllers/GraphSyncController.ts`](/Users/emilforsvall/WebstormProjects/cadenza-workspace/cadenza-service/src/graph/controllers/GraphSyncController.ts)
- Database abstractions:
  - [`src/database/DatabaseController.ts`](/Users/emilforsvall/WebstormProjects/cadenza-workspace/cadenza-service/src/database/DatabaseController.ts)
  - [`src/graph/definition/DatabaseTask.ts`](/Users/emilforsvall/WebstormProjects/cadenza-workspace/cadenza-service/src/graph/definition/DatabaseTask.ts)

## Boot Flow

1. `Cadenza.bootstrap()` initializes core runtime.
2. Service bootstrap wires singleton controllers (`SignalController`, `RestController`, `SocketController`, `GraphMetadataController`, `GraphSyncController`).
3. `createCadenzaService(...)` registers service identity and transport surface.
4. Meta flows sync local graph metadata and runtime status to the wider system.

## PostgresActor vs Database Service

- `createPostgresActor(...)` is actor-only:
  - creates the specialized actor
  - bootstraps the Postgres pool/schema
  - generates CRUD tasks and intents
  - does not create a network service
- `createDatabaseService(...)` is the higher-level wrapper:
  - creates the PostgresActor first
  - waits for actor setup readiness
  - then creates the actual service and bridge metadata signal

This separation allows multiple Postgres actors inside one service process while preserving the common dedicated database-service bootstrap helper.

## Actor Integration in Service Runtime

Service re-exports core actor APIs:

- `Cadenza.createActor(...)`
- `Cadenza.createActorFromDefinition(...)`

Actor metadata path:

1. Core actor emits `meta.actor.created`.
2. Core actor-bound task emits `meta.actor.task_associated`.
3. Core actor durable writes (opt-in) inquire intent `meta-actor-session-state-persist`.
4. `GraphMetadataController` enriches with `service_name` and emits:
   - `global.meta.graph_metadata.actor_created`
   - `global.meta.graph_metadata.actor_task_associated`
5. Service intent responder persists `actor_session_state` using CadenzaDB upsert with durable-version stale-write guard.
6. DB insert tasks (from sync/controller flows) persist actor metadata into `actor` and `actor_task_map`.

## Socket Transport as Actor-Backed Runtime

`SocketController` now models socket state through actors:

- `SocketServerActor` for server session/runtime state
- `SocketClientActor` for client session/runtime state
- `SocketClientDiagnosticsActor` for diagnostics snapshots

This keeps transport orchestration inside Cadenza primitives and task flows while allowing runtime objects to live in actor runtime state.

## Metadata and Sync Paths

Two complementary mechanisms are active:

1. Event path: immediate metadata emission through `GraphMetadataController` on primitive events.
2. Sync path: bootstrap/full-sync reconciliation through `GraphSyncController` for tasks/routines/signals/actors and mappings.

The combined model supports eventual consistency after restart and near-real-time updates during runtime.

## Distributed Inquiry and Readiness

Service inquiry resolution includes responder ranking/filtering, meta-intent safety checks, timeout handling, merged results, and summary metadata.

Runtime status/readiness is tracked by `ServiceRegistry` using heartbeat/state aggregation utilities.

## Boundaries

Service owns transport/distribution behavior, but does not define authoritative DB table contracts. DB schema authority remains in `cadenza-db`.
