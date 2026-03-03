# RFC: Actors as Stateful Data Interaction Primitive (MVP)

Status: Active Draft  
Last updated: 2026-03-03  
Scope: `cadenza` (core), `cadenza-service`, `cadenza-db`

## 1. Summary

Actors are Cadenza's state interaction primitive.

- Actors are **not** graph nodes.
- Actor-bound tasks are normal tasks in the graph.
- Signals trigger those tasks exactly as any other task flow.

This keeps orchestration inside existing primitives while standardizing stateful behavior.

## 2. Core Principles

1. Keep behavior inside the ecosystem.
   - Signals trigger tasks.
   - Tasks read/write actor state.
   - No hidden lifecycle hooks outside tasks.
2. Separate durable and runtime concerns.
   - Durable state is serializable and definition-driven.
   - Runtime state is process-local and task-managed.
3. Preserve graph simplicity.
   - No new graph node type for actors.
   - No mandatory naming convention for actor tasks.

## 3. MVP Goals

- Standardize stateful interactions without changing execution semantics.
- Provide a minimal local in-memory actor primitive in core.
- Reuse task/signal/inquiry/deputy primitives unchanged.
- Support optional idempotency and session behavior.
- Provide a clean base for later distributed/persistent extensions.

## 4. Non-goals (MVP)

- No actor-level distributed sync in core.
- No automatic DB definition loader in this repo (deferred to `cadenza/engine`).
- No mandatory global actor addressing API.
- No universal consistency policy enforced across all use cases.

## 5. Current API Shape (MVP)

```ts
export type ActorLoadPolicy = "eager" | "lazy";
export type ActorWriteContract = "overwrite" | "patch" | "reducer";
export type ActorTaskMode = "read" | "write" | "meta";
export type ActorKind = "standard" | "meta";

export interface ActorInvocationOptions {
  actorKey?: string;
  idempotencyKey?: string;
}

export interface ActorSpec<D extends Record<string, any>, R = Record<string, any>> {
  name: string;
  description?: string;
  defaultKey: string;
  initState?: D | (() => D);
  state?: {
    durable?: {
      initState?: D | (() => D);
      schema?: Record<string, any>;
      description?: string;
    };
    runtime?: {
      schema?: Record<string, any>;
      description?: string;
    };
  };
  keyResolver?: (input: Record<string, any>) => string | undefined;
  loadPolicy?: ActorLoadPolicy;
  writeContract?: ActorWriteContract;
  kind?: ActorKind;
  retry?: {
    attempts?: number;
    delayMs?: number;
    maxDelayMs?: number;
    factor?: number;
  };
  idempotency?: {
    enabled?: boolean;
    mode?: "required" | "optional";
    rerunOnFailedDuplicate?: boolean;
    ttlMs?: number;
  };
  session?: {
    enabled?: boolean;
    idleTtlMs?: number;
    absoluteTtlMs?: number;
    extendIdleTtlOnRead?: boolean;
  };
}
```

## 6. Initialization Model

Actor-level `init` lifecycle hooks are removed.

- Durable bootstrap:
  - Use `initState` (prefer static value; function only for computed/advanced cases).
- Runtime bootstrap:
  - Use normal write tasks that call `setRuntimeState` / `patchRuntimeState`.
  - Trigger those tasks through signals or graph flows (eager or on-demand).

This keeps initialization observable and fully expressible through primitives.

## 7. Defaults (Locked)

- Identity: static key by default, overridable via invocation `actorKey`.
- Load policy default: `eager`.
- Write contract default: `overwrite`.
- Patch contract: shallow merge.
- Idempotency: disabled by default.
- If idempotency enabled: duplicate failed executions re-run by default.
- Session touch default: `extendIdleTtlOnRead = true`.
- Per-task session touch override remains available.

## 8. Service-Layer Usage Pattern

In `cadenza-service`, actors should integrate through normal task wiring:

- Setup tasks are meta tasks subscribed to signals.
- Those tasks manage runtime state through actor write mode.
- Read paths should use actor read tasks when actor state access is required.

SocketController migration pattern:

- `Setup SocketServer` and `Connect to socket server` remain signal-triggered tasks.
- Durable/session updates go through actor write tasks.
- Runtime handles (socket instances, timers, callbacks) remain runtime state and are never persisted.
- Diagnostics reads should execute via diagnostics actor read task (not direct ad-hoc state reads).

## 9. Distributed Extension Boundary

Distributed concerns remain outside the core MVP primitive.

- Sync/reconciliation/ownership/failover meta tasks are extension behavior.
- DB-native definition materialization/auto-instantiation is deferred to `cadenza/engine`.

## 10. cadenza-db Minimal Schema (Implemented)

### 10.1 `actor`

Actor definition registry.

Core identity:

- `name`
- `service_name`
- `version`

Primary key: `(name, service_name, version)`

Core metadata:

- `description`
- `is_meta`
- `default_key`
- `load_policy`
- `write_contract`
- `runtime_read_guard`
- `consistency_profile`
- `key_definition`
- `state_definition`
- `retry_policy`
- `idempotency_policy`
- `session_policy`

System fields:

- `generated_by`
- `created`
- `deleted`

Notes:

- `kind` is intentionally removed; actor classification is `is_meta: boolean`.
- `lifecycle_definition` is intentionally removed in MVP.

### 10.2 `actor_task_map`

Task-to-actor binding metadata.

Fields:

- `actor_name`
- `actor_version`
- `task_name`
- `task_version`
- `service_name`
- `mode` (`read|write|meta`)
- `description`
- `is_meta`
- `created`
- `deleted`

Primary key:

- `(actor_name, actor_version, task_name, task_version, service_name)`

Foreign keys:

- `(actor_name, service_name, actor_version)` -> `actor(name, service_name, version)`
- `(task_name, task_version, service_name)` -> `task(name, version, service_name)`

### 10.3 `actor_session_state`

Durable actor state per resolved key.

Fields:

- `id`
- `actor_name`
- `actor_version`
- `actor_key`
- `service_name`
- `durable_state`
- `durable_version`
- `expires_at`
- `updated`
- `created`
- `deleted`

Unique constraint:

- `(actor_name, actor_version, actor_key, service_name)`

### 10.4 Metadata Signal Contracts for DB Sync

The DB sync path requires **field identity match** with DB schemas. Case style may differ during transport/normalization, but the logical field names must match.

`global.meta.graph_metadata.actor_created` data must map to `actor` fields:

- `name`
- `description`
- `service_name`
- `default_key`
- `load_policy`
- `write_contract`
- `runtime_read_guard`
- `consistency_profile`
- `key_definition`
- `state_definition`
- `retry_policy`
- `idempotency_policy`
- `session_policy`
- `is_meta`
- `version`

`global.meta.graph_metadata.actor_task_associated` data must map to `actor_task_map` fields:

- `actor_name`
- `actor_version`
- `task_name`
- `task_version`
- `service_name`
- `mode`
- `description`
- `is_meta`

Distributed sync ownership/failover tables remain out of MVP scope and are deferred.

## 11. Example (Task-Driven Runtime Init)

```ts
const sessionActor = Cadenza.createActor<
  { userId: string | null },
  { socket: unknown } | undefined
>({
  name: "SessionActor",
  defaultKey: "default",
  initState: { userId: null },
  loadPolicy: "eager",
  writeContract: "overwrite",
});

const setupRuntimeTask = Cadenza.createMetaTask(
  "SessionActor.SetupRuntime",
  sessionActor.task(({ input, setRuntimeState }) => {
    setRuntimeState({ socket: input.socketInstance });
  }, { mode: "write" }),
).doOn("meta.session.runtime_setup_requested");

const readTask = Cadenza.createTask(
  "SessionActor.Read",
  sessionActor.task(({ state, runtimeState }) => ({
    userId: state.userId,
    hasRuntime: Boolean(runtimeState?.socket),
  }), { mode: "read" }),
);
```

## 12. Decision Log

- Actors are not graph nodes; actor tasks are.
- `Cadenza.createActor(...)` is the primary creation API.
- `actor.task(...)` is the binding mechanism.
- MetaActors are supported; their bound tasks are forced meta.
- Actor-level `init` lifecycle hook was removed.
- Durable bootstrap uses `initState`.
- Runtime bootstrap is done by signal-triggered write tasks.
- Keep actor behavior inside primitives to support DB-native generation later.
- DB `actor` table uses `is_meta` instead of `kind`.
- DB sync contracts are validated by field identity, not naming style.

## 13. MVP Acceptance Criteria

### 13.1 Core (`cadenza`)

- Actor spec registration and task binding works.
- Key resolution order is deterministic:
  - explicit `actorKey`
  - `keyResolver(input)`
  - `defaultKey`
- Write contracts:
  - `overwrite`
  - `patch` (shallow)
  - `reducer` (mixed usage allowed)
- Idempotency remains optional and off by default.
- No actor-level init lifecycle API.

### 13.2 Service (`cadenza-service`)

- Actor-backed controllers use signal-triggered tasks for runtime setup/teardown.
- Runtime object handling remains in runtime state only.
- Read paths can be expressed through actor read tasks where actor state is involved.

### 13.3 DB (`cadenza-db`)

- Minimal actor tables are in place:
  - `actor`
  - `actor_task_map`
  - `actor_session_state`
- `is_meta` is used for actor type classification.
- Required actor metadata signal fields match DB schema contracts.
- Unique constraints for actor definitions and actor durable state keys are enforced.
