# Repo-Specific Agent Rules

This document defines repository-level execution rules.

Global workflow governance (WIP limits, clarification protocol,
assumptions policy, complexity gate, contract governance)
is defined in the workspace root AGENTS.md.

If conflict exists:
- Root AGENTS.md governs workflow and process.
- This file governs tooling, commands, and repo-specific constraints.

---

# 1. Repository Overview

Name: cadenza-service
Purpose: Distributed/service extension layer for Cadenza core.
Owner Domain: Service transport, sync metadata, distribution integration contracts.

Boundaries:
- Do NOT modify other repos from here.
- Cross-repo changes must follow workspace multi-repo discipline.
- Keep core primitive semantics in `cadenza`; keep DB schema authority in `cadenza-db`.

---

# 2. Local Development Commands

Use these canonical commands. Do not invent alternatives.

## Install

```bash
yarn install
```

## Build

```bash
yarn build
```

## Test

```bash
yarn test
```

## Typecheck

```bash
yarn tsc --noEmit
```

## Format

```bash
yarn prettier --check .
```

If CI uses a specific command, prefer that command.

# 3. Pre-PR Checklist (Repo-Specific)

## Before opening PR:

- [ ] Install succeeds
- [ ] Typecheck passes
- [ ] Tests pass
- [ ] No console logs left
- [ ] No commented-out code
- [ ] No debug artifacts
- [ ] Migration files included (if applicable)

If this repo exposes contracts:

- [ ] Contract changes propagated per workspace rules

# 4. Environment & Configuration

Required environment variables (from source usage):

- `HTTP_PORT`: Local HTTP server port.
- `SECURITY_PROFILE`: Service security profile (`low|medium|high`).
- `NETWORK_MODE`: Service network mode (`internal|exposed|exposed-high-sec|auto|dev`).
- `CADENZA_DB_ADDRESS`: CadenzaDB service host/address.
- `CADENZA_DB_PORT`: CadenzaDB service port.
- `RELATED_SERVICES`: Pipe-separated service bootstrap list.
- `DATABASE_POOL_SIZE`: DB pool size for database service setup.
- `DATABASE_ADDRESS`: Database connection string.
- `CORS_ORIGIN`: CORS allowlist origin.
- `IS_DOCKER`: Docker runtime flag (`true`/`false`).
- `CADENZA_SERVER_URL`: Public service URL hint when containerized.
- `SSL_KEY_PATH`: HTTPS key file path.
- `SSL_CERT_PATH`: HTTPS cert file path.
- `NODE_ENV`: Runtime environment (`development`/`production`).

Local dev setup notes:

- Default local service bootstrap works without TLS.
- DB-backed flows require reachable DB/CadenzaDB endpoints.

Never hardcode secrets.

Never commit .env files.

# 5. Testing Rules

Test expectations:

- All new logic must include tests.
- Edge cases must be tested.
- Regression tests required for bug fixes.
- Snapshot tests updated intentionally, never blindly.

If integration tests exist:

- Ensure external services are mocked or containerized.

# 6. Contract Responsibilities (If This Repo Owns Contracts)

This repo owns service/distribution integration contracts.

- Update authority source first.
- Keep metadata signal payload field identity aligned with `cadenza-db` schemas.
- Update or notify consumers in same task OR create follow-up issue.
- Add/update tests that lock payload contract behavior.

Breaking contract changes require:

- Explicit approval

  OR

- Design-required phase.

# 7. Logging & Observability

- Use structured logging.
- Avoid logging sensitive data.
- Log errors with context.
- No silent catches.

# 8. Performance & Safety Constraints

- Avoid unbounded loops over incoming network input.
- Validate external input and metadata payloads.
- Keep retry/delegation logic bounded and observable.
- Fail fast on invalid states.

# 9. Repo-Specific Anti-Patterns

Do NOT:

- Bypass primitive flow with hidden side channels.
- Call `task.execute(...)` directly outside the internal graph runner/runtime.
- Read/write actor state outside actor-bound tasks when actor context is required.
- Persist runtime-only objects.
- Modify generated files manually.
- Disable tests to make builds pass.
- Introduce new dependencies without justification.

# 10. Documentation Discipline

If you modify:

- Public API
- DB sync payload contracts
- Build system
- Major module structure

Update:

- README.md
- This AGENTS.md (if command/process changes)
- Relevant repo docs (including actor RFC when applicable)

All documentation changes must be:

- Evidence-based
- Implemented via approved proposals from Queue Health process

# 11. Execution Principle

Within this repo:

- Prefer small, incremental changes.
- Prefer additive changes over breaking.
- If uncertain, trigger clarification per root policy.
- If complexity increases, trigger design-required per root policy.
- Evolve service behavior mainly by composing Cadenza primitives rather than imperative controller chains.
- Prefer flat task and signal graphs over deep nested helper-function call graphs.
- Use signals as fire-and-forget detach points when no response contract is required.
- Reuse tasks across multiple flows by cloning tasks when the task behavior is unchanged.
- Keep helper functions narrowly scoped to repeated low-level work, normalization, or hot paths where primitive-only modeling would be inefficient.
- When a task must be reused in another flow, wire it through signals, inquiries, routines, or cloned task graphs rather than imperative task invocation.

When in doubt: stop and ask.

# Agents Notes: cadenza-service

## What I have learned

- `CadenzaService` wraps core primitives and adds service-level concerns (service registry, transport, database abstractions, distributed inquiry behavior).
- Primitive flow remains central:
  - signals trigger tasks
  - tasks execute work
  - intents/inquiries resolve task responders
  - actors provide formalized state access for tasks
- Actor API is exposed at service layer via:
  - `CadenzaService.createActor(...)`
  - `CadenzaService.createActorFromDefinition(...)`

## SocketController alignment

- Socket setup remains signal-driven task orchestration:
  - `Setup SocketServer` triggered by `global.meta.rest.network_configured`
  - `Connect to socket server` triggered by `meta.fetch.handshake_complete`
- Actor-backed model:
  - durable actor state for serializable session/diagnostic data
  - runtime actor state for live socket/timer handles
- Runtime-only objects are not persisted.

## Actor/DB sync alignment

- Actor metadata and actor-task association are treated as DB-sync contracts.
- Field identity must match DB schemas; case conversion is transport-level.
- Graph metadata tasks enrich actor payloads with `service_name` before DB-facing global meta signals.

## Long-term direction (recorded)

- Business logic is intended to move toward DB-native primitives authored/generated outside static files.
- Service runtime remains a materialization/execution layer that syncs metadata and runtime telemetry.

## What I will keep learning in this discussion

- Actor session persistence strategy (`actor_session_state`) with clear durable/runtime boundaries.
- Conflict/consistency behavior for actor durable state hydration and write-back.
- Minimal metadata contracts needed for engine-driven DB-native materialization.
