# Cadenza Service Product

## Product Definition

`@cadenza.io/service` is the distributed runtime package for Cadenza.

It includes core primitives and adds service-to-service execution, remote signaling, and metadata synchronization for observability.

## Primary Product Capabilities

1. Create named services with networked runtime behavior.
2. Expose local graph tasks to remote services (socket/REST).
3. Delegate remote execution through `DeputyTask`.
4. Sync graph and actor metadata to CadenzaDB-compatible storage.
5. Build DB-backed services using schema-driven database task generation.

## Core User Workflows

- Stand up a service process via `createCadenzaService(...)`.
- Register tasks/routines/signals exactly as in core.
- Add remote interactions using deputy tasks or global signals.
- Optionally connect to CadenzaDB for persistence/introspection.
- Use actor APIs for stateful service components (including transport internals).

## Actor Product Behavior in Service

- Actor semantics are inherited from core.
- Actor metadata is service-enriched and emitted globally for persistence.
- Actor-task associations are emitted with DB-shape field identity.
- Meta actors are supported for internal runtime components.
- Actor session durable persistence is per-actor opt-in and strict write-through by default.
- Runtime actor state remains non-persistent.
- No automatic hydration from `actor_session_state` is performed in v1.

## Configuration Surface

Common environment variables used by this repo include:

- `HTTP_PORT`
- `SECURITY_PROFILE`
- `NETWORK_MODE`
- `CADENZA_DB_ADDRESS`
- `CADENZA_DB_PORT`
- `RELATED_SERVICES`
- `DATABASE_POOL_SIZE`
- `DATABASE_ADDRESS`
- `CORS_ORIGIN`
- `IS_DOCKER`
- `CADENZA_SERVER_URL`
- `SSL_KEY_PATH`
- `SSL_CERT_PATH`
- `NODE_ENV`

## Product Boundaries

Service package does not own:

- canonical DB schema/table definitions
- DB migration authority
- cross-repo governance/workflow policy

Those belong to `cadenza-db` and workspace meta docs.

## Quality Targets

- Preserve core primitive semantics under distribution.
- Keep metadata contracts stable and DB-compatible.
- Prefer additive contract evolution across repos.
