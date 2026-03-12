# PostgresActor Guide

This guide is for end users building data flows with the canonical Postgres actor API in `cadenza-service`.

It covers:

- How to define schemas
- How CRUD and macro operations work
- How to interact through tasks, intents, and signals
- When to use each interaction method
- Local and distributed usage patterns

## 1. Mental Model

`PostgresActor` is a specialized actor plugin:

- Durable actor state: config/setup/health snapshots
- Runtime actor state: live `pg.Pool` and runtime handles
- Auto-generated DB tasks per table (`query|insert|update|delete`)
- Auto-generated actor-scoped intents for query and write flows
- No service bootstrap side effects

Canonical creation APIs:

- `Cadenza.createPostgresActor(...)`
- `Cadenza.createMetaPostgresActor(...)`

Dedicated database service wrappers:

- `Cadenza.createDatabaseService(...)`
- `Cadenza.createMetaDatabaseService(...)`

Use the actor API when you want Postgres-backed data access inside an existing service or process. Use the database-service wrapper when you want the common pattern of "create actor first, then expose it as a service".

## 2. Quick Start

```ts
import Cadenza from "@cadenza.io/service";
import type { DatabaseSchemaDefinition } from "@cadenza.io/service/src/types/database";

const schema: DatabaseSchemaDefinition = {
  version: 1,
  tables: {
    telemetry: {
      fields: {
        uuid: { type: "uuid", primary: true, required: true },
        device_id: { type: "varchar", required: true, constraints: { maxLength: 128 } },
        temperature: { type: "decimal", required: true, constraints: { precision: 10, scale: 2 } },
        created: { type: "timestamp", required: true, default: "NOW()" },
      },
      indexes: [["device_id"], ["created"]],
      customSignals: {
        emissions: {
          insert: ["global.iot.telemetry.ingested"],
        },
      },
    },
  },
};

Cadenza.createPostgresActor(
  "TelemetryReadModel",
  schema,
  "Stores telemetry inside an existing service",
  {
    databaseName: "telemetry_read_model",
  },
);
```

What this gives you for table `telemetry`:

- Tasks: `Query telemetry`, `Insert telemetry`, `Update telemetry`, `Delete telemetry`
- Intents:
  - `query-pg-telemetry-read-model-postgres-actor-telemetry`
  - `insert-pg-telemetry-read-model-postgres-actor-telemetry`
  - `update-pg-telemetry-read-model-postgres-actor-telemetry`
  - `delete-pg-telemetry-read-model-postgres-actor-telemetry`
- Macro intents:
  - `count-pg-telemetry-read-model-postgres-actor-telemetry`
  - `exists-pg-telemetry-read-model-postgres-actor-telemetry`
  - `one-pg-telemetry-read-model-postgres-actor-telemetry`
  - `aggregate-pg-telemetry-read-model-postgres-actor-telemetry`
  - `upsert-pg-telemetry-read-model-postgres-actor-telemetry`

## 2.1 Dedicated database service helper

```ts
Cadenza.createDatabaseService(
  "TelemetryDatabaseService",
  schema,
  "Dedicated telemetry data access layer",
  {
    port: 3010,
    networkMode: "dev",
    securityProfile: "medium",
    databaseName: "telemetry_service",
  },
);
```

This wrapper creates the PostgresActor first and only creates the service after actor setup completes.

## 3. Choosing Tasks vs Intents vs Signals

| Method | Best for | Return value | Coupling |
|---|---|---|---|
| Task chaining (`then`, graph flow) | Local deterministic workflows inside one service | Direct context flow | Tight (graph-local) |
| Intent inquiry (`Cadenza.inquire`) | Request/response queries and effect calls, local or distributed | Yes (merged inquiry response) | Loose (contract by intent name) |
| Signal emission (`emit` / `doOn`) | Event-driven fan-out, async triggers, notifications | No request/response contract | Loosest (pub/sub) |

Use this default:

1. Use **tasks** for local orchestration inside a service.
2. Use **intents** when you need a response, especially across services.
3. Use **signals** for fire-and-forget event fan-out.

## 4. Schema Design

## 4.1 Naming Rules

For SQL identifiers (database names, table names, field names):

- Use lowercase snake_case.
- Allowed regex: `^[a-z_][a-z0-9_]*$`.

For references:

- Use `table(field)` format.

## 4.2 Field Types

Supported `FieldDefinition.type` values:

- `varchar`, `text`, `int`, `bigint`, `decimal`, `boolean`
- `array`, `object`, `jsonb`
- `uuid`, `timestamp`, `date`, `geo_point`, `bytea`, `any`

## 4.3 Field Options

Common options:

- `primary`, `required`, `nullable`, `unique`, `default`
- `constraints` (`min`, `max`, `pattern`, `maxLength`, `precision`, `scale`, `check`, ...)
- `references`, `onDelete`, `onUpdate`
- `generated` (`uuid`, `timestamp`, `now`, `autoIncrement`)

## 4.4 Table Options

Common table-level options:

- `indexes`, `uniqueConstraints`, `primaryKey`
- `foreignKeys`
- `triggers`
- `initialData`
- `customSignals`
- `customIntents`

## 5. CRUD Operations

Each table gets auto-generated operations:

1. `query`
2. `insert`
3. `update`
4. `delete`

Each operation can be called via intent or by running the generated task in graph flows.

### 5.1 Query Example

```ts
const result = await Cadenza.inquire(
  "query-pg-telemetry-service-postgres-actor-telemetry",
  {
    queryData: {
      filter: { device_id: "device-1" },
      fields: ["uuid", "device_id", "temperature", "created"],
      sort: { created: "desc" },
      limit: 100,
    },
  },
);
```

### 5.2 Insert Example

```ts
const result = await Cadenza.inquire(
  "insert-pg-telemetry-service-postgres-actor-telemetry",
  {
    queryData: {
      data: {
        uuid: "f96aeb52-37ad-4018-a9a2-199f2a274d77",
        device_id: "device-1",
        temperature: 24.4,
        created: "2026-03-09T10:00:00.000Z",
      },
    },
  },
);
```

### 5.3 Update Example

```ts
await Cadenza.inquire("update-pg-telemetry-service-postgres-actor-telemetry", {
  queryData: {
    filter: { uuid: "f96aeb52-37ad-4018-a9a2-199f2a274d77" },
    data: { temperature: 25.0 },
  },
});
```

### 5.4 Delete Example

```ts
await Cadenza.inquire("delete-pg-telemetry-service-postgres-actor-telemetry", {
  queryData: {
    filter: { uuid: "f96aeb52-37ad-4018-a9a2-199f2a274d77" },
  },
});
```

## 6. Query Modes and Macro Intents

`query` supports `queryMode`:

- `rows` (default)
- `count`
- `exists`
- `one`
- `aggregate`

Macro intents are thin wrappers over these common modes.

### 6.1 `count`

```ts
const res = await Cadenza.inquire(
  "count-pg-telemetry-service-postgres-actor-telemetry",
  {
    queryData: {
      filter: { device_id: "device-1" },
    },
  },
);
// res.count
```

### 6.2 `exists`

```ts
const res = await Cadenza.inquire(
  "exists-pg-telemetry-service-postgres-actor-telemetry",
  {
    queryData: {
      filter: { device_id: "device-1" },
    },
  },
);
// res.exists
```

### 6.3 `one`

```ts
const res = await Cadenza.inquire(
  "one-pg-telemetry-service-postgres-actor-telemetry",
  {
    queryData: {
      filter: { device_id: "device-1" },
      sort: { created: "desc" },
    },
  },
);
// res.telemetry
```

### 6.4 `aggregate` + `groupBy`

```ts
const res = await Cadenza.inquire(
  "aggregate-pg-telemetry-service-postgres-actor-telemetry",
  {
    queryData: {
      queryMode: "aggregate",
      groupBy: ["device_id"],
      aggregates: [
        { fn: "count", as: "sample_count" },
        { fn: "avg", field: "temperature", as: "avg_temperature" },
        { fn: "max", field: "temperature", as: "max_temperature" },
      ],
      sort: { sample_count: "desc" },
    },
  },
);
// res.aggregates
```

### 6.5 `upsert`

```ts
await Cadenza.inquire("upsert-pg-telemetry-service-postgres-actor-telemetry", {
  queryData: {
    data: {
      uuid: "f96aeb52-37ad-4018-a9a2-199f2a274d77",
      device_id: "device-1",
      temperature: 26.1,
      created: "2026-03-09T10:02:00.000Z",
    },
    onConflict: {
      target: ["uuid"],
      action: {
        do: "update",
        set: {
          temperature: "excluded",
          created: "excluded",
        },
      },
    },
  },
});
```

## 7. Signals and Task Wiring

## 7.1 Default Signals (emitted by generated CRUD tasks)

For non-meta actors:

- `global.{table}.queried`
- `global.{table}.inserted`
- `global.{table}.updated`
- `global.{table}.deleted`

For meta actors:

- `global.meta.{table}.queried`
- `global.meta.{table}.inserted`
- `global.meta.{table}.updated`
- `global.meta.{table}.deleted`

## 7.2 Custom Signals in Schema

You can define operation-scoped triggers and emissions:

```ts
customSignals: {
  triggers: {
    insert: [{ signal: "global.iot.telemetry.received" }],
  },
  emissions: {
    insert: [{ signal: "global.iot.telemetry.ingested" }],
  },
}
```

You can also add:

- `condition(ctx) => boolean`
- `queryData` merge payload (for trigger-driven defaults)

## 8. Local Pattern

Local service workflow using tasks and signals:

```ts
const normalizeTask = Cadenza.createTask("Normalize telemetry", (ctx) => {
  return {
    ...ctx,
    queryData: {
      data: {
        uuid: ctx.uuid,
        device_id: ctx.deviceId,
        temperature: ctx.temperature,
        created: ctx.created,
      },
    },
  };
}).then(Cadenza.get("Insert telemetry")!);

normalizeTask.doOn("global.iot.telemetry.received");
```

Use this when:

- Producer and DB actor are in the same service
- You want explicit graph-level orchestration

## 9. Distributed Pattern

Cross-service request/response through intents:

```ts
Cadenza.createTask("Fetch latest telemetry", async (ctx) => {
  const response = await Cadenza.inquire(
    "one-pg-telemetry-service-postgres-actor-telemetry",
    {
      queryData: {
        filter: { device_id: ctx.deviceId },
        sort: { created: "desc" },
      },
    },
    { overallTimeoutMs: 3000, requireComplete: true },
  );

  return {
    ...ctx,
    telemetry: response.telemetry ?? null,
  };
}).doOn("global.iot.readings.latest_requested");
```

Use this when:

- You need a response contract from another service
- You want service-agnostic addressing through intent names

## 10. Error Contract

All operations return `__success`.

Failure responses include:

- `errored: true`
- `__error: string`
- operation-specific defaults (`rowCount: 0`, etc.)

Recommended pattern:

```ts
const res = await Cadenza.inquire("count-pg-telemetry-service-postgres-actor-telemetry", {
  queryData: { filter: { device_id: "device-1" } },
});

if (!res.__success) {
  throw new Error(res.__error ?? "Database operation failed");
}
```

## 11. Common Pitfalls

1. `count` with joins counts joined rows, not always logical entities.
   - Use `aggregate` + distinct counting strategy when needed.
2. `aggregate` mode requires `aggregates[]`.
3. `groupBy` and `aggregates` are only valid when `queryMode: "aggregate"`.
4. `upsert` requires `onConflict`.
5. SQL identifiers must be snake_case-safe.

## 12. Production Checklist

1. Enforce naming conventions (`snake_case` for identifiers).
2. Define proper `indexes` and `uniqueConstraints`.
3. Use explicit intents for cross-service access.
4. Use signal fan-out for async event processing.
5. Set environment values for timeout/retry policy (`DATABASE_*`).
6. Keep contract tests for payloads and intent names.
