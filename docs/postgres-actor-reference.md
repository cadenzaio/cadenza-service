# PostgresActor Reference

This is the canonical reference for PostgresActor in `cadenza-service`.

## 1. Canonical API

```ts
Cadenza.createPostgresActor(
  serviceName: string,
  schema: DatabaseSchemaDefinition,
  description?: string,
  options?: ServerOptions & DatabaseOptions,
): void

Cadenza.createMetaPostgresActor(
  serviceName: string,
  schema: DatabaseSchemaDefinition,
  description?: string,
  options?: ServerOptions & DatabaseOptions,
): void
```

`serviceName` drives actor identity:

- Actor name: `${serviceName}PostgresActor`
- Actor token: `kebab-case(actorName)`

## 2. Generated Artifacts

For each table `{table}` and actor token `{actor}`:

### 2.1 Generated CRUD tasks

- `Query {table}`
- `Insert {table}`
- `Update {table}`
- `Delete {table}`

### 2.2 Generated CRUD intents

- `query-pg-{actor}-{table}`
- `insert-pg-{actor}-{table}`
- `update-pg-{actor}-{table}`
- `delete-pg-{actor}-{table}`

### 2.3 Generated macro intents

- `count-pg-{actor}-{table}`
- `exists-pg-{actor}-{table}`
- `one-pg-{actor}-{table}`
- `aggregate-pg-{actor}-{table}`
- `upsert-pg-{actor}-{table}`

### 2.4 Default emitted signals

Non-meta actor:

- `global.{table}.queried`
- `global.{table}.inserted`
- `global.{table}.updated`
- `global.{table}.deleted`

Meta actor:

- `global.meta.{table}.queried`
- `global.meta.{table}.inserted`
- `global.meta.{table}.updated`
- `global.meta.{table}.deleted`

## 3. Interaction Methods

## 3.1 Task-based interaction

Use when orchestration is local and graph-driven.

```ts
Cadenza.createTask("Load hot devices", (ctx) => ({
  ...ctx,
  queryData: {
    queryMode: "aggregate",
    groupBy: ["device_id"],
    aggregates: [{ fn: "count", as: "samples" }],
    sort: { samples: "desc" },
    limit: 20,
  },
}))
  .then(Cadenza.get("Query telemetry")!)
  .doOn("global.iot.report.hot_devices_requested");
```

## 3.2 Intent-based interaction

Use when you need request/response semantics (local or distributed).

```ts
const response = await Cadenza.inquire(
  "one-pg-telemetry-service-postgres-actor-telemetry",
  {
    queryData: {
      filter: { device_id: "device-1" },
      sort: { created: "desc" },
    },
  },
  { overallTimeoutMs: 3000, requireComplete: true },
);
```

## 3.3 Signal-based interaction

Use when you need event fan-out and async fire-and-forget.

Configure in schema:

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

## 4. Schema Contract

## 4.1 `DatabaseSchemaDefinition`

| Field | Type | Required | Notes |
|---|---|---|---|
| `version` | `number` | no | Schema version metadata |
| `tables` | `Record<string, TableDefinition>` | yes | Table map |
| `relations` | relation map | no | Optional relation metadata |
| `meta.defaultEncoding` | `"utf8" \| "base64"` | no | Metadata only |
| `meta.autoIndex` | `boolean` | no | Metadata only |
| `meta.relationsVersion` | `number` | no | Metadata only |
| `meta.dropExisting` | `boolean` | no | Used by setup logic |

## 4.2 `TableDefinition`

| Field | Type | Required | Notes |
|---|---|---|---|
| `fields` | `Record<string, FieldDefinition>` | yes | Column contract |
| `meta` | object | no | Description/tags/etc |
| `indexes` | `string[][]` | no | Regular indexes |
| `uniqueConstraints` | `string[][]` | no | Unique constraints |
| `primaryKey` | `string[]` | no | Primary key |
| `fullTextIndexes` | `string[][]` | no | Metadata only in current implementation |
| `foreignKeys` | array | no | FK definitions |
| `triggers` | record | no | SQL trigger declarations |
| `customSignals` | object | no | Trigger and emission signal wiring |
| `customIntents` | object | no | Extra intent names per CRUD op |
| `initialData` | `{ fields: string[]; data: any[][] }` | no | Seed rows |

## 4.3 `FieldDefinition`

| Field | Type | Required | Notes |
|---|---|---|---|
| `type` | schema type | yes | See supported types below |
| `primary` | `boolean` | no | Primary key hint |
| `index` | `boolean` | no | Metadata/index hint |
| `unique` | `boolean` | no | Unique hint |
| `default` | `any` | no | SQL default expression/value |
| `required` | `boolean` | no | Required field |
| `nullable` | `boolean` | no | Nullable field |
| `encrypted` | `boolean` | no | Metadata only in current implementation |
| `constraints` | object | no | min/max/length/check/precision/scale/etc |
| `references` | `string` | no | Must be `table(field)` |
| `items` | `FieldDefinition` | no | Array item type metadata |
| `description` | `string` | no | Documentation |
| `generated` | `"uuid" \| "timestamp" \| "now" \| "autoIncrement"` | no | Generated behavior hints |
| `onDelete` | FK action | no | FK delete behavior |
| `onUpdate` | FK action | no | FK update behavior |

Supported field `type` values:

- `varchar`, `text`, `int`, `bigint`, `decimal`, `boolean`
- `array`, `object`, `jsonb`
- `uuid`, `timestamp`, `date`, `geo_point`, `bytea`, `any`

## 5. Payload Contract (`DbOperationPayload`)

| Field | Type | Used by | Notes |
|---|---|---|---|
| `data` | object or array | insert, update, upsert | Write payload |
| `filter` | object | query, update, delete | WHERE shape |
| `fields` | `string[]` | query | Projection |
| `joins` | `Record<string, JoinDefinition>` | query | LEFT JOIN graph |
| `queryMode` | `"rows" \| "count" \| "exists" \| "one" \| "aggregate"` | query | Query result mode |
| `aggregates` | `AggregateDefinition[]` | query(`aggregate`) | Aggregate list |
| `groupBy` | `string[]` | query(`aggregate`) | Grouping fields |
| `sort` | `Record<string, "asc" \| "desc">` | query | Ordering |
| `limit` | `number` | query | Limit |
| `offset` | `number` | query | Offset |
| `transaction` | `boolean` | write ops | Defaults true for insert/update/delete |
| `batch` | `boolean` | insert | Batch flag support |
| `onConflict` | object | insert/upsert | Conflict target/action |

## 5.1 `onConflict` contract

```ts
onConflict: {
  target: string[];
  action: {
    do: "nothing" | "update";
    set?: Record<string, any>; // "excluded" value means use excluded.<field>
    where?: string;
  };
}
```

## 6. Operation Contracts

## 6.1 Query (`query-pg-*`)

### Request

```ts
{
  queryData: {
    filter?: Record<string, any | any[]>;
    fields?: string[];
    joins?: Record<string, JoinDefinition>;
    queryMode?: "rows" | "count" | "exists" | "one" | "aggregate";
    aggregates?: AggregateDefinition[];
    groupBy?: string[];
    sort?: Record<string, "asc" | "desc">;
    limit?: number;
    offset?: number;
  }
}
```

### Response by `queryMode`

| Mode | Response keys |
|---|---|
| `rows` (default) | `{ [camelTable + "s"]: any[]; rowCount; __success }` |
| `count` | `{ count: number; rowCount: number; __success }` |
| `exists` | `{ exists: boolean; rowCount: 0\|1; __success }` |
| `one` | `{ [camelTable]: any \| null; rowCount; __success }` |
| `aggregate` | `{ aggregates: any[]; rowCount; __success }` |

## 6.2 Insert (`insert-pg-*`)

| Request keys | Response keys |
|---|---|
| `data`, optional `fields`, optional `onConflict`, optional `transaction` | `{ [camelTable or camelTable+"s"]; rowCount; __success }` |

## 6.3 Update (`update-pg-*`)

| Request keys | Response keys |
|---|---|
| `filter`, `data`, optional `transaction` | `{ [camelTable]: row \| null; rowCount; __success }` |

## 6.4 Delete (`delete-pg-*`)

| Request keys | Response keys |
|---|---|
| `filter`, optional `transaction` | `{ [camelTable]: row \| null; rowCount; __success }` |

## 6.5 Macro intents

| Intent | Behavior |
|---|---|
| `count-pg-*` | Query with `queryMode: "count"` |
| `exists-pg-*` | Query with `queryMode: "exists"` |
| `one-pg-*` | Query with `queryMode: "one"` |
| `aggregate-pg-*` | Query with `queryMode: "aggregate"` |
| `upsert-pg-*` | Insert with required `onConflict` |

## 7. Custom Signals and Custom Intents

## 7.1 `customSignals`

```ts
customSignals: {
  triggers?: {
    query?: TriggerDef[];
    insert?: TriggerDef[];
    update?: TriggerDef[];
    delete?: TriggerDef[];
  };
  emissions?: {
    query?: EmissionDef[];
    insert?: EmissionDef[];
    update?: EmissionDef[];
    delete?: EmissionDef[];
  };
}
```

Where:

- `TriggerDef = string | { signal: string; condition?: (ctx) => boolean; queryData?: DbOperationPayload }`
- `EmissionDef = string | { signal: string; condition?: (ctx) => boolean }`

## 7.2 `customIntents`

```ts
customIntents: {
  query?: IntentDef[];
  insert?: IntentDef[];
  update?: IntentDef[];
  delete?: IntentDef[];
}
```

Where:

- `IntentDef = string | { intent: string; description?: string; input?: SchemaDefinition }`

Validation guarantees:

- no spaces, dots, or backslashes in intent names
- max length 100
- duplicates rejected

## 8. Local and Distributed Usage

## 8.1 Local

Use local tasks + signals when DB actor and business logic are in same service.

## 8.2 Distributed

Use intent inquiries for cross-service response contracts:

```ts
const res = await Cadenza.inquire(
  "aggregate-pg-telemetry-service-postgres-actor-telemetry",
  {
    queryData: {
      queryMode: "aggregate",
      groupBy: ["device_id"],
      aggregates: [{ fn: "avg", field: "temperature", as: "avg_temp" }],
    },
  },
  {
    overallTimeoutMs: 4000,
    requireComplete: true,
  },
);
```

## 9. Runtime Safety and Validation

Safety behavior includes:

1. SQL identifier allowlists for table/field/sort/join keys
2. Query mode + aggregate contract validation
3. Transaction guarantees for write operations
4. Timeout handling for DB statements
5. Retry policy for transient DB failures

## 10. Environment Variables

Common runtime environment values used by PostgresActor:

- `DATABASE_ADDRESS` (required)
- `DATABASE_POOL_SIZE` (default: `10`)
- `DATABASE_STATEMENT_TIMEOUT_MS` (default: `15000`)
- `DATABASE_RETRY_DELAY_MS` (default: `100`)
- `DATABASE_RETRY_DELAY_MAX_MS` (default: `1000`)
- `DATABASE_RETRY_DELAY_FACTOR` (default: `2`)

Service/runtime values still apply (for example `HTTP_PORT`, `SECURITY_PROFILE`, `NETWORK_MODE`, `CADENZA_DB_ADDRESS`, `CADENZA_DB_PORT`).

## 11. Failure Contract

Failures return:

```ts
{
  errored: true,
  __success: false,
  __error: string,
  rowCount?: 0
}
```

Always check `__success` before consuming result data.
