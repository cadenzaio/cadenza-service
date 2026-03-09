import {
  DbOperationType,
  FieldDefinition,
  SCHEMA_TYPES,
  DatabaseSchemaDefinition,
  TableDefinition,
} from "../types/database";
import Cadenza, { DatabaseOptions, ServerOptions } from "../Cadenza";
import { Pool, PoolClient } from "pg";
import { camelCase, kebabCase, snakeCase } from "lodash-es";
import type { Actor, AnyObject, Schema, SchemaDefinition } from "@cadenza.io/core";
import {
  AggregateDefinition,
  DbOperationPayload,
  JoinDefinition,
  OnConflictAction,
  QueryMode,
  SubOperation,
} from "../types/queryData";

export interface OperationIntentDefinition {
  name: string;
  description: string;
  input: SchemaDefinition;
}

export interface OperationIntentResolution {
  intents: OperationIntentDefinition[];
}

interface PostgresActorSafetyPolicy {
  statementTimeoutMs: number;
  retryCount: number;
  retryDelayMs: number;
  retryDelayMaxMs: number;
  retryDelayFactor: number;
}

interface PostgresActorDurableState {
  actorName: string;
  actorToken: string;
  serviceName: string;
  databaseName: string;
  status: "idle" | "initializing" | "ready" | "error";
  schemaVersion: number;
  setupStartedAt: string | null;
  setupCompletedAt: string | null;
  lastHealthCheckAt: string | null;
  lastError: string | null;
  tables: string[];
  safetyPolicy: PostgresActorSafetyPolicy;
}

interface PostgresActorRuntimeState {
  pool: Pool | null;
  ready: boolean;
  pendingQueries: number;
  lastHealthCheckAt: number | null;
  lastError: string | null;
}

interface PostgresActorRegistration {
  serviceName: string;
  databaseName: string;
  actorName: string;
  actorToken: string;
  actorKey: string;
  actor: Actor<PostgresActorDurableState, PostgresActorRuntimeState>;
  schema: DatabaseSchemaDefinition;
  options: ServerOptions & DatabaseOptions;
  tasksGenerated: boolean;
  intentNames: Set<string>;
}

type QueryMacroOperation = "count" | "exists" | "one" | "aggregate";

function normalizeIntentToken(value: string): string {
  const normalized = kebabCase(String(value ?? "").trim());
  if (!normalized) {
    throw new Error("Actor token cannot be empty");
  }

  return normalized;
}

function validateIntentName(intentName: string): void {
  if (!intentName || typeof intentName !== "string") {
    throw new Error("Intent name must be a non-empty string");
  }

  if (intentName.length > 100) {
    throw new Error(`Intent name must be <= 100 characters: ${intentName}`);
  }

  if (intentName.includes(" ") || intentName.includes(".") || intentName.includes("\\")) {
    throw new Error(
      `Intent name cannot contain spaces, dots or backslashes: ${intentName}`,
    );
  }
}

function defaultOperationIntentDescription(
  operation: DbOperationType,
  tableName: string,
): string {
  return `Perform a ${operation} operation on the ${tableName} table`;
}

function readCustomIntentConfig(
  customIntent: string | { intent: string; description?: string; input?: SchemaDefinition },
): { intent: string; description?: string; input?: SchemaDefinition } {
  if (typeof customIntent === "string") {
    return {
      intent: customIntent,
    };
  }

  return {
    intent: customIntent.intent,
    description: customIntent.description,
    input: customIntent.input,
  };
}

export function resolveTableOperationIntents(
  actorName: string,
  tableName: string,
  table: TableDefinition,
  operation: DbOperationType,
  defaultInputSchema: SchemaDefinition,
): OperationIntentResolution {
  const actorToken = normalizeIntentToken(actorName);
  const defaultIntentName = `${operation}-pg-${actorToken}-${tableName}`;
  validateIntentName(defaultIntentName);

  const intents: OperationIntentDefinition[] = [
    {
      name: defaultIntentName,
      description: defaultOperationIntentDescription(operation, tableName),
      input: defaultInputSchema,
    },
  ];

  const seenNames = new Set<string>([defaultIntentName]);
  const customIntentList = table.customIntents?.[operation] ?? [];

  for (const rawCustomIntent of customIntentList) {
    const customIntent = readCustomIntentConfig(rawCustomIntent);
    const intentName = String(customIntent.intent ?? "").trim();

    if (!intentName) {
      throw new Error(
        `Invalid custom ${operation} intent on table '${tableName}': intent must be a non-empty string`,
      );
    }

    validateIntentName(intentName);

    if (seenNames.has(intentName)) {
      throw new Error(
        `Duplicate ${operation} intent '${intentName}' on table '${tableName}'`,
      );
    }

    seenNames.add(intentName);
    intents.push({
      name: intentName,
      description:
        customIntent.description ??
        defaultOperationIntentDescription(operation, tableName),
      input: customIntent.input ?? defaultInputSchema,
    });
  }

  return { intents };
}

export function resolveTableQueryIntents(
  actorName: string | null | undefined,
  tableName: string,
  table: TableDefinition,
  defaultInputSchema: SchemaDefinition,
): OperationIntentResolution {
  const resolvedActorName = actorName ?? "unknown-actor";
  return resolveTableOperationIntents(
    resolvedActorName,
    tableName,
    table,
    "query",
    defaultInputSchema,
  );
}

function ensurePlainObject(value: unknown, label: string): Record<string, any> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error(`${label} must be an object`);
  }

  return value as Record<string, any>;
}

function sleep(timeoutMs: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, timeoutMs);
  });
}

function normalizePositiveInteger(
  value: unknown,
  fallback: number,
  min: number = 1,
): number {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return fallback;
  }

  const normalized = Math.trunc(value);
  if (normalized < min) {
    return fallback;
  }

  return normalized;
}

function errorMessage(error: unknown): string {
  if (error instanceof Error) {
    return error.message;
  }

  return String(error);
}

function isTransientDatabaseError(error: unknown): boolean {
  if (!error || typeof error !== "object") {
    return false;
  }

  const dbError = error as { code?: string; message?: string };
  const code = String(dbError.code ?? "");

  if (["40001", "40P01", "57P03", "53300", "08006", "08001"].includes(code)) {
    return true;
  }

  const message = String(dbError.message ?? "").toLowerCase();
  return (
    message.includes("timeout") ||
    message.includes("terminating connection") ||
    message.includes("connection reset")
  );
}

function isSqlIdentifier(value: string): boolean {
  return /^[a-z_][a-z0-9_]*$/.test(value);
}

function toSafeSqlIdentifier(value: string, fallback: string): string {
  const normalized = snakeCase(value || "").replace(/[^a-z0-9_]/g, "_");
  const candidate = normalized || fallback;
  if (!isSqlIdentifier(candidate)) {
    throw new Error(`Invalid SQL identifier: ${value}`);
  }
  return candidate;
}

function isSupportedAggregateFunction(value: unknown): boolean {
  const fn = String(value ?? "").toLowerCase();
  return ["count", "sum", "avg", "min", "max"].includes(fn);
}

function buildAggregateAlias(aggregate: AggregateDefinition, index: number): string {
  const fn = String(aggregate.fn ?? "").toLowerCase();
  const hasField =
    typeof aggregate.field === "string" && aggregate.field.trim().length > 0;
  return toSafeSqlIdentifier(
    String(aggregate.as ?? `${fn}_${hasField ? aggregate.field : "all"}_${index}`),
    `${fn || "aggregate"}_${index}`,
  );
}

function resolveDataRows(data: unknown): Record<string, any>[] {
  if (!data) {
    return [];
  }

  if (Array.isArray(data)) {
    return data.map((entry) => ensurePlainObject(entry, "data item"));
  }

  return [ensurePlainObject(data, "data")];
}

/**
 * DatabaseController now acts as a PostgresActor plugin coordinator.
 *
 * Runtime ownership lives in actor runtime state (Pool + runtime health counters).
 * Durable actor state stores setup/configuration/health snapshots.
 */
export default class DatabaseController {
  private static _instance: DatabaseController;
  public static get instance(): DatabaseController {
    if (!this._instance) this._instance = new DatabaseController();
    return this._instance;
  }

  private readonly registrationsByService: Map<string, PostgresActorRegistration> =
    new Map();

  private readonly adminDbClient = new Pool({
    connectionString: process.env.DATABASE_ADDRESS ?? "",
    database: "postgres",
    ssl: {
      rejectUnauthorized: false,
    },
  });

  constructor() {
    Cadenza.createMetaTask(
      "Route PostgresActor setup requests",
      (ctx) => {
        const serviceName = String(ctx.options?.serviceName ?? ctx.serviceName ?? "");
        if (!serviceName) {
          return ctx;
        }

        const registration = this.registrationsByService.get(serviceName);
        if (!registration) {
          return ctx;
        }

        Cadenza.emit(`meta.postgres_actor.setup_requested.${registration.actorToken}`, ctx);
        return ctx;
      },
      "Routes generic database init requests to actor-scoped setup signal.",
      { isMeta: true, isSubMeta: true },
    ).doOn("meta.database_init_requested");
  }

  reset() {
    for (const registration of this.registrationsByService.values()) {
      const runtimeState = registration.actor.getRuntimeState(registration.actorKey);
      if (runtimeState?.pool) {
        runtimeState.pool.end().catch(() => undefined);
      }
    }

    this.registrationsByService.clear();
    this.adminDbClient.end().catch(() => undefined);
  }

  createPostgresActor(
    serviceName: string,
    schema: DatabaseSchemaDefinition,
    options: ServerOptions & DatabaseOptions,
  ): PostgresActorRegistration {
    const existing = this.registrationsByService.get(serviceName);
    if (existing) {
      return existing;
    }

    const actorName = `${serviceName}PostgresActor`;
    const actorToken = normalizeIntentToken(actorName);
    const actorKey = String(options.databaseName ?? snakeCase(serviceName));

    const optionTimeout =
      typeof (options as AnyObject).timeoutMs === "number"
        ? Number((options as AnyObject).timeoutMs)
        : Number((options as AnyObject).timeout);

    const safetyPolicy: PostgresActorSafetyPolicy = {
      statementTimeoutMs: normalizePositiveInteger(
        optionTimeout,
        normalizePositiveInteger(Number(process.env.DATABASE_STATEMENT_TIMEOUT_MS ?? 15000), 15000),
      ),
      retryCount: normalizePositiveInteger(options.retryCount, 3, 0),
      retryDelayMs: normalizePositiveInteger(Number(process.env.DATABASE_RETRY_DELAY_MS ?? 100), 100),
      retryDelayMaxMs: normalizePositiveInteger(
        Number(process.env.DATABASE_RETRY_DELAY_MAX_MS ?? 1000),
        1000,
      ),
      retryDelayFactor: Number(process.env.DATABASE_RETRY_DELAY_FACTOR ?? 2),
    };

    const actor = Cadenza.createActor<
      PostgresActorDurableState,
      PostgresActorRuntimeState
    >(
      {
        name: actorName,
        description:
          "Specialized PostgresActor owning pool runtime state and schema-driven DB task generation.",
        defaultKey: actorKey,
        keyResolver: (input: Record<string, any>) =>
          typeof input.databaseName === "string" ? input.databaseName : undefined,
        loadPolicy: "eager",
        writeContract: "overwrite",
        initState: {
          actorName,
          actorToken,
          serviceName,
          databaseName: actorKey,
          status: "idle",
          schemaVersion: Number(schema.version ?? 1),
          setupStartedAt: null,
          setupCompletedAt: null,
          lastHealthCheckAt: null,
          lastError: null,
          tables: Object.keys(schema.tables ?? {}),
          safetyPolicy,
        },
      },
      { isMeta: Boolean(options.isMeta) },
    );

    const registration: PostgresActorRegistration = {
      serviceName,
      databaseName: actorKey,
      actorName,
      actorToken,
      actorKey,
      actor,
      schema,
      options,
      tasksGenerated: false,
      intentNames: new Set(),
    };

    this.registrationsByService.set(serviceName, registration);
    this.registerSetupTask(registration);

    return registration;
  }

  private registerSetupTask(registration: PostgresActorRegistration): void {
    const setupSignal = `meta.postgres_actor.setup_requested.${registration.actorToken}`;

    Cadenza.createMetaTask(
      `Setup ${registration.actorName}`,
      registration.actor.task(
        async ({ input, state, runtimeState, setState, setRuntimeState, emit }) => {
          const requestedDatabaseName = String(
            input.options?.databaseName ?? input.databaseName ?? registration.databaseName,
          );

          if (requestedDatabaseName !== registration.databaseName) {
            return input;
          }

          setState({
            ...state,
            status: "initializing",
            setupStartedAt: new Date().toISOString(),
            lastError: null,
          });

          const priorRuntimePool = runtimeState?.pool ?? null;
          if (priorRuntimePool) {
            await priorRuntimePool.end().catch(() => undefined);
          }

          try {
            await this.createDatabaseIfMissing(requestedDatabaseName);

            const pool = this.createTargetPool(
              requestedDatabaseName,
              state.safetyPolicy.statementTimeoutMs,
            );

            await this.checkPoolHealth(pool, state.safetyPolicy);

            this.validateSchema({
              schema: registration.schema,
              options: registration.options,
            });

            const sortedTables = this.sortTablesByReferences({
              schema: registration.schema,
            }).sortedTables as string[];

            const ddlStatements = this.buildSchemaDdlStatements(
              registration.schema,
              sortedTables,
            );
            await this.applyDdlStatements(pool, ddlStatements);

            if (!registration.tasksGenerated) {
              this.generateDatabaseTasks(registration);
              registration.tasksGenerated = true;
            }

            const nowIso = new Date().toISOString();
            setRuntimeState({
              pool,
              ready: true,
              pendingQueries: 0,
              lastHealthCheckAt: Date.now(),
              lastError: null,
            });

            setState({
              ...state,
              status: "ready",
              setupCompletedAt: nowIso,
              lastHealthCheckAt: nowIso,
              lastError: null,
              tables: Object.keys(registration.schema.tables ?? {}),
            });

            emit("meta.database.setup_done", {
              serviceName: registration.serviceName,
              databaseName: registration.databaseName,
              actorName: registration.actorName,
              __success: true,
            });

            return {
              ...input,
              __success: true,
              actorName: registration.actorName,
              databaseName: registration.databaseName,
            };
          } catch (error) {
            const message = errorMessage(error);
            setRuntimeState({
              pool: null,
              ready: false,
              pendingQueries: 0,
              lastHealthCheckAt: runtimeState?.lastHealthCheckAt ?? null,
              lastError: message,
            });
            setState({
              ...state,
              status: "error",
              setupCompletedAt: new Date().toISOString(),
              lastError: message,
            });
            throw error;
          }
        },
        { mode: "write" },
      ),
      "Initializes PostgresActor runtime pool, applies schema, and generates CRUD tasks/intents.",
      { isMeta: true },
    ).doOn(setupSignal);
  }

  private createTargetPool(databaseName: string, statementTimeoutMs: number): Pool {
    const connectionString = this.buildDatabaseConnectionString(databaseName);
    return new Pool({
      connectionString,
      statement_timeout: statementTimeoutMs,
      query_timeout: statementTimeoutMs,
      ssl: {
        rejectUnauthorized: false,
      },
    });
  }

  private buildDatabaseConnectionString(databaseName: string): string {
    const base = process.env.DATABASE_ADDRESS ?? "";
    if (!base) {
      throw new Error("DATABASE_ADDRESS environment variable is required");
    }

    try {
      const parsed = new URL(base);
      parsed.pathname = `/${databaseName}`;
      if (!parsed.searchParams.has("sslmode")) {
        parsed.searchParams.set("sslmode", "disable");
      }
      return parsed.toString();
    } catch {
      const lastSlashIndex = base.lastIndexOf("/");
      if (lastSlashIndex === -1) {
        throw new Error("DATABASE_ADDRESS must be a valid postgres connection string");
      }

      const root = base.slice(0, lastSlashIndex + 1);
      return `${root}${databaseName}?sslmode=disable`;
    }
  }

  private async createDatabaseIfMissing(databaseName: string): Promise<void> {
    if (!isSqlIdentifier(databaseName)) {
      throw new Error(
        `Invalid database name '${databaseName}'. Names must contain only lowercase alphanumerics and underscores and cannot start with a number.`,
      );
    }

    try {
      await this.adminDbClient.query(`CREATE DATABASE ${databaseName}`);
      Cadenza.log(`Database ${databaseName} created.`);
    } catch (error: any) {
      if (error?.code === "42P04") {
        Cadenza.log(`Database ${databaseName} already exists.`);
        return;
      }
      throw new Error(`Failed to create database '${databaseName}': ${errorMessage(error)}`);
    }
  }

  private async checkPoolHealth(
    pool: Pool,
    safetyPolicy: PostgresActorSafetyPolicy,
  ): Promise<void> {
    await this.runWithRetries(
      async () => {
        await this.withTimeout(
          () => pool.query("SELECT 1 as health"),
          safetyPolicy.statementTimeoutMs,
          "Database health check timed out",
        );
      },
      safetyPolicy,
      "Health check",
    );
  }

  private getPoolOrThrow(registration: PostgresActorRegistration): Pool {
    const runtimeState = registration.actor.getRuntimeState(
      registration.actorKey,
    ) as PostgresActorRuntimeState | undefined;

    if (!runtimeState || !runtimeState.ready || !runtimeState.pool) {
      throw new Error(
        `PostgresActor '${registration.actorName}' is not ready. Ensure setup completed before running DB tasks.`,
      );
    }

    return runtimeState.pool;
  }

  private async withTimeout<T>(
    work: () => Promise<T>,
    timeoutMs: number,
    timeoutMessage: string,
  ): Promise<T> {
    let timeoutHandle: NodeJS.Timeout | null = null;

    try {
      return await Promise.race([
        work(),
        new Promise<T>((_, reject) => {
          timeoutHandle = setTimeout(() => {
            reject(new Error(timeoutMessage));
          }, timeoutMs);
        }),
      ]);
    } finally {
      if (timeoutHandle) {
        clearTimeout(timeoutHandle);
      }
    }
  }

  private async runWithRetries<T>(
    work: () => Promise<T>,
    safetyPolicy: PostgresActorSafetyPolicy,
    operationLabel: string,
  ): Promise<T> {
    const attempts = normalizePositiveInteger(safetyPolicy.retryCount, 0, 0) + 1;
    let delayMs = normalizePositiveInteger(safetyPolicy.retryDelayMs, 100);
    const maxDelayMs = normalizePositiveInteger(safetyPolicy.retryDelayMaxMs, 1000);
    const factor = Number.isFinite(safetyPolicy.retryDelayFactor)
      ? Math.max(1, safetyPolicy.retryDelayFactor)
      : 1;

    let lastError: unknown;

    for (let attempt = 1; attempt <= attempts; attempt += 1) {
      try {
        return await work();
      } catch (error) {
        lastError = error;
        const transient = isTransientDatabaseError(error);
        if (!transient || attempt >= attempts) {
          break;
        }

        Cadenza.log(
          `${operationLabel} failed with transient error. Retrying...`,
          {
            attempt,
            attempts,
            delayMs,
            error: errorMessage(error),
          },
          "warning",
        );

        await sleep(delayMs);
        delayMs = Math.min(Math.trunc(delayMs * factor), maxDelayMs);
      }
    }

    throw lastError;
  }

  private async executeWithTransaction<T>(
    pool: Pool,
    transaction: boolean,
    callback: (client: Pool | PoolClient) => Promise<T>,
  ): Promise<T> {
    if (!transaction) {
      return callback(pool);
    }

    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      const result = await callback(client);
      await client.query("COMMIT");
      return result;
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }

  private async queryFunction(
    registration: PostgresActorRegistration,
    tableName: string,
    context: DbOperationPayload,
  ): Promise<any> {
    const {
      filter = {},
      fields = [],
      joins = {},
      sort = {},
      limit,
      offset,
      queryMode = "rows",
      aggregates = [],
      groupBy = [],
    } = context;

    const pool = this.getPoolOrThrow(registration);
    const statementTimeoutMs = this.resolveSafetyPolicy(registration).statementTimeoutMs;
    const resolvedMode: QueryMode = queryMode;
    const aggregateDefinitions = Array.isArray(aggregates) ? aggregates : [];
    const groupByFields = Array.isArray(groupBy) ? groupBy : [];
    const params: any[] = [];

    const whereClause =
      Object.keys(filter).length > 0 ? this.buildWhereClause(filter, params) : "";
    const joinClause = Object.keys(joins).length > 0 ? this.buildJoinClause(joins) : "";

    let sql: string;
    if (resolvedMode === "count") {
      sql = `SELECT COUNT(*)::bigint AS count FROM ${tableName} ${joinClause} ${whereClause}`;
    } else if (resolvedMode === "exists") {
      sql = `SELECT EXISTS(SELECT 1 FROM ${tableName} ${joinClause} ${whereClause}) AS exists`;
    } else if (resolvedMode === "aggregate") {
      if (aggregateDefinitions.length === 0) {
        throw new Error("Aggregate queries require at least one aggregate definition");
      }

      const aggregateExpressions = aggregateDefinitions.map(
        (aggregate: AggregateDefinition, index: number) => {
          const fn = String(aggregate.fn ?? "").toLowerCase();
          if (!isSupportedAggregateFunction(fn)) {
            throw new Error(`Unsupported aggregate function '${aggregate.fn}'`);
          }

          const hasField =
            typeof aggregate.field === "string" && aggregate.field.trim().length > 0;
          if (fn !== "count" && !hasField) {
            throw new Error(`Aggregate '${fn}' requires a field`);
          }

          const fieldExpression = hasField ? snakeCase(String(aggregate.field)) : "*";
          const distinctPrefix = aggregate.distinct ? "DISTINCT " : "";
          const expression =
            fn === "count" && !hasField
              ? "COUNT(*)"
              : `${fn.toUpperCase()}(${distinctPrefix}${fieldExpression})`;
          const alias = buildAggregateAlias(aggregate, index);
          return `${expression} AS ${alias}`;
        },
      );

      const groupByExpressions = groupByFields.map((field) => snakeCase(field));
      const selectExpressions = [...groupByExpressions, ...aggregateExpressions];
      sql = `SELECT ${selectExpressions.join(", ")} FROM ${tableName} ${joinClause} ${whereClause}`;

      if (groupByExpressions.length > 0) {
        sql += ` GROUP BY ${groupByExpressions.join(", ")}`;
      }
    } else {
      sql = `SELECT ${fields.length ? fields.map(snakeCase).join(", ") : "*"} FROM ${tableName} ${joinClause} ${whereClause}`;
    }

    if (
      Object.keys(sort).length > 0 &&
      resolvedMode !== "count" &&
      resolvedMode !== "exists"
    ) {
      sql +=
        " ORDER BY " +
        Object.entries(sort)
          .map(([field, direction]) => `${snakeCase(field)} ${direction}`)
          .join(", ");
    }

    if (resolvedMode === "one") {
      sql += ` LIMIT $${params.length + 1}`;
      params.push(1);
    } else if (
      resolvedMode !== "count" &&
      resolvedMode !== "exists" &&
      limit !== undefined
    ) {
      sql += ` LIMIT $${params.length + 1}`;
      params.push(limit);
    }
    if (
      resolvedMode !== "count" &&
      resolvedMode !== "exists" &&
      offset !== undefined
    ) {
      sql += ` OFFSET $${params.length + 1}`;
      params.push(offset);
    }

    try {
      const result = await this.withTimeout(
        () => pool.query(sql, params),
        statementTimeoutMs,
        `Query timeout on table ${tableName}`,
      );

      const rows = this.toCamelCase(result.rows);
      const rowCount = Number(result.rowCount ?? 0);

      if (resolvedMode === "count") {
        return {
          count: Number(rows[0]?.count ?? 0),
          rowCount: Number(rows[0]?.count ?? 0),
          __success: true,
        };
      }

      if (resolvedMode === "exists") {
        const exists = Boolean(rows[0]?.exists);
        return {
          exists,
          rowCount: exists ? 1 : 0,
          __success: true,
        };
      }

      if (resolvedMode === "one") {
        return {
          [`${camelCase(tableName)}`]: rows[0] ?? null,
          rowCount,
          __success: true,
        };
      }

      if (resolvedMode === "aggregate") {
        return {
          aggregates: rows,
          rowCount,
          __success: true,
        };
      }

      return {
        [`${camelCase(tableName)}s`]: rows,
        rowCount,
        __success: true,
      };
    } catch (error) {
      return {
        rowCount: 0,
        errored: true,
        __error: `Query failed: ${errorMessage(error)}`,
        __success: false,
      };
    }
  }

  private async insertFunction(
    registration: PostgresActorRegistration,
    tableName: string,
    context: DbOperationPayload,
  ): Promise<any> {
    const { data, transaction = true, fields = [], onConflict } = context;

    if (!data || (Array.isArray(data) && data.length === 0)) {
      return {
        rowCount: 0,
        errored: true,
        __error: "No data provided for insert",
        __success: false,
      };
    }

    const pool = this.getPoolOrThrow(registration);
    const safetyPolicy = this.resolveSafetyPolicy(registration);

    try {
      const resultContext = await this.runWithRetries(
        async () =>
          this.executeWithTransaction(pool, Boolean(transaction), async (client) => {
            const resolvedData = await this.resolveNestedData(
              registration,
              data,
              tableName,
            );
            const rows = Array.isArray(resolvedData) ? resolvedData : [resolvedData];

            if (rows.length === 0) {
              throw new Error("No rows available for insert after resolving data");
            }

            const keys = Object.keys(rows[0]);
            const sqlPrefix = `INSERT INTO ${tableName} (${keys
              .map((key) => snakeCase(key))
              .join(", ")}) VALUES `;

            const params: any[] = [];
            const placeholders = rows
              .map((row) => {
                const tuple = keys
                  .map((key) => {
                    params.push(row[key]);
                    return `$${params.length}`;
                  })
                  .join(", ");
                return `(${tuple})`;
              })
              .join(", ");

            let onConflictSql = "";
            if (onConflict) {
              onConflictSql = this.buildOnConflictClause(onConflict, params);
            }

            const sql = `${sqlPrefix}${placeholders}${onConflictSql} RETURNING ${
              fields.length ? fields.map(snakeCase).join(", ") : "*"
            }`;

            const result = await this.withTimeout(
              () => client.query(sql, params),
              safetyPolicy.statementTimeoutMs,
              `Insert timeout on table ${tableName}`,
            );

            const resultRows = this.toCamelCase(result.rows);
            return {
              [`${camelCase(tableName)}${rows.length > 1 ? "s" : ""}`]:
                rows.length > 1 ? resultRows : resultRows[0] ?? null,
              rowCount: result.rowCount,
              __success: true,
            };
          }),
        safetyPolicy,
        `Insert ${tableName}`,
      );

      return resultContext;
    } catch (error) {
      return {
        rowCount: 0,
        errored: true,
        __error: `Insert failed: ${errorMessage(error)}`,
        __success: false,
      };
    }
  }

  private async updateFunction(
    registration: PostgresActorRegistration,
    tableName: string,
    context: DbOperationPayload,
  ): Promise<any> {
    const { data, filter = {}, transaction = true } = context;

    if (!data || Object.keys(data).length === 0) {
      return {
        rowCount: 0,
        errored: true,
        __error: `No data provided for update of ${tableName}`,
        __success: false,
      };
    }

    const pool = this.getPoolOrThrow(registration);
    const safetyPolicy = this.resolveSafetyPolicy(registration);

    try {
      return await this.runWithRetries(
        async () =>
          this.executeWithTransaction(pool, Boolean(transaction), async (client) => {
            const resolvedData = await this.resolveNestedData(
              registration,
              data,
              tableName,
            );
            const params = Object.values(resolvedData);

            let offset = 0;
            const setClause = Object.keys(resolvedData)
              .map((key, index) => {
                const value: any = (resolvedData as AnyObject)[key];
                const offsetIndex = index - offset;
                if (value?.__effect === "increment") {
                  params.splice(offsetIndex, 1);
                  offset += 1;
                  return `${snakeCase(key)} = ${snakeCase(key)} + 1`;
                }
                if (value?.__effect === "decrement") {
                  params.splice(offsetIndex, 1);
                  offset += 1;
                  return `${snakeCase(key)} = ${snakeCase(key)} - 1`;
                }
                if (value?.__effect === "set") {
                  params.splice(offsetIndex, 1);
                  offset += 1;
                  return `${snakeCase(key)} = ${value.__value}`;
                }

                return `${snakeCase(key)} = $${offsetIndex + 1}`;
              })
              .join(", ");

            const whereClause = this.buildWhereClause(filter, params);
            const sql = `UPDATE ${tableName} SET ${setClause} ${whereClause} RETURNING *`;
            const result = await this.withTimeout(
              () => client.query(sql, params),
              safetyPolicy.statementTimeoutMs,
              `Update timeout on table ${tableName}`,
            );

            const rows = this.toCamelCase(result.rows);
            const rowCount = Number(result.rowCount ?? 0);
            return {
              [`${camelCase(tableName)}`]: rows[0] ?? null,
              rowCount,
              __success: rowCount > 0,
            };
          }),
        safetyPolicy,
        `Update ${tableName}`,
      );
    } catch (error) {
      return {
        rowCount: 0,
        errored: true,
        __error: `Update failed: ${errorMessage(error)}`,
        __success: false,
      };
    }
  }

  private async deleteFunction(
    registration: PostgresActorRegistration,
    tableName: string,
    context: DbOperationPayload,
  ): Promise<any> {
    const { filter = {}, transaction = true } = context;

    if (Object.keys(filter).length === 0) {
      return {
        rowCount: 0,
        errored: true,
        __error: "No filter provided for delete",
        __success: false,
      };
    }

    const pool = this.getPoolOrThrow(registration);
    const safetyPolicy = this.resolveSafetyPolicy(registration);

    try {
      return await this.runWithRetries(
        async () =>
          this.executeWithTransaction(pool, Boolean(transaction), async (client) => {
            const params: any[] = [];
            const whereClause = this.buildWhereClause(filter, params);
            const sql = `DELETE FROM ${tableName} ${whereClause} RETURNING *`;
            const result = await this.withTimeout(
              () => client.query(sql, params),
              safetyPolicy.statementTimeoutMs,
              `Delete timeout on table ${tableName}`,
            );
            const rows = this.toCamelCase(result.rows);

            return {
              [`${camelCase(tableName)}`]: rows[0] ?? null,
              rowCount: result.rowCount,
              __success: true,
            };
          }),
        safetyPolicy,
        `Delete ${tableName}`,
      );
    } catch (error) {
      return {
        rowCount: 0,
        errored: true,
        __error: `Delete failed: ${errorMessage(error)}`,
        __success: false,
      };
    }
  }

  private resolveSafetyPolicy(
    registration: PostgresActorRegistration,
  ): PostgresActorSafetyPolicy {
    const durableState = registration.actor.getState(
      registration.actorKey,
    ) as PostgresActorDurableState;

    return {
      statementTimeoutMs: normalizePositiveInteger(
        durableState.safetyPolicy?.statementTimeoutMs,
        15000,
      ),
      retryCount: normalizePositiveInteger(durableState.safetyPolicy?.retryCount, 3, 0),
      retryDelayMs: normalizePositiveInteger(durableState.safetyPolicy?.retryDelayMs, 100),
      retryDelayMaxMs: normalizePositiveInteger(
        durableState.safetyPolicy?.retryDelayMaxMs,
        1000,
      ),
      retryDelayFactor: Number.isFinite(durableState.safetyPolicy?.retryDelayFactor)
        ? Math.max(1, Number(durableState.safetyPolicy?.retryDelayFactor))
        : 1,
    };
  }

  private buildOnConflictClause(
    onConflict: { target: string[]; action: OnConflictAction },
    params: any[],
  ): string {
    const { target, action } = onConflict;
    let sql = ` ON CONFLICT (${target.map(snakeCase).join(", ")})`;

    if (action.do === "update") {
      if (!action.set || Object.keys(action.set).length === 0) {
        throw new Error("Update action requires 'set' fields");
      }

      const assignments = Object.entries(action.set).map(([field, value]) => {
        if (typeof value === "string" && value === "excluded") {
          return `${snakeCase(field)} = excluded.${snakeCase(field)}`;
        }

        params.push(value);
        return `${snakeCase(field)} = $${params.length}`;
      });

      sql += ` DO UPDATE SET ${assignments.join(", ")}`;
      if (action.where) {
        sql += ` WHERE ${action.where}`;
      }
      return sql;
    }

    sql += " DO NOTHING";
    return sql;
  }

  /**
   * Validates database schema structure and content.
   */
  validateSchema(ctx: AnyObject): true {
    const schema = ctx.schema as DatabaseSchemaDefinition;
    if (!schema?.tables || typeof schema.tables !== "object") {
      throw new Error("Invalid schema: missing or invalid tables");
    }

    for (const [tableName, table] of Object.entries(schema.tables)) {
      if (!isSqlIdentifier(tableName)) {
        throw new Error(
          `Invalid table name ${tableName}. Table names must use lowercase snake_case identifiers.`,
        );
      }

      if (!table.fields || typeof table.fields !== "object") {
        throw new Error(`Invalid table ${tableName}: missing fields`);
      }

      for (const [fieldName, field] of Object.entries(table.fields)) {
        if (!isSqlIdentifier(fieldName)) {
          throw new Error(
            `Invalid field name ${fieldName} for ${tableName}. Field names must use lowercase snake_case identifiers.`,
          );
        }

        if (!SCHEMA_TYPES.includes(field.type)) {
          throw new Error(`Invalid type ${field.type} for ${tableName}.${fieldName}`);
        }

        if (field.references && !field.references.match(/^[\w]+\([\w]+\)$/)) {
          throw new Error(
            `Invalid reference ${field.references} for ${tableName}.${fieldName}`,
          );
        }
      }

      for (const operation of ["query", "insert", "update", "delete"] as DbOperationType[]) {
        const customIntents = table.customIntents?.[operation] ?? [];
        if (!Array.isArray(customIntents)) {
          throw new Error(
            `Invalid customIntents.${operation} for table ${tableName}: expected array`,
          );
        }

        for (const customIntent of customIntents) {
          const parsed = readCustomIntentConfig(
            customIntent as
              | string
              | { intent: string; description?: string; input?: SchemaDefinition },
          );
          validateIntentName(String(parsed.intent ?? ""));
        }
      }
    }

    return true;
  }

  sortTablesByReferences(ctx: AnyObject): AnyObject {
    const schema: DatabaseSchemaDefinition = ctx.schema;
    const graph: Map<string, Set<string>> = new Map();
    const allTables = Object.keys(schema.tables);

    allTables.forEach((table) => graph.set(table, new Set()));

    for (const [tableName, table] of Object.entries(schema.tables)) {
      for (const field of Object.values(table.fields)) {
        if (field.references) {
          const [refTable] = field.references.split("(");
          if (refTable !== tableName && allTables.includes(refTable)) {
            graph.get(refTable)?.add(tableName);
          }
        }
      }

      if (table.foreignKeys) {
        for (const foreignKey of table.foreignKeys) {
          const refTable = foreignKey.tableName;
          if (refTable !== tableName && allTables.includes(refTable)) {
            graph.get(refTable)?.add(tableName);
          }
        }
      }
    }

    const visited: Set<string> = new Set();
    const tempMark: Set<string> = new Set();
    const sorted: string[] = [];
    let hasCycles = false;

    const visit = (table: string) => {
      if (tempMark.has(table)) {
        hasCycles = true;
        return;
      }
      if (visited.has(table)) return;

      tempMark.add(table);
      for (const dependent of graph.get(table) || []) {
        visit(dependent);
      }
      tempMark.delete(table);
      visited.add(table);
      sorted.push(table);
    };

    for (const table of allTables) {
      if (!visited.has(table)) {
        visit(table);
      }
    }

    for (const table of allTables) {
      if (!visited.has(table)) {
        sorted.push(table);
      }
    }

    sorted.reverse();
    return { ...ctx, sortedTables: sorted, hasCycles };
  }

  private buildSchemaDdlStatements(
    schema: DatabaseSchemaDefinition,
    sortedTables: string[],
  ): string[] {
    const ddl: string[] = [];

    for (const tableName of sortedTables) {
      const table = schema.tables[tableName];
      const fieldDefs = Object.entries(table.fields)
        .map(([fieldName, field]) => this.fieldDefinitionToSql(fieldName, field))
        .join(", ");

      ddl.push(`CREATE TABLE IF NOT EXISTS ${tableName} (${fieldDefs});`);

      for (const indexFields of table.indexes ?? []) {
        ddl.push(
          `CREATE INDEX IF NOT EXISTS idx_${tableName}_${indexFields.join("_")} ON ${tableName} (${indexFields
            .map(snakeCase)
            .join(", ")});`,
        );
      }

      if (table.primaryKey) {
        ddl.push(
          `ALTER TABLE ${tableName} DROP CONSTRAINT IF EXISTS pk_${tableName}_${table.primaryKey.join("_")};`,
          `ALTER TABLE ${tableName} ADD CONSTRAINT pk_${tableName}_${table.primaryKey.join("_")} PRIMARY KEY (${table.primaryKey
            .map(snakeCase)
            .join(", ")});`,
        );
      }

      for (const uniqueFields of table.uniqueConstraints ?? []) {
        ddl.push(
          `ALTER TABLE ${tableName} DROP CONSTRAINT IF EXISTS uq_${tableName}_${uniqueFields.join("_")};`,
          `ALTER TABLE ${tableName} ADD CONSTRAINT uq_${tableName}_${uniqueFields.join("_")} UNIQUE (${uniqueFields
            .map(snakeCase)
            .join(", ")});`,
        );
      }

      for (const foreignKey of table.foreignKeys ?? []) {
        const fkName = `fk_${tableName}_${foreignKey.fields.join("_")}`;
        ddl.push(
          `ALTER TABLE ${tableName} DROP CONSTRAINT IF EXISTS ${fkName};`,
          `ALTER TABLE ${tableName} ADD CONSTRAINT ${fkName} FOREIGN KEY (${foreignKey.fields
            .map(snakeCase)
            .join(", ")}) REFERENCES ${foreignKey.tableName} (${foreignKey.referenceFields
            .map(snakeCase)
            .join(", ")});`,
        );
      }

      for (const [triggerName, trigger] of Object.entries(table.triggers ?? {})) {
        ddl.push(
          `CREATE OR REPLACE TRIGGER ${triggerName} ${trigger.when} ${trigger.event} ON ${tableName} FOR EACH STATEMENT EXECUTE FUNCTION ${trigger.function};`,
        );
      }

      if (table.initialData) {
        ddl.push(
          `INSERT INTO ${tableName} (${table.initialData.fields
            .map(snakeCase)
            .join(", ")}) VALUES ${table.initialData.data
            .map(
              (row) =>
                `(${row
                  .map((value) => {
                    if (value === undefined) return "NULL";
                    if (value === null) return "NULL";
                    if (typeof value === "number") return String(value);
                    if (typeof value === "boolean") return value ? "TRUE" : "FALSE";
                    const stringValue = String(value);
                    return `'${stringValue.replace(/'/g, "''")}'`;
                  })
                  .join(", ")})`,
            )
            .join(", ")} ON CONFLICT DO NOTHING;`,
        );
      }
    }

    return ddl;
  }

  private fieldDefinitionToSql(fieldName: string, field: FieldDefinition): string {
    let definition = `${snakeCase(fieldName)} ${field.type.toUpperCase()}`;

    if (field.type === "varchar") {
      definition += `(${field.constraints?.maxLength ?? 255})`;
    }

    if (field.type === "decimal") {
      definition += `(${field.constraints?.precision ?? 10},${field.constraints?.scale ?? 2})`;
    }

    if (field.primary) definition += " PRIMARY KEY";
    if (field.unique) definition += " UNIQUE";
    if (field.default !== undefined) {
      definition += ` DEFAULT ${field.default === "" ? "''" : String(field.default)}`;
    }
    if (field.required && !field.nullable) definition += " NOT NULL";
    if (field.nullable) definition += " NULL";
    if (field.generated) {
      definition += ` GENERATED ALWAYS AS ${field.generated.toUpperCase()} STORED`;
    }
    if (field.references) {
      definition += ` REFERENCES ${field.references} ON DELETE ${field.onDelete || "CASCADE"}`;
    }
    if (field.constraints?.check) {
      definition += ` CHECK (${field.constraints.check})`;
    }

    return definition;
  }

  private async applyDdlStatements(pool: Pool, statements: string[]): Promise<void> {
    for (const sql of statements) {
      try {
        await pool.query(sql);
      } catch (error) {
        Cadenza.log(
          "Error applying DDL statement",
          {
            sql,
            error: errorMessage(error),
          },
          "error",
        );
        throw error;
      }
    }
  }

  private generateDatabaseTasks(registration: PostgresActorRegistration): void {
    for (const [tableName, table] of Object.entries(registration.schema.tables)) {
      this.createDatabaseTask(registration, "query", tableName, table);
      this.createDatabaseTask(registration, "insert", tableName, table);
      this.createDatabaseTask(registration, "update", tableName, table);
      this.createDatabaseTask(registration, "delete", tableName, table);
      this.createDatabaseMacroTasks(registration, tableName, table);
    }
  }

  private createDatabaseMacroTasks(
    registration: PostgresActorRegistration,
    tableName: string,
    table: TableDefinition,
  ): void {
    const querySchema = this.getInputSchema("query", tableName, table);
    const insertSchema = this.getInputSchema("insert", tableName, table);

    const queryMacroOperations: QueryMacroOperation[] = [
      "count",
      "exists",
      "one",
      "aggregate",
    ];

    for (const macroOperation of queryMacroOperations) {
      const intentName = `${macroOperation}-pg-${registration.actorToken}-${tableName}`;
      if (registration.intentNames.has(intentName)) {
        throw new Error(
          `Duplicate macro intent '${intentName}' detected for table '${tableName}' in actor '${registration.actorName}'`,
        );
      }

      registration.intentNames.add(intentName);
      Cadenza.defineIntent({
        name: intentName,
        description: `Macro ${macroOperation} operation for table ${tableName}`,
        input: querySchema,
      });

      Cadenza.createThrottledTask(
        `${macroOperation.toUpperCase()} ${tableName}`,
        registration.actor.task(
          async ({ input }) => {
            const payload =
              typeof input.queryData === "object" && input.queryData
                ? (input.queryData as DbOperationPayload)
                : (input as DbOperationPayload);

            const result = await this.queryFunction(registration, tableName, {
              ...payload,
              queryMode: macroOperation,
            });

            return {
              ...input,
              ...result,
            };
          },
          { mode: "read" },
        ),
        (context?: AnyObject) =>
          context?.__metadata?.__executionTraceId ??
          context?.__executionTraceId ??
          "default",
        `Macro ${macroOperation} task for ${tableName}`,
        {
          isMeta: registration.options.isMeta,
          isSubMeta: registration.options.isMeta,
          validateInputContext: registration.options.securityProfile !== "low",
          inputSchema: querySchema,
        },
      ).respondsTo(intentName);
    }

    const upsertIntentName = `upsert-pg-${registration.actorToken}-${tableName}`;
    if (registration.intentNames.has(upsertIntentName)) {
      throw new Error(
        `Duplicate macro intent '${upsertIntentName}' detected for table '${tableName}' in actor '${registration.actorName}'`,
      );
    }

    registration.intentNames.add(upsertIntentName);
    Cadenza.defineIntent({
      name: upsertIntentName,
      description: `Macro upsert operation for table ${tableName}`,
      input: insertSchema,
    });

    Cadenza.createThrottledTask(
      `UPSERT ${tableName}`,
      registration.actor.task(
        async ({ input }) => {
          const payload =
            typeof input.queryData === "object" && input.queryData
              ? (input.queryData as DbOperationPayload)
              : (input as DbOperationPayload);

          if (!payload.onConflict) {
            return {
              ...input,
              errored: true,
              __success: false,
              __error: `Macro upsert requires 'onConflict' payload for table '${tableName}'`,
            };
          }

          const result = await this.insertFunction(registration, tableName, payload);
          return {
            ...input,
            ...result,
          };
        },
        { mode: "write" },
      ),
      (context?: AnyObject) =>
        context?.__metadata?.__executionTraceId ??
        context?.__executionTraceId ??
        "default",
      `Macro upsert task for ${tableName}`,
      {
        isMeta: registration.options.isMeta,
        isSubMeta: registration.options.isMeta,
        validateInputContext: registration.options.securityProfile !== "low",
        inputSchema: insertSchema,
      },
    ).respondsTo(upsertIntentName);
  }

  private createDatabaseTask(
    registration: PostgresActorRegistration,
    op: DbOperationType,
    tableName: string,
    table: TableDefinition,
  ): void {
    const opAction =
      op === "query"
        ? "queried"
        : op === "insert"
          ? "inserted"
          : op === "update"
            ? "updated"
            : "deleted";

    const defaultSignal = `global.${registration.options.isMeta ? "meta." : ""}${tableName}.${opAction}`;
    const taskName = `${op.charAt(0).toUpperCase() + op.slice(1)} ${tableName}`;
    const schema = this.getInputSchema(op, tableName, table);

    const databaseTaskFunction = registration.actor.task(
      async ({ input, emit }) => {
        let context: AnyObject = { ...input };
        let payloadModifiedByTriggers = false;

        for (const action of Object.keys(table.customSignals?.triggers ?? {})) {
          const triggerDefinitions = (table.customSignals?.triggers as AnyObject)?.[
            action
          ] as
            | (
                | string
                | {
                    signal: string;
                    condition?: (ctx: AnyObject) => boolean;
                    queryData?: DbOperationPayload;
                  }
              )[]
            | undefined;

          for (const trigger of triggerDefinitions ?? []) {
            if (typeof trigger === "string") {
              continue;
            }

            if (trigger.condition && !trigger.condition(context)) {
              return {
                failed: true,
                __success: false,
                __error: `Condition for signal trigger failed: ${trigger.signal}`,
              };
            }

            if (trigger.queryData) {
              context.queryData = {
                ...(context.queryData ?? {}),
                ...trigger.queryData,
              };
              payloadModifiedByTriggers = true;
            }
          }
        }

        const operationPayload =
          typeof context.queryData === "object" && context.queryData
            ? (context.queryData as DbOperationPayload)
            : (context as DbOperationPayload);

        this.validateOperationPayload(
          registration,
          op,
          tableName,
          table,
          operationPayload,
          {
            enforceFieldAllowlist:
              registration.options.securityProfile === "low" ||
              payloadModifiedByTriggers,
          },
        );

        let result: AnyObject;
        if (op === "query") {
          result = await this.queryFunction(registration, tableName, operationPayload);
        } else if (op === "insert") {
          result = await this.insertFunction(registration, tableName, operationPayload);
        } else if (op === "update") {
          result = await this.updateFunction(registration, tableName, operationPayload);
        } else {
          result = await this.deleteFunction(registration, tableName, operationPayload);
        }

        context = {
          ...context,
          ...result,
        };

        if (!context.errored) {
          for (const signal of table.customSignals?.emissions?.[op] ?? []) {
            if (typeof signal === "string") {
              emit(signal, context);
              continue;
            }

            if (signal.condition && !signal.condition(context)) {
              continue;
            }

            emit(signal.signal, context);
          }
        }

        if (tableName !== "system_log" && context.errored) {
          Cadenza.log(
            `ERROR in ${taskName}`,
            JSON.stringify({
              data: context.data,
              queryData: context.queryData,
              filter: context.filter,
              fields: context.fields,
              joins: context.joins,
              sort: context.sort,
              limit: context.limit,
              offset: context.offset,
              error: context.__error,
            }),
            "error",
          );
        }

        delete context.queryData;
        delete context.data;
        delete context.filter;
        delete context.fields;
        delete context.joins;
        delete context.sort;
        delete context.limit;
        delete context.offset;

        return context;
      },
      { mode: op === "query" ? "read" : "write" },
    );

    const task = Cadenza.createThrottledTask(
      taskName,
      databaseTaskFunction,
      (context?: AnyObject) =>
        context?.__metadata?.__executionTraceId ??
        context?.__executionTraceId ??
        "default",
      `Auto-generated ${op} task for ${tableName} (PostgresActor)`,
      {
        isMeta: registration.options.isMeta,
        isSubMeta: registration.options.isMeta,
        validateInputContext: registration.options.securityProfile !== "low",
        inputSchema: schema,
      },
    )
      .doOn(
        ...(table.customSignals?.triggers?.[op]?.map((signal: any) =>
          typeof signal === "string" ? signal : signal.signal,
        ) ?? []),
      )
      .emits(defaultSignal)
      .attachSignal(
        ...(table.customSignals?.emissions?.[op]?.map((signal: any) =>
          typeof signal === "string" ? signal : signal.signal,
        ) ?? []),
      );

    const { intents } = resolveTableOperationIntents(
      registration.actorName,
      tableName,
      table,
      op,
      schema,
    );

    for (const intent of intents) {
      if (registration.intentNames.has(intent.name)) {
        throw new Error(
          `Duplicate auto/custom intent '${intent.name}' detected while generating ${op} task for table '${tableName}' in actor '${registration.actorName}'`,
        );
      }

      registration.intentNames.add(intent.name);
      Cadenza.defineIntent({
        name: intent.name,
        description: intent.description,
        input: intent.input,
      });
    }

    task.respondsTo(...intents.map((intent) => intent.name));
  }

  private validateOperationPayload(
    registration: PostgresActorRegistration,
    operation: DbOperationType,
    tableName: string,
    table: TableDefinition,
    payload: DbOperationPayload,
    options: { enforceFieldAllowlist: boolean },
  ): void {
    const allowedFields = new Set<string>(Object.keys(table.fields));
    const resolvedMode: QueryMode = payload.queryMode ?? "rows";
    if (
      !["rows", "count", "exists", "one", "aggregate"].includes(resolvedMode)
    ) {
      throw new Error(`Unsupported queryMode '${String(payload.queryMode)}'`);
    }

    const assertAllowedField = (fieldName: string, label: string) => {
      if (!allowedFields.has(fieldName)) {
        throw new Error(
          `Invalid field '${fieldName}' in ${label} for ${operation} on ${tableName}`,
        );
      }
    };

    const aggregateDefinitions = Array.isArray(payload.aggregates)
      ? payload.aggregates
      : [];
    const aggregateSortAllowlist = new Set<string>();
    if (resolvedMode === "aggregate") {
      if (aggregateDefinitions.length === 0) {
        throw new Error(
          `Aggregate queryMode requires at least one aggregate on table '${tableName}'`,
        );
      }

      for (const groupField of payload.groupBy ?? []) {
        assertAllowedField(groupField, "groupBy");
        aggregateSortAllowlist.add(groupField);
      }

      for (const [index, aggregate] of aggregateDefinitions.entries()) {
        if (!isSupportedAggregateFunction(aggregate.fn)) {
          throw new Error(
            `Unsupported aggregate function '${String(aggregate.fn)}' on table '${tableName}'`,
          );
        }

        if (aggregate.fn !== "count" && !aggregate.field) {
          throw new Error(
            `Aggregate '${aggregate.fn}' requires field on table '${tableName}'`,
          );
        }

        if (aggregate.field) {
          assertAllowedField(aggregate.field, "aggregates.field");
        }

        aggregateSortAllowlist.add(buildAggregateAlias(aggregate, index));
      }
    } else if (aggregateDefinitions.length > 0 || (payload.groupBy ?? []).length > 0) {
      throw new Error(
        `aggregates/groupBy payload requires queryMode='aggregate' on table '${tableName}'`,
      );
    }

    if (options.enforceFieldAllowlist) {
      if (payload.fields) {
        for (const field of payload.fields) {
          assertAllowedField(field, "fields");
        }
      }

      if (payload.filter) {
        for (const field of Object.keys(payload.filter)) {
          assertAllowedField(field, "filter");
        }
      }

      if (payload.data) {
        const rows = resolveDataRows(payload.data);
        for (const row of rows) {
          for (const field of Object.keys(row)) {
            assertAllowedField(field, "data");
          }
        }
      }
    }

    if (payload.sort) {
      for (const field of Object.keys(payload.sort)) {
        if (resolvedMode === "aggregate" && aggregateSortAllowlist.has(field)) {
          continue;
        }
        assertAllowedField(field, "sort");
      }
    }

    if (payload.onConflict) {
      for (const conflictField of payload.onConflict.target ?? []) {
        assertAllowedField(conflictField, "onConflict.target");
      }

      for (const setField of Object.keys(payload.onConflict.action?.set ?? {})) {
        assertAllowedField(setField, "onConflict.action.set");
      }
    }

    if (payload.joins) {
      this.validateJoinPayload(registration.schema, payload.joins);
    }
  }

  private validateJoinPayload(
    schema: DatabaseSchemaDefinition,
    joins: Record<string, JoinDefinition>,
  ): void {
    for (const [joinTableName, joinDefinition] of Object.entries(joins)) {
      if (!schema.tables[joinTableName]) {
        throw new Error(`Invalid join table '${joinTableName}'. Table does not exist in schema.`);
      }

      const joinTable = schema.tables[joinTableName];
      for (const field of joinDefinition.fields ?? []) {
        if (!joinTable.fields[field]) {
          throw new Error(
            `Invalid join field '${field}' on joined table '${joinTableName}'`,
          );
        }
      }

      if (joinDefinition.filter) {
        for (const filterField of Object.keys(joinDefinition.filter)) {
          if (!joinTable.fields[filterField]) {
            throw new Error(
              `Invalid join filter field '${filterField}' on joined table '${joinTableName}'`,
            );
          }
        }
      }

      if (joinDefinition.joins) {
        this.validateJoinPayload(schema, joinDefinition.joins);
      }
    }
  }

  toCamelCase(rows: any[]) {
    return rows.map((row: any) => {
      const camelCasedRow: any = {};
      for (const [key, value] of Object.entries(row)) {
        camelCasedRow[camelCase(key)] = value;
      }
      return camelCasedRow;
    });
  }

  buildWhereClause(filter: AnyObject, params: any[]): string {
    const conditions = [];
    for (const [key, value] of Object.entries(filter)) {
      if (value !== undefined) {
        if (Array.isArray(value)) {
          const placeholders = value
            .map((entry) => {
              params.push(entry);
              return `$${params.length}`;
            })
            .join(", ");
          conditions.push(`${snakeCase(key)} IN (${placeholders})`);
        } else {
          params.push(value);
          conditions.push(`${snakeCase(key)} = $${params.length}`);
        }
      }
    }

    return conditions.length ? `WHERE ${conditions.join(" AND ")}` : "";
  }

  buildJoinClause(joins: Record<string, JoinDefinition>): string {
    let joinSql = "";
    for (const [table, join] of Object.entries(joins)) {
      const alias = join.alias ? ` ${join.alias}` : "";
      joinSql += ` LEFT JOIN ${snakeCase(table)}${alias} ON ${join.on}`;
      if (join.joins) joinSql += " " + this.buildJoinClause(join.joins);
    }
    return joinSql;
  }

  async resolveNestedData(
    registration: PostgresActorRegistration,
    data: any,
    tableName: string,
  ): Promise<any> {
    if (Array.isArray(data)) {
      return Promise.all(
        data.map((entry) => this.resolveNestedData(registration, entry, tableName)),
      );
    }

    if (typeof data !== "object" || data === null) {
      return data;
    }

    const resolved = { ...data };
    for (const [key, value] of Object.entries(data)) {
      if (typeof value === "object" && value !== null && "subOperation" in value) {
        const subOperation = value as SubOperation;
        resolved[key] = await this.executeSubOperation(registration, subOperation);
      } else if (
        typeof value === "string" &&
        ["increment", "decrement", "set"].includes(value)
      ) {
        resolved[key] = { __effect: value };
      } else if (typeof value === "object" && value !== null) {
        resolved[key] = await this.resolveNestedData(registration, value, tableName);
      }
    }

    return resolved;
  }

  async executeSubOperation(
    registration: PostgresActorRegistration,
    operation: SubOperation,
  ): Promise<any> {
    const targetTableName = operation.table;
    if (!registration.schema.tables[targetTableName]) {
      throw new Error(
        `Sub-operation table '${targetTableName}' does not exist in actor schema`,
      );
    }

    const pool = this.getPoolOrThrow(registration);
    const safetyPolicy = this.resolveSafetyPolicy(registration);

    return this.executeWithTransaction(pool, true, async (client) => {
      if (operation.subOperation === "insert") {
        const resolvedData = await this.resolveNestedData(
          registration,
          operation.data,
          operation.table,
        );
        const row = ensurePlainObject(resolvedData, "sub-operation insert data");

        const keys = Object.keys(row);
        const params = Object.values(row);
        const sql = `INSERT INTO ${operation.table} (${keys
          .map((key) => snakeCase(key))
          .join(", ")}) VALUES (${params
          .map((_, index) => `$${index + 1}`)
          .join(", ")}) ON CONFLICT DO NOTHING RETURNING ${operation.return ?? "*"}`;

        const result = await this.withTimeout(
          () => client.query(sql, params),
          safetyPolicy.statementTimeoutMs,
          `Sub-operation insert timeout on table ${operation.table}`,
        );

        const returnKey = operation.return ?? "uuid";
        if (result.rows[0]?.[returnKey] !== undefined) {
          return result.rows[0][returnKey];
        }

        return row[returnKey] ?? row.uuid ?? {};
      }

      const queryParams: any[] = [];
      const whereClause = this.buildWhereClause(operation.filter || {}, queryParams);
      const sql = `SELECT ${(operation.fields ?? ["*"])
        .map((field) => (field === "*" ? field : snakeCase(field)))
        .join(", ")} FROM ${operation.table} ${whereClause} LIMIT 1`;

      const result = await this.withTimeout(
        () => client.query(sql, queryParams),
        safetyPolicy.statementTimeoutMs,
        `Sub-operation query timeout on table ${operation.table}`,
      );

      const returnKey = operation.return ?? "uuid";
      return result.rows[0]?.[returnKey] ?? {};
    });
  }

  private getInputSchema(
    op: DbOperationType,
    tableName: string,
    table: TableDefinition,
  ): SchemaDefinition {
    const inputSchema: SchemaDefinition = {
      type: "object",
      properties: {
        queryData: {
          type: "object",
          properties: {},
          strict: true,
        },
      },
      strict: false,
    };

    if (!inputSchema.properties) {
      return inputSchema;
    }

    inputSchema.properties.transaction = getTransactionSchema();
    // @ts-ignore
    inputSchema.properties.queryData.properties.transaction =
      inputSchema.properties.transaction;

    if (op === "insert" || op === "update") {
      inputSchema.properties.data = getInsertDataSchemaFromTable(table, tableName);
      // @ts-ignore
      inputSchema.properties.queryData.properties.data = inputSchema.properties.data;
    }

    if (op === "insert") {
      inputSchema.properties.batch = getQueryBatchSchemaFromTable();
      // @ts-ignore
      inputSchema.properties.queryData.properties.batch = inputSchema.properties.batch;

      inputSchema.properties.onConflict = getQueryOnConflictSchemaFromTable(
        table,
        tableName,
      );
      // @ts-ignore
      inputSchema.properties.queryData.properties.onConflict =
        inputSchema.properties.onConflict;
    }

    if (op === "query" || op === "update" || op === "delete") {
      inputSchema.properties.filter = getQueryFilterSchemaFromTable(table, tableName);
      // @ts-ignore
      inputSchema.properties.queryData.properties.filter =
        inputSchema.properties.filter;
    }

    if (op === "query") {
      inputSchema.properties.queryMode = getQueryModeSchema();
      // @ts-ignore
      inputSchema.properties.queryData.properties.queryMode =
        inputSchema.properties.queryMode;

      inputSchema.properties.fields = getQueryFieldsSchemaFromTable(table, tableName);
      // @ts-ignore
      inputSchema.properties.queryData.properties.fields =
        inputSchema.properties.fields;

      inputSchema.properties.joins = getQueryJoinsSchemaFromTable(table, tableName);
      // @ts-ignore
      inputSchema.properties.queryData.properties.joins = inputSchema.properties.joins;

      inputSchema.properties.sort = getQuerySortSchemaFromTable(table, tableName);
      // @ts-ignore
      inputSchema.properties.queryData.properties.sort = inputSchema.properties.sort;

      inputSchema.properties.aggregates = getQueryAggregatesSchemaFromTable(
        table,
        tableName,
      );
      // @ts-ignore
      inputSchema.properties.queryData.properties.aggregates =
        inputSchema.properties.aggregates;

      inputSchema.properties.groupBy = getQueryGroupBySchemaFromTable(table, tableName);
      // @ts-ignore
      inputSchema.properties.queryData.properties.groupBy = inputSchema.properties.groupBy;

      inputSchema.properties.limit = getQueryLimitSchemaFromTable();
      // @ts-ignore
      inputSchema.properties.queryData.properties.limit = inputSchema.properties.limit;

      inputSchema.properties.offset = getQueryOffsetSchemaFromTable();
      // @ts-ignore
      inputSchema.properties.queryData.properties.offset =
        inputSchema.properties.offset;
    }

    return inputSchema;
  }
}

export function getInsertDataSchemaFromTable(
  table: TableDefinition,
  tableName: string,
): Schema {
  const dataSchema: Schema = {
    type: "object",
    properties: {
      ...Object.fromEntries(
        Object.entries(table.fields).map(([fieldName, field]) => {
          return [
            fieldName,
            {
              value: {
                type: tableFieldTypeToSchemaType(field.type),
                description: `Inferred from field '${fieldName}' of type [${field.type}] on table ${tableName}.`,
              },
              effect: {
                type: "string",
                constraints: {
                  oneOf: ["increment", "decrement", "set"],
                },
              },
              subOperation: {
                type: "object",
                properties: {
                  subOperation: {
                    type: "string",
                    enum: ["insert", "query"],
                  },
                  table: {
                    type: "string",
                  },
                  data: {
                    single: {
                      type: "object",
                    },
                    batch: {
                      type: "array",
                      items: {
                        type: "object",
                      },
                    },
                  },
                  filter: {
                    type: "object",
                  },
                  fields: {
                    type: "array",
                    items: {
                      type: "string",
                    },
                  },
                  return: {
                    type: "string",
                  },
                },
                required: ["subOperation", "table"],
              },
            },
          ];
        }),
      ),
    },
    required: Object.entries(table.fields)
      .filter(([, field]) => field.required || field.primary)
      .map(([fieldName]) => fieldName),
    strict: true,
  };

  return {
    single: dataSchema,
    batch: {
      type: "array",
      items: dataSchema,
    },
  };
}

export function getQueryFilterSchemaFromTable(
  table: TableDefinition,
  tableName: string,
): SchemaDefinition {
  return {
    type: "object",
    properties: {
      ...Object.fromEntries(
        Object.entries(table.fields).map(([fieldName, field]) => {
          return [
            fieldName,
            {
              value: {
                type: tableFieldTypeToSchemaType(field.type),
                description: `Inferred from field '${fieldName}' of type [${field.type}] on table ${tableName}.`,
              },
              in: {
                type: "array",
                items: {
                  type: tableFieldTypeToSchemaType(field.type),
                },
              },
            },
          ];
        }),
      ),
    },
    strict: true,
    description: `Inferred from table '${tableName}' on postgres actor table contract.`,
  };
}

function getQueryFieldsSchemaFromTable(
  table: TableDefinition,
  tableName: string,
): SchemaDefinition {
  return {
    type: "array",
    items: {
      type: "string",
      constraints: {
        oneOf: Object.keys(table.fields),
      },
    },
    description: `Inferred field projection from table '${tableName}'.`,
  };
}

function getQueryModeSchema(): SchemaDefinition {
  return {
    type: "string",
    constraints: {
      oneOf: ["rows", "count", "exists", "one", "aggregate"],
    },
  };
}

function getQueryAggregatesSchemaFromTable(
  table: TableDefinition,
  tableName: string,
): SchemaDefinition {
  return {
    type: "array",
    items: {
      type: "object",
      properties: {
        fn: {
          type: "string",
          constraints: {
            oneOf: ["count", "sum", "avg", "min", "max"],
          },
        },
        field: {
          type: "string",
          constraints: {
            oneOf: Object.keys(table.fields),
          },
        },
        as: {
          type: "string",
        },
        distinct: {
          type: "boolean",
        },
      },
      required: ["fn"],
      strict: true,
    },
    description: `Aggregate definitions inferred from table '${tableName}'.`,
  };
}

function getQueryGroupBySchemaFromTable(
  table: TableDefinition,
  tableName: string,
): SchemaDefinition {
  return {
    type: "array",
    items: {
      type: "string",
      constraints: {
        oneOf: Object.keys(table.fields),
      },
    },
    description: `Group by fields inferred from table '${tableName}'.`,
  };
}

function getQueryJoinsSchemaFromTable(
  _table: TableDefinition,
  tableName: string,
): SchemaDefinition {
  return {
    type: "object",
    description: `Join definitions for table '${tableName}'.`,
  };
}

function getQuerySortSchemaFromTable(
  _table: TableDefinition,
  tableName: string,
): SchemaDefinition {
  return {
    type: "object",
    strict: false,
    description:
      `Sort definition for table '${tableName}'. Keys are validated at runtime against allowlists and aggregate aliases.`,
  };
}

function getQueryLimitSchemaFromTable(): SchemaDefinition {
  return {
    type: "number",
    constraints: {
      min: 0,
      max: 1000,
    },
  };
}

function getQueryOffsetSchemaFromTable(): SchemaDefinition {
  return {
    type: "number",
    constraints: {
      min: 0,
      max: 1000000,
    },
  };
}

function getQueryBatchSchemaFromTable(): SchemaDefinition {
  return {
    type: "boolean",
  };
}

function getTransactionSchema(): SchemaDefinition {
  return {
    type: "boolean",
    description: "Execute the operation in a transaction.",
  };
}

function getQueryOnConflictSchemaFromTable(
  table: TableDefinition,
  tableName: string,
): SchemaDefinition {
  return {
    type: "object",
    properties: {
      target: {
        type: "array",
        items: {
          type: "string",
          constraints: {
            oneOf: Object.keys(table.fields),
          },
        },
      },
      action: {
        type: "object",
        properties: {
          do: {
            type: "string",
            constraints: {
              oneOf: ["nothing", "update"],
            },
          },
          set: {
            type: "object",
          },
          where: {
            type: "string",
          },
        },
      },
    },
    strict: true,
    description: `Conflict strategy for inserts on table '${tableName}'.`,
  };
}

function tableFieldTypeToSchemaType(type: string): SchemaDefinition["type"] {
  switch (type) {
    case "varchar":
    case "text":
    case "uuid":
    case "timestamp":
    case "date":
    case "geo_point":
      return "string";
    case "int":
    case "bigint":
    case "decimal":
      return "number";
    case "boolean":
      return "boolean";
    case "array":
      return "array";
    case "object":
    case "jsonb":
      return "object";
    case "bytea":
      return "string";
    default:
      return "any";
  }
}
