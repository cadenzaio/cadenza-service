import {
  DatabaseMigrationConstraintDefinition,
  DatabaseMigrationDefinition,
  DatabaseMigrationPolicy,
  DatabaseMigrationStep,
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
  ownerServiceName: string | null;
  databaseName: string;
  status: "idle" | "initializing" | "ready" | "error";
  schemaVersion: number;
  schemaVersionTarget: number;
  schemaVersionApplied: number;
  migrationStatus: "idle" | "applying" | "ready" | "error";
  lastMigrationVersion: number | null;
  lastMigrationName: string | null;
  lastMigrationChecksum: string | null;
  lastMigrationAppliedAt: string | null;
  lastMigrationError: string | null;
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
  ownerServiceName: string | null;
  databaseName: string;
  actorName: string;
  actorToken: string;
  actorKey: string;
  setupSignal: string;
  setupDoneSignal: string;
  setupFailedSignal: string;
  actor: Actor<PostgresActorDurableState, PostgresActorRuntimeState>;
  schema: DatabaseSchemaDefinition;
  description: string;
  options: ServerOptions & DatabaseOptions;
  tasksGenerated: boolean;
  intentNames: Set<string>;
}

interface AppliedMigrationRecord {
  version: number;
  name: string;
  checksum: string;
  status: string;
  appliedAt: string | null;
  errorMessage: string | null;
}

type QueryMacroOperation = "count" | "exists" | "one" | "aggregate";
const AUTHORITY_SYNC_DEBUG_PREFIX = "[CADENZA_DB_TASK_DEBUG]";
const AUTHORITY_SYNC_DEBUG_ENABLED =
  typeof process !== "undefined" &&
  typeof process.env === "object" &&
  process.env.CADENZA_DB_TASK_DEBUG === "true";
const AUTHORITY_SYNC_DEBUG_TASK_NAMES = new Set<string>([
  "Query service_instance",
  "Query service_instance_transport",
  "Query intent_to_task_map",
  "Query signal_to_task_map",
  "Forward service instance sync",
  "Forward service transport sync",
  "Forward intent to task map sync",
  "Forward signal to task map sync",
  "Compile sync data and broadcast",
  "Prepare for signal sync",
]);
const AUTHORITY_SYNC_DEBUG_ROUTINE_NAMES = new Set<string>(["Sync services"]);
const INTENT_MAP_DEBUG_ENABLED =
  process.env.CADENZA_INTENT_MAP_DEBUG === "1" ||
  process.env.CADENZA_INTENT_MAP_DEBUG === "true";

function normalizeQueryResultValue(value: unknown): unknown {
  if (value instanceof Date) {
    return value.toISOString();
  }

  if (Array.isArray(value)) {
    return value.map((entry) => normalizeQueryResultValue(entry));
  }

  if (value && typeof value === "object") {
    const prototype = Object.getPrototypeOf(value);
    if (prototype === Object.prototype || prototype === null) {
      return Object.fromEntries(
        Object.entries(value as Record<string, unknown>).map(([key, nestedValue]) => [
          key,
          normalizeQueryResultValue(nestedValue),
        ]),
      );
    }
  }

  return value;
}
const POSTGRES_SETUP_DEBUG_ENABLED =
  process.env.CADENZA_POSTGRES_SETUP_DEBUG === "1" ||
  process.env.CADENZA_POSTGRES_SETUP_DEBUG === "true";
const ACTOR_SESSION_TRACE_ENABLED =
  process.env.CADENZA_ACTOR_SESSION_TRACE === "1" ||
  process.env.CADENZA_ACTOR_SESSION_TRACE === "true";
const ACTOR_SESSION_TRACE_LIMIT = 20;
let actorSessionTraceCount = 0;
const GENERATED_POSTGRES_WRITE_TASK_CONCURRENCY = 200;
const GENERATED_POSTGRES_WRITE_TASK_TIMEOUT_MS = 120_000;
const EXECUTION_OBSERVABILITY_TABLES = new Set<string>([
  "execution_trace",
  "routine_execution",
  "task_execution",
  "signal_emission",
  "inquiry",
]);
const EXECUTION_OBSERVABILITY_RETRY_COUNT = 8;
const EXECUTION_OBSERVABILITY_RETRY_DELAY_MS = 250;
const EXECUTION_OBSERVABILITY_RETRY_DELAY_MAX_MS = 5_000;
const EXECUTION_OBSERVABILITY_RETRY_DELAY_FACTOR = 2;
const EXECUTION_OBSERVABILITY_INSERT_TASK_CONCURRENCY = 1;
const SCHEMA_MIGRATION_LEDGER_TABLE = "cadenza_schema_migration";
const DEFAULT_MIGRATION_POLICY: Required<DatabaseMigrationPolicy> = {
  baselineOnEmpty: true,
  adoptExistingVersion: null,
  allowDestructive: false,
  transactionalMode: "per_migration",
};

function logAuthoritySyncDebug(
  event: string,
  payload: Record<string, unknown>,
): void {
  if (!AUTHORITY_SYNC_DEBUG_ENABLED) {
    return;
  }
  console.log(`${AUTHORITY_SYNC_DEBUG_PREFIX} ${event}`, payload);
}

function resolveAuthoritySyncPayloadName(
  payload: DbOperationPayload,
  tableName: string,
): string | null {
  const data =
    payload.data && typeof payload.data === "object" && !Array.isArray(payload.data)
      ? (payload.data as Record<string, unknown>)
      : null;

  if (!data) {
    return null;
  }

  if (tableName === "task") {
    const taskName = data.name;
    return typeof taskName === "string" ? taskName : null;
  }

  if (tableName === "task_to_routine_map") {
    const taskName = data.taskName ?? data.task_name;
    return typeof taskName === "string" ? taskName : null;
  }

  return null;
}

function shouldDebugAuthoritySyncPayload(
  tableName: string,
  payload: DbOperationPayload,
): boolean {
  if (!AUTHORITY_SYNC_DEBUG_ENABLED) {
    return false;
  }

  if (tableName !== "task" && tableName !== "task_to_routine_map") {
    return false;
  }

  const payloadName = resolveAuthoritySyncPayloadName(payload, tableName);
  if (payloadName && AUTHORITY_SYNC_DEBUG_TASK_NAMES.has(payloadName)) {
    return true;
  }

  if (tableName === "task_to_routine_map") {
    const data =
      payload.data && typeof payload.data === "object" && !Array.isArray(payload.data)
        ? (payload.data as Record<string, unknown>)
        : null;
    const routineName = data?.routineName ?? data?.routine_name;
    return (
      typeof routineName === "string" &&
      AUTHORITY_SYNC_DEBUG_ROUTINE_NAMES.has(routineName)
    );
  }

  return false;
}

function logIntentMapSetupDebug(
  event: string,
  payload: Record<string, unknown>,
): void {
  if (!INTENT_MAP_DEBUG_ENABLED) {
    return;
  }

  console.log("[CADENZA_INTENT_MAP_DEBUG]", event, payload);
}

function logPostgresSetupDebug(
  event: string,
  payload: Record<string, unknown>,
): void {
  if (!POSTGRES_SETUP_DEBUG_ENABLED) {
    return;
  }

  console.log("[CADENZA_POSTGRES_SETUP_DEBUG]", event, payload);
}

function logActorSessionTrace(
  event: string,
  payload: Record<string, unknown>,
): void {
  if (!ACTOR_SESSION_TRACE_ENABLED) {
    return;
  }

  actorSessionTraceCount += 1;
  if (actorSessionTraceCount > ACTOR_SESSION_TRACE_LIMIT) {
    return;
  }

  console.log("[CADENZA_ACTOR_SESSION_TRACE]", event, payload);
}

function buildAuthoritySyncDebugSummary(
  payload: DbOperationPayload,
  context: AnyObject,
): Record<string, unknown> {
  const data =
    payload.data && typeof payload.data === "object" && !Array.isArray(payload.data)
      ? (payload.data as Record<string, unknown>)
      : {};

  return {
    taskName:
      data.name ??
      data.taskName ??
      data.task_name ??
      context.__taskName ??
      null,
    routineName: data.routineName ?? data.routine_name ?? context.__routineName ?? null,
    serviceName:
      data.serviceName ??
      data.service_name ??
      context.__syncServiceName ??
      null,
    predecessorTaskName:
      data.predecessorTaskName ?? data.predecessor_task_name ?? null,
    queryDataKeys:
      context.queryData && typeof context.queryData === "object"
        ? Object.keys(context.queryData as Record<string, unknown>)
        : [],
    dataKeys: Object.keys(data),
    onConflict:
      payload.onConflict && typeof payload.onConflict === "object"
        ? payload.onConflict
        : null,
    rowCount: context.rowCount ?? null,
    error: context.__error ?? null,
  };
}

function normalizeIntentToken(value: string): string {
  const normalized = kebabCase(String(value ?? "").trim());
  if (!normalized) {
    throw new Error("Actor token cannot be empty");
  }

  return normalized;
}

function shouldValidateGeneratedDbTaskInput(
  registration: PostgresActorRegistration,
): boolean {
  return registration.options.securityProfile !== "low" && !registration.options.isMeta;
}

function buildPostgresActorName(name: string): string {
  return `${String(name ?? "").trim()}PostgresActor`;
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

function isExplicitSqlLiteral(value: string): boolean {
  return /^'.*'::[a-z_][a-z0-9_]*(\[\])?$/i.test(value.trim());
}

function extractExplicitJsonLiteral(value: string): string | null {
  const match = /^'(.*)'::jsonb$/i.exec(value.trim());
  if (!match) {
    return null;
  }

  return match[1].replace(/''/g, "'");
}

function isExplicitSqlExpression(value: string): boolean {
  const trimmed = value.trim();
  if (!trimmed) {
    return false;
  }

  if (isExplicitSqlLiteral(trimmed)) {
    return true;
  }

  if (/^'.*'$/.test(trimmed)) {
    return true;
  }

  if (/^[a-z_][a-z0-9_]*\([^)]*\)$/i.test(trimmed)) {
    return true;
  }

  return /^(current_timestamp|current_date|current_time)$/i.test(trimmed);
}

export function serializeFieldDefaultForSql(
  value: unknown,
  field?: FieldDefinition,
): string {
  if (value === undefined || value === null) {
    return "NULL";
  }

  if (typeof value === "number") {
    return String(value);
  }

  if (typeof value === "boolean") {
    return value ? "TRUE" : "FALSE";
  }

  const stringValue = String(value);
  if (isExplicitSqlExpression(stringValue)) {
    return stringValue;
  }

  if (field?.type === "jsonb") {
    return `'${JSON.stringify(value).replace(/'/g, "''")}'::jsonb`;
  }

  return `'${stringValue.replace(/'/g, "''")}'`;
}

export function serializeInitialDataValueForSql(
  value: unknown,
  field?: FieldDefinition,
): string {
  if (value === undefined || value === null) {
    return "NULL";
  }

  if (typeof value === "number") {
    return String(value);
  }

  if (typeof value === "boolean") {
    return value ? "TRUE" : "FALSE";
  }

  if (field?.type === "jsonb") {
    if (typeof value === "string" && isExplicitSqlLiteral(value)) {
      return value;
    }

    const jsonString = JSON.stringify(value);
    return `'${jsonString.replace(/'/g, "''")}'::jsonb`;
  }

  const stringValue = String(value);
  if (isExplicitSqlExpression(stringValue)) {
    return stringValue;
  }

  return `'${stringValue.replace(/'/g, "''")}'`;
}

export function serializeFieldValueForQuery(
  value: unknown,
  field?: FieldDefinition,
): unknown {
  if (value === undefined || value === null || field?.type !== "jsonb") {
    return value;
  }

  if (typeof value === "string") {
    const explicitJsonLiteral = extractExplicitJsonLiteral(value);
    if (explicitJsonLiteral !== null) {
      return explicitJsonLiteral;
    }

    try {
      JSON.parse(value);
      return value;
    } catch {
      return JSON.stringify(value);
    }
  }

  return JSON.stringify(value);
}

function isSubOperationPayload(value: unknown): value is SubOperation {
  if (!value || typeof value !== "object") {
    return false;
  }

  const candidate = value as Partial<SubOperation>;
  return (
    typeof candidate.subOperation === "string" &&
    typeof candidate.table === "string" &&
    candidate.table.trim().length > 0
  );
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

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function stableStringify(value: unknown): string {
  if (Array.isArray(value)) {
    return `[${value.map((item) => stableStringify(item)).join(",")}]`;
  }

  if (isPlainObject(value)) {
    return `{${Object.keys(value)
      .sort()
      .map((key) => `${JSON.stringify(key)}:${stableStringify(value[key])}`)
      .join(",")}}`;
  }

  return JSON.stringify(value);
}

export function computeDatabaseMigrationChecksum(
  migration: DatabaseMigrationDefinition,
): string {
  return stableStringify({
    version: migration.version,
    name: migration.name,
    description: migration.description ?? "",
    transaction: migration.transaction ?? "inherit",
    steps: migration.steps,
  });
}

function resolveDatabaseMigrationPolicy(
  schema: DatabaseSchemaDefinition,
): Required<DatabaseMigrationPolicy> {
  return {
    baselineOnEmpty:
      schema.migrationPolicy?.baselineOnEmpty ??
      DEFAULT_MIGRATION_POLICY.baselineOnEmpty,
    adoptExistingVersion:
      schema.migrationPolicy?.adoptExistingVersion ??
      DEFAULT_MIGRATION_POLICY.adoptExistingVersion,
    allowDestructive:
      schema.migrationPolicy?.allowDestructive ??
      DEFAULT_MIGRATION_POLICY.allowDestructive,
    transactionalMode:
      schema.migrationPolicy?.transactionalMode ??
      DEFAULT_MIGRATION_POLICY.transactionalMode,
  };
}

const EXECUTION_OBSERVABILITY_RETRYABLE_FOREIGN_KEYS = new Set<string>([
  "routine_execution_execution_trace_id_fkey",
  "task_execution_routine_execution_id_fkey",
  "task_execution_execution_trace_id_fkey",
  "task_execution_signal_emission_id_fkey",
  "task_execution_inquiry_id_fkey",
  "signal_emission_execution_trace_id_fkey",
  "signal_emission_routine_execution_id_fkey",
  "signal_emission_task_execution_id_fkey",
  "inquiry_execution_trace_id_fkey",
  "inquiry_routine_execution_id_fkey",
  "inquiry_task_execution_id_fkey",
]);

const EXECUTION_OBSERVABILITY_ZERO_ROW_RETRYABLE_TABLES = new Set<string>([
  "routine_execution",
  "task_execution",
  "signal_emission",
  "inquiry",
]);

function resolveOperationTableName(operationLabel?: string): string | null {
  if (!operationLabel) {
    return null;
  }

  const match = /^insert\s+([a-z0-9_]+)$/i.exec(operationLabel.trim());
  return match?.[1]?.toLowerCase() ?? null;
}

function isRetryableExecutionObservabilityForeignKeyError(
  error: unknown,
  operationLabel?: string,
): boolean {
  if (!error || typeof error !== "object") {
    return false;
  }

  const dbError = error as {
    code?: string;
    message?: string;
    constraint?: string;
    table?: string;
  };
  if (String(dbError.code ?? "") !== "23503") {
    return false;
  }

  const constraint = String(dbError.constraint ?? "").toLowerCase();
  if (
    constraint &&
    EXECUTION_OBSERVABILITY_RETRYABLE_FOREIGN_KEYS.has(constraint)
  ) {
    return true;
  }

  const table =
    String(dbError.table ?? "").toLowerCase() ||
    resolveOperationTableName(operationLabel) ||
    "";
  if (
    !["routine_execution", "task_execution", "signal_emission", "inquiry"].includes(
      table,
    )
  ) {
    return false;
  }

  const message = String(dbError.message ?? "").toLowerCase();
  return (
    message.includes("foreign key constraint") &&
    (message.includes("execution_trace") ||
      message.includes("routine_execution") ||
      message.includes("task_execution") ||
      message.includes("signal_emission") ||
      message.includes("inquiry"))
  );
}

export function isTransientDatabaseError(
  error: unknown,
  operationLabel?: string,
): boolean {
  if (!error || typeof error !== "object") {
    return false;
  }

  const dbError = error as { code?: string; message?: string };
  const code = String(dbError.code ?? "");

  if (["40001", "40P01", "57P03", "53300", "08006", "08001"].includes(code)) {
    return true;
  }

  if (code === "EXEC_OBSERVABILITY_UPDATE_MISSING_ROW") {
    return true;
  }

  const message = String(dbError.message ?? "").toLowerCase();
  if (
    message.includes("timeout") ||
    message.includes("terminating connection") ||
    message.includes("connection reset")
  ) {
    return true;
  }

  return isRetryableExecutionObservabilityForeignKeyError(error, operationLabel);
}

export function shouldRetryExecutionObservabilityMissingUpdate(
  tableName: string,
  filter: Record<string, unknown>,
): boolean {
  if (!EXECUTION_OBSERVABILITY_ZERO_ROW_RETRYABLE_TABLES.has(tableName)) {
    return false;
  }

  if (!filter || typeof filter !== "object" || Object.keys(filter).length === 0) {
    return false;
  }

  return true;
}

export function resolveGeneratedInsertTaskConcurrency(tableName: string): number {
  if (EXECUTION_OBSERVABILITY_TABLES.has(tableName)) {
    return EXECUTION_OBSERVABILITY_INSERT_TASK_CONCURRENCY;
  }

  return GENERATED_POSTGRES_WRITE_TASK_CONCURRENCY;
}

function resolveExecutionObservabilityThrottleKey(
  tableName: string,
  context?: AnyObject,
): string {
  const queryData =
    context?.queryData && typeof context.queryData === "object"
      ? (context.queryData as Record<string, unknown>)
      : undefined;
  const data =
    queryData?.data && typeof queryData.data === "object"
      ? (queryData.data as Record<string, unknown>)
      : context?.data && typeof context.data === "object"
        ? (context.data as Record<string, unknown>)
        : undefined;
  const metadata =
    context?.__metadata && typeof context.__metadata === "object"
      ? (context.__metadata as Record<string, unknown>)
      : undefined;

  const traceId =
    context?.__executionTraceId ??
    metadata?.__executionTraceId ??
    data?.executionTraceId ??
    data?.execution_trace_id ??
    (tableName === "execution_trace" ? data?.uuid : undefined) ??
    data?.routineExecutionId ??
    data?.routine_execution_id ??
    data?.inquiryId ??
    data?.inquiry_id ??
    data?.taskExecutionId ??
    data?.task_execution_id ??
    data?.previousTaskExecutionId ??
    data?.previous_task_execution_id ??
    "global";

  return `execution-observability:${String(traceId)}`;
}

export function resolveGeneratedInsertTaskTag(
  tableName: string,
  actorToken: string,
  context?: AnyObject,
): string {
  return resolveGeneratedTaskTag(tableName, actorToken, context, "insert");
}

export function resolveGeneratedTaskTag(
  tableName: string,
  actorToken: string,
  context: AnyObject | undefined,
  operation: DbOperationType,
): string {
  if (EXECUTION_OBSERVABILITY_TABLES.has(tableName)) {
    return resolveExecutionObservabilityThrottleKey(tableName, context);
  }

  if (operation === "insert") {
    return `insert:${actorToken}:${tableName}`;
  }

  const traceScopedTag =
    context?.__metadata?.__executionTraceId ??
    context?.__executionTraceId ??
    context?.__metadata?.__deputyExecId ??
    context?.__deputyExecId ??
    context?.__metadata?.__inquiryId ??
    context?.__inquiryId ??
    context?.__metadata?.__routineExecId ??
    context?.__routineExecId ??
    context?.__metadata?.__localRoutineExecId ??
    context?.__localRoutineExecId;

  if (typeof traceScopedTag === "string" && traceScopedTag.length > 0) {
    return traceScopedTag;
  }

  const queryScope = {
    queryData: context?.queryData,
    filter: context?.filter,
    fields: context?.fields,
    joins: context?.joins,
    sort: context?.sort,
    limit: context?.limit,
    offset: context?.offset,
    queryMode: context?.queryMode,
    aggregates: context?.aggregates,
    groupBy: context?.groupBy,
  };

  return (
    `read:${actorToken}:${tableName}:${stableStringify(queryScope)}`
  );
}

export function resolveExecutionObservabilitySafetyPolicyForTable(
  basePolicy: PostgresActorSafetyPolicy,
  tableName: string,
): PostgresActorSafetyPolicy {
  if (!EXECUTION_OBSERVABILITY_TABLES.has(tableName)) {
    return basePolicy;
  }

  return {
    ...basePolicy,
    retryCount: Math.max(basePolicy.retryCount, EXECUTION_OBSERVABILITY_RETRY_COUNT),
    retryDelayMs: Math.max(
      basePolicy.retryDelayMs,
      EXECUTION_OBSERVABILITY_RETRY_DELAY_MS,
    ),
    retryDelayMaxMs: Math.max(
      basePolicy.retryDelayMaxMs,
      EXECUTION_OBSERVABILITY_RETRY_DELAY_MAX_MS,
    ),
    retryDelayFactor: Math.max(
      basePolicy.retryDelayFactor,
      EXECUTION_OBSERVABILITY_RETRY_DELAY_FACTOR,
    ),
  };
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

const DB_OPERATION_CONTEXT_KEYS = [
  "data",
  "batch",
  "transaction",
  "onConflict",
  "filter",
  "fields",
  "joins",
  "sort",
  "limit",
  "offset",
  "queryMode",
  "aggregates",
  "groupBy",
] as const;

export function mergeTriggerQueryData(
  context: AnyObject,
  triggerQueryData: DbOperationPayload,
): DbOperationPayload {
  const existingQueryData =
    typeof context.queryData === "object" && context.queryData
      ? ({ ...(context.queryData as AnyObject) } as DbOperationPayload)
      : ({} as DbOperationPayload);

  for (const key of DB_OPERATION_CONTEXT_KEYS) {
    if (
      !Object.prototype.hasOwnProperty.call(existingQueryData, key) &&
      context[key] !== undefined
    ) {
      (existingQueryData as AnyObject)[key] = context[key];
    }
  }

  return {
    ...existingQueryData,
    ...triggerQueryData,
  };
}

function isNonEmptyPlainObject(
  value: unknown,
): value is Record<string, unknown> {
  return (
    !!value &&
    typeof value === "object" &&
    !Array.isArray(value) &&
    Object.keys(value as Record<string, unknown>).length > 0
  );
}

function isMissingInsertData(data: unknown): boolean {
  return (
    !data ||
    (Array.isArray(data) && data.length === 0) ||
    (typeof data === "object" &&
      !Array.isArray(data) &&
      Object.keys(data as Record<string, unknown>).length === 0)
  );
}

function resolveActorSessionDelegationSnapshot(
  context: AnyObject,
): AnyObject | null {
  const direct =
    context.__delegationRequestContext &&
    typeof context.__delegationRequestContext === "object"
      ? (context.__delegationRequestContext as AnyObject)
      : null;
  if (direct) {
    return direct;
  }

  const metadata =
    context.__metadata && typeof context.__metadata === "object"
      ? (context.__metadata as AnyObject)
      : null;
  const nested =
    metadata?.__delegationRequestContext &&
    typeof metadata.__delegationRequestContext === "object"
      ? (metadata.__delegationRequestContext as AnyObject)
      : null;
  return nested;
}

export function recoverActorSessionInsertPayload(
  context: AnyObject,
  payload: DbOperationPayload,
): DbOperationPayload {
  if (!isMissingInsertData(payload.data)) {
    return payload;
  }

  const snapshot = resolveActorSessionDelegationSnapshot(context);
  if (!snapshot) {
    return payload;
  }

  const snapshotQueryData =
    snapshot.queryData && typeof snapshot.queryData === "object"
      ? ({ ...(snapshot.queryData as AnyObject) } as DbOperationPayload)
      : ({} as DbOperationPayload);
  const snapshotData = isNonEmptyPlainObject(snapshotQueryData.data)
    ? (snapshotQueryData.data as Record<string, unknown>)
    : isNonEmptyPlainObject(snapshot.data)
      ? (snapshot.data as Record<string, unknown>)
      : null;

  if (!snapshotData) {
    return payload;
  }

  const recoveredPayload: DbOperationPayload = {
    ...snapshotQueryData,
    ...payload,
    data: snapshotData,
  };

  if (recoveredPayload.onConflict === undefined && snapshotQueryData.onConflict) {
    recoveredPayload.onConflict = snapshotQueryData.onConflict;
  }

  return recoveredPayload;
}

export function resolveOperationPayload(context: AnyObject): DbOperationPayload {
  const queryData =
    typeof context.queryData === "object" && context.queryData
      ? (context.queryData as DbOperationPayload)
      : ({} as DbOperationPayload);

  return mergeTriggerQueryData(context, queryData);
}

function buildAddConstraintIfMissingStatement(
  tableName: string,
  constraintName: string,
  constraintDefinition: string,
): string {
  const escapedConstraintName = constraintName.replace(/'/g, "''");
  const escapedTableName = tableName.replace(/'/g, "''");

  return `DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = '${escapedConstraintName}' AND conrelid = '${escapedTableName}'::regclass) THEN ALTER TABLE ${tableName} ADD CONSTRAINT ${constraintName} ${constraintDefinition}; END IF; END $$;`;
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

  private readonly registrationsByActorName: Map<string, PostgresActorRegistration> =
    new Map();
  private readonly registrationsByActorToken: Map<string, PostgresActorRegistration> =
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
        const registration = this.resolveRegistration(ctx);
        if (!registration) {
          return ctx;
        }

        this.requestPostgresActorSetup(registration, ctx);
        return ctx;
      },
      "Routes generic database init requests to actor-scoped setup signal.",
      { isMeta: true, isSubMeta: true },
    ).doOn("meta.database_init_requested");
  }

  reset() {
    for (const registration of this.registrationsByActorName.values()) {
      const runtimeState = registration.actor.getRuntimeState(registration.actorKey);
      if (runtimeState?.pool) {
        runtimeState.pool.end().catch(() => undefined);
      }
    }

    this.registrationsByActorName.clear();
    this.registrationsByActorToken.clear();
    this.adminDbClient.end().catch(() => undefined);
  }

  private resolveExecutionObservabilityRegistration():
    | PostgresActorRegistration
    | undefined {
    const localServiceName = Cadenza.serviceRegistry?.serviceName ?? null;

    for (const registration of this.registrationsByActorName.values()) {
      if (
        registration.ownerServiceName !== localServiceName ||
        !registration.schema?.tables?.execution_trace ||
        !registration.schema?.tables?.routine_execution ||
        !registration.schema?.tables?.task_execution
      ) {
        continue;
      }

      return registration;
    }

    return undefined;
  }

  private buildExecutionObservabilityOnConflict(
    tableName: string,
  ): { target: string[]; action: OnConflictAction } | undefined {
    return {
      target: ["uuid"],
      action: {
        do: "nothing",
      },
    };
  }

  public async persistExecutionObservabilityInsert(
    tableName: string,
    data: Record<string, any>,
  ): Promise<any> {
    const registration = this.resolveExecutionObservabilityRegistration();
    if (!registration) {
      return {
        rowCount: 0,
        errored: true,
        __error:
          "Execution observability registration is unavailable on this service.",
        __success: false,
      };
    }

    return this.insertFunction(registration, tableName, {
      data,
      onConflict: this.buildExecutionObservabilityOnConflict(tableName),
    });
  }

  public async persistExecutionObservabilityUpdate(
    tableName: string,
    data: Record<string, any>,
    filter: Record<string, any>,
  ): Promise<any> {
    const registration = this.resolveExecutionObservabilityRegistration();
    if (!registration) {
      return {
        rowCount: 0,
        errored: true,
        __error:
          "Execution observability registration is unavailable on this service.",
        __success: false,
      };
    }

    return this.updateFunction(registration, tableName, {
      data,
      filter,
    });
  }

  createPostgresActor(
    name: string,
    schema: DatabaseSchemaDefinition,
    description: string,
    options: ServerOptions & DatabaseOptions,
  ): PostgresActorRegistration {
    const actorName = buildPostgresActorName(name);
    const existing = this.registrationsByActorName.get(actorName);
    if (existing) {
      return existing;
    }

    const actorToken = normalizeIntentToken(actorName);
    const actorKey = String(options.databaseName ?? snakeCase(name));
    const ownerServiceName =
      options.ownerServiceName ?? Cadenza.serviceRegistry?.serviceName ?? null;

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
          description ||
          "Specialized PostgresActor owning pool runtime state and schema-driven DB task generation.",
        defaultKey: actorKey,
        keyResolver: (input: Record<string, any>) =>
          typeof input.databaseName === "string" ? input.databaseName : undefined,
        loadPolicy: "eager",
        writeContract: "overwrite",
        initState: {
          actorName,
          actorToken,
          ownerServiceName,
          databaseName: actorKey,
          status: "idle",
          schemaVersion: Number(schema.version ?? 1),
          schemaVersionTarget: Number(schema.version ?? 1),
          schemaVersionApplied: 0,
          migrationStatus: "idle",
          lastMigrationVersion: null,
          lastMigrationName: null,
          lastMigrationChecksum: null,
          lastMigrationAppliedAt: null,
          lastMigrationError: null,
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
      ownerServiceName,
      databaseName: actorKey,
      actorName,
      actorToken,
      actorKey,
      setupSignal: `meta.postgres_actor.setup_requested.${actorToken}`,
      setupDoneSignal: `meta.postgres_actor.setup_done.${actorToken}`,
      setupFailedSignal: `meta.postgres_actor.setup_failed.${actorToken}`,
      actor,
      schema,
      description,
      options,
      tasksGenerated: false,
      intentNames: new Set(),
    };

    this.registrationsByActorName.set(actorName, registration);
    this.registrationsByActorToken.set(actorToken, registration);
    this.registerSetupTask(registration);

    return registration;
  }

  requestPostgresActorSetup(
    registrationOrName: PostgresActorRegistration | string,
    ctx: AnyObject = {},
  ): PostgresActorRegistration | undefined {
    const registration =
      typeof registrationOrName === "string"
        ? this.resolveRegistration({
            actorName: registrationOrName,
          })
        : registrationOrName;

    if (!registration) {
      return undefined;
    }

    const payload = {
      ...ctx,
      actorName: registration.actorName,
      actorToken: registration.actorToken,
      databaseName: registration.databaseName,
      ownerServiceName: registration.ownerServiceName,
      options: {
        ...(ctx.options ?? {}),
        actorName: registration.actorName,
        actorToken: registration.actorToken,
        ownerServiceName: registration.ownerServiceName,
        databaseName: registration.databaseName,
      },
    };

    const runtimeState = registration.actor.getRuntimeState(registration.actorKey);
    if (runtimeState?.ready) {
      logPostgresSetupDebug("setup_already_ready", {
        actorName: registration.actorName,
        databaseName: registration.databaseName,
      });
      this.emitSetupDone(registration, payload);
      return registration;
    }

    logPostgresSetupDebug("emit_setup_requested", {
      actorName: registration.actorName,
      databaseName: registration.databaseName,
      ownerServiceName: registration.ownerServiceName,
      payloadKeys: Object.keys(payload),
    });
    Cadenza.emit(registration.setupSignal, payload);
    return registration;
  }

  private resolveRegistration(ctx: AnyObject): PostgresActorRegistration | undefined {
    const rawActorToken = String(
      ctx.options?.actorToken ?? ctx.actorToken ?? "",
    ).trim();
    if (rawActorToken) {
      const actorToken = normalizeIntentToken(rawActorToken);
      const registration = this.registrationsByActorToken.get(actorToken);
      if (registration) {
        return registration;
      }
    }

    const rawActorName = String(
      ctx.options?.actorName ??
        ctx.actorName ??
        ctx.options?.postgresActorName ??
        ctx.postgresActorName ??
        "",
    ).trim();
    if (rawActorName) {
      const registration =
        this.registrationsByActorName.get(rawActorName) ??
        this.registrationsByActorName.get(buildPostgresActorName(rawActorName));
      if (registration) {
        return registration;
      }
    }

    const legacyServiceName = String(
      ctx.options?.serviceName ?? ctx.serviceName ?? "",
    ).trim();
    if (legacyServiceName) {
      return this.registrationsByActorName.get(
        buildPostgresActorName(legacyServiceName),
      );
    }

    return undefined;
  }

  public async queryAuthorityTableRows(
    tableName: string,
    payload: DbOperationPayload = {},
  ): Promise<Array<Record<string, unknown>>> {
    const registration = this.resolveRegistration({
      serviceName: "CadenzaDB",
    });

    if (!registration) {
      throw new Error("CadenzaDB PostgresActor registration is not available");
    }

    if (!registration.schema.tables[tableName]) {
      throw new Error(
        `Table '${tableName}' is not registered on the CadenzaDB PostgresActor`,
      );
    }

    const result = (await this.queryFunction(
      registration,
      tableName,
      payload,
    )) as AnyObject;

    if (result?.errored) {
      throw new Error(
        typeof result.__error === "string"
          ? result.__error
          : `Query failed for table '${tableName}'`,
      );
    }

    const pluralKey = `${camelCase(tableName)}s`;
    const singularKey = camelCase(tableName);
    const rows =
      result?.[pluralKey] ??
      result?.[singularKey] ??
      result?.rows ??
      [];

    return Array.isArray(rows)
      ? rows.filter(
          (row): row is Record<string, unknown> =>
            !!row && typeof row === "object",
        )
      : [];
  }

  public async persistAuthorityInsert(
    tableName: string,
    payload: DbOperationPayload,
  ): Promise<any> {
    const registration = this.resolveRegistration({
      serviceName: "CadenzaDB",
    });

    if (!registration) {
      return {
        rowCount: 0,
        errored: true,
        __error: "CadenzaDB PostgresActor registration is not available",
        __success: false,
      };
    }

    if (!registration.schema.tables[tableName]) {
      return {
        rowCount: 0,
        errored: true,
        __error: `Table '${tableName}' is not registered on the CadenzaDB PostgresActor`,
        __success: false,
      };
    }

    return this.insertFunction(registration, tableName, payload);
  }

  private emitSetupDone(
    registration: PostgresActorRegistration,
    payload: AnyObject,
  ): void {
    const resolvedPayload = {
      ...payload,
      actorName: registration.actorName,
      actorToken: registration.actorToken,
      databaseName: registration.databaseName,
      ownerServiceName: registration.ownerServiceName,
      __success: true,
    };

    Cadenza.emit(registration.setupDoneSignal, resolvedPayload);
    Cadenza.emit("meta.postgres_actor.setup_done", resolvedPayload);
    Cadenza.emit("meta.database.setup_done", resolvedPayload);
  }

  private registerSetupTask(registration: PostgresActorRegistration): void {
    Cadenza.createMetaTask(
      `Setup ${registration.actorName}`,
      registration.actor.task(
        async ({ input, state, runtimeState, setState, setRuntimeState, emit }) => {
          logPostgresSetupDebug("setup_task_started", {
            actorName: registration.actorName,
            databaseName: registration.databaseName,
            inputKeys: Object.keys(input ?? {}),
          });
          const requestedDatabaseName = String(
            input.options?.databaseName ?? input.databaseName ?? registration.databaseName,
          );

          if (requestedDatabaseName !== registration.databaseName) {
            return input;
          }

          setState({
            ...state,
            status: "initializing",
            migrationStatus: "applying",
            schemaVersionTarget: Number(registration.schema.version ?? 1),
            setupStartedAt: new Date().toISOString(),
            lastError: null,
            lastMigrationError: null,
          });

          const priorRuntimePool = runtimeState?.pool ?? null;
          if (priorRuntimePool) {
            await priorRuntimePool.end().catch(() => undefined);
          }

          try {
            await this.createDatabaseIfMissing(requestedDatabaseName);
            logPostgresSetupDebug("database_ready", {
              actorName: registration.actorName,
              databaseName: requestedDatabaseName,
            });

            const pool = this.createTargetPool(
              requestedDatabaseName,
              state.safetyPolicy.statementTimeoutMs,
            );

            await this.checkPoolHealth(pool, state.safetyPolicy);
            logPostgresSetupDebug("pool_healthy", {
              actorName: registration.actorName,
              databaseName: requestedDatabaseName,
            });

            this.validateSchema({
              schema: registration.schema,
              options: registration.options,
            });

            const migrationResult = await this.applySchemaMigrations(
              pool,
              registration,
            );

            const sortedTables = this.sortTablesByReferences({
              schema: registration.schema,
            }).sortedTables as string[];

            const ddlStatements = this.buildSchemaDdlStatements(
              registration.schema,
              sortedTables,
            );
            await this.applyDdlStatements(pool, ddlStatements);

            for (const migration of migrationResult.pendingBaselineMigrations) {
              await this.recordMigrationStatus(
                pool,
                registration,
                migration,
                computeDatabaseMigrationChecksum(migration),
                "baselined",
                null,
              );
            }

            if (!registration.tasksGenerated) {
              this.generateDatabaseTasks(registration);
              registration.tasksGenerated = true;
              const localTasks = Array.from(Cadenza.registry.tasks.values());
              logIntentMapSetupDebug("generated_database_tasks", {
                actorName: registration.actorName,
                ownerServiceName: registration.ownerServiceName,
                totalLocalTasks: localTasks.length,
                generatedTaskNames: localTasks
                  .map((task) => task.name)
                  .filter((taskName) =>
                    /(Query|Insert|Update|Delete|COUNT|EXISTS|ONE|AGGREGATE|UPSERT) /.test(
                      taskName,
                    ),
                  )
                  .slice(0, 24),
              });
              Cadenza.schedule("meta.sync_requested", { __syncing: true }, 250);
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
              migrationStatus: "ready",
              schemaVersionTarget: Number(registration.schema.version ?? 1),
              schemaVersionApplied: migrationResult.schemaVersionApplied,
              lastMigrationVersion: migrationResult.lastMigrationVersion,
              lastMigrationName: migrationResult.lastMigrationName,
              lastMigrationChecksum: migrationResult.lastMigrationChecksum,
              lastMigrationAppliedAt: migrationResult.lastMigrationAppliedAt,
              lastMigrationError: null,
              setupCompletedAt: nowIso,
              lastHealthCheckAt: nowIso,
              lastError: null,
              tables: Object.keys(registration.schema.tables ?? {}),
            });

            this.emitSetupDone(registration, {
              ...input,
            });
            logPostgresSetupDebug("setup_task_completed", {
              actorName: registration.actorName,
              databaseName: requestedDatabaseName,
            });

            return {
              ...input,
              __success: true,
              actorName: registration.actorName,
              databaseName: registration.databaseName,
              ownerServiceName: registration.ownerServiceName,
            };
          } catch (error) {
            const message = errorMessage(error);
            logPostgresSetupDebug("setup_task_failed", {
              actorName: registration.actorName,
              databaseName: requestedDatabaseName,
              error: message,
            });
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
              migrationStatus: "error",
              setupCompletedAt: new Date().toISOString(),
              lastError: message,
              lastMigrationError: message,
            });
            throw error;
          }
        },
        { mode: "write" },
      ),
      "Initializes PostgresActor runtime pool, applies schema, and generates CRUD tasks/intents.",
      { isMeta: true },
    )
      .doOn(registration.setupSignal)
      .emitsOnFail(
        registration.setupFailedSignal,
        "meta.postgres_actor.setup_failed",
        "meta.database.setup_failed",
      );
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
        const transient = isTransientDatabaseError(error, operationLabel);
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
      if (tableName === "actor_session_state") {
        const rawContext = context as Record<string, unknown>;
        const rawMetadata =
          rawContext.__metadata && typeof rawContext.__metadata === "object"
            ? (rawContext.__metadata as Record<string, unknown>)
            : undefined;
        console.warn("[CADENZA_ACTOR_SESSION_EMPTY_INSERT]", {
          tableName,
          contextKeys: Object.keys(context ?? {}),
          queryDataKeys: [],
          hasDelegationSnapshot:
            rawContext.__delegationRequestContext !== undefined ||
            rawMetadata?.__delegationRequestContext !== undefined,
          localTaskName:
            typeof rawContext.__localTaskName === "string"
              ? rawContext.__localTaskName
              : undefined,
          remoteRoutineName:
            typeof rawContext.__remoteRoutineName === "string"
              ? rawContext.__remoteRoutineName
              : undefined,
          serviceName:
            typeof rawContext.__serviceName === "string"
              ? rawContext.__serviceName
              : undefined,
          actorName:
            typeof rawContext.actor_name === "string"
              ? rawContext.actor_name
              : undefined,
          actorKey:
            typeof rawContext.actor_key === "string"
              ? rawContext.actor_key
              : undefined,
          durableVersion: rawContext.durable_version,
          metadata: rawMetadata,
        });
      }

      return {
        rowCount: 0,
        errored: true,
        __error: "No data provided for insert",
        __success: false,
      };
    }

    const pool = this.getPoolOrThrow(registration);
    const safetyPolicy = resolveExecutionObservabilitySafetyPolicyForTable(
      this.resolveSafetyPolicy(registration),
      tableName,
    );

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
            const tableFields = registration.schema.tables[tableName]?.fields ?? {};
            const sqlPrefix = `INSERT INTO ${tableName} (${keys
              .map((key) => snakeCase(key))
              .join(", ")}) VALUES `;

            const params: any[] = [];
            const placeholders = rows
              .map((row) => {
                const tuple = keys
                  .map((key) => {
                    params.push(
                      serializeFieldValueForQuery(
                        row[key],
                        this.getFieldDefinitionForKey(tableFields, key),
                      ),
                    );
                    return `$${params.length}`;
                  })
                  .join(", ");
                return `(${tuple})`;
              })
              .join(", ");

            let onConflictSql = "";
            if (onConflict) {
              onConflictSql = this.buildOnConflictClause(
                onConflict,
                params,
                tableFields,
              );
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
        __transientDatabaseError: isTransientDatabaseError(error, `Insert ${tableName}`),
        __retryableExecutionObservabilityForeignKeyError:
          isRetryableExecutionObservabilityForeignKeyError(error, `Insert ${tableName}`),
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
    const safetyPolicy = resolveExecutionObservabilitySafetyPolicyForTable(
      this.resolveSafetyPolicy(registration),
      tableName,
    );

    try {
      return await this.runWithRetries(
        async () =>
          this.executeWithTransaction(pool, Boolean(transaction), async (client) => {
            const resolvedData = await this.resolveNestedData(
              registration,
              data,
              tableName,
            );
            const tableFields = registration.schema.tables[tableName]?.fields ?? {};
            const params: any[] = [];

            const setClause = Object.keys(resolvedData)
              .map((key) => {
                const value: any = (resolvedData as AnyObject)[key];
                if (value?.__effect === "increment") {
                  return `${snakeCase(key)} = ${snakeCase(key)} + 1`;
                }
                if (value?.__effect === "decrement") {
                  return `${snakeCase(key)} = ${snakeCase(key)} - 1`;
                }
                if (value?.__effect === "set") {
                  return `${snakeCase(key)} = ${value.__value}`;
                }

                params.push(
                  serializeFieldValueForQuery(
                    value,
                    this.getFieldDefinitionForKey(tableFields, key),
                  ),
                );
                return `${snakeCase(key)} = $${params.length}`;
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
            if (
              rowCount === 0 &&
              shouldRetryExecutionObservabilityMissingUpdate(tableName, filter)
            ) {
              const retryError = new Error(
                `Execution observability update matched no rows for ${tableName}`,
              ) as Error & { code?: string; table?: string };
              retryError.code = "EXEC_OBSERVABILITY_UPDATE_MISSING_ROW";
              retryError.table = tableName;
              throw retryError;
            }

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
    tableFields: TableDefinition["fields"],
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

        params.push(
          serializeFieldValueForQuery(
            value,
            this.getFieldDefinitionForKey(tableFields, field),
          ),
        );
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

  private getFieldDefinitionForKey(
    fields: TableDefinition["fields"],
    key: string,
  ): FieldDefinition | undefined {
    return fields[key] ?? fields[snakeCase(key)];
  }

  /**
   * Validates database schema structure and content.
   */
  validateSchema(ctx: AnyObject): true {
    const schema = ctx.schema as DatabaseSchemaDefinition;
    if (!schema?.tables || typeof schema.tables !== "object") {
      throw new Error("Invalid schema: missing or invalid tables");
    }

    const schemaVersion = normalizePositiveInteger(schema.version, 1, 1);

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

    if (schema.migrations !== undefined && !Array.isArray(schema.migrations)) {
      throw new Error("Invalid schema: migrations must be an array when provided");
    }

    const seenMigrationVersions = new Set<number>();
    let previousVersion = 0;
    for (const migration of schema.migrations ?? []) {
      const version = normalizePositiveInteger(migration.version, 0, 1);
      if (version === 0) {
        throw new Error("Migration versions must be positive integers");
      }
      if (version > schemaVersion) {
        throw new Error(
          `Migration version ${version} exceeds schema version ${schemaVersion}`,
        );
      }
      if (seenMigrationVersions.has(version)) {
        throw new Error(`Duplicate migration version ${version}`);
      }
      if (version <= previousVersion) {
        throw new Error("Migrations must be ordered by strictly increasing version");
      }
      if (typeof migration.name !== "string" || migration.name.trim().length === 0) {
        throw new Error(`Migration ${version} must have a non-empty name`);
      }
      if (!Array.isArray(migration.steps) || migration.steps.length === 0) {
        throw new Error(`Migration ${version} must define at least one step`);
      }

      for (const step of migration.steps) {
        this.validateMigrationStep(step);
      }

      seenMigrationVersions.add(version);
      previousVersion = version;
    }

    return true;
  }

  private validateMigrationStep(step: DatabaseMigrationStep): void {
    switch (step.kind) {
      case "createTable":
        if (!isSqlIdentifier(step.table)) {
          throw new Error(`Invalid migration table name ${step.table}`);
        }
        if (!step.definition || typeof step.definition !== "object") {
          throw new Error(`Migration createTable for ${step.table} is missing definition`);
        }
        return;
      case "dropTable":
        if (!isSqlIdentifier(step.table)) {
          throw new Error(`Invalid migration table name ${step.table}`);
        }
        return;
      case "addColumn":
      case "dropColumn":
        if (!isSqlIdentifier(step.table) || !isSqlIdentifier(step.column)) {
          throw new Error(`Invalid migration column reference ${step.table}.${step.column}`);
        }
        return;
      case "alterColumn":
        if (!isSqlIdentifier(step.table) || !isSqlIdentifier(step.column)) {
          throw new Error(`Invalid migration column reference ${step.table}.${step.column}`);
        }
        if (
          step.setType === undefined &&
          step.setDefault === undefined &&
          step.dropDefault !== true &&
          step.setNotNull !== true &&
          step.dropNotNull !== true
        ) {
          throw new Error(
            `Migration alterColumn for ${step.table}.${step.column} must change at least one property`,
          );
        }
        return;
      case "renameColumn":
        if (
          !isSqlIdentifier(step.table) ||
          !isSqlIdentifier(step.from) ||
          !isSqlIdentifier(step.to)
        ) {
          throw new Error(`Invalid renameColumn step for ${step.table}`);
        }
        return;
      case "renameTable":
        if (!isSqlIdentifier(step.from) || !isSqlIdentifier(step.to)) {
          throw new Error(`Invalid renameTable step from ${step.from} to ${step.to}`);
        }
        return;
      case "addIndex":
        if (!isSqlIdentifier(step.table)) {
          throw new Error(`Invalid addIndex table name ${step.table}`);
        }
        if (!Array.isArray(step.fields) || step.fields.length === 0) {
          throw new Error(`Migration addIndex for ${step.table} must define fields`);
        }
        if (step.name && !isSqlIdentifier(step.name)) {
          throw new Error(`Invalid migration index name ${step.name}`);
        }
        return;
      case "dropIndex":
        if (step.table && !isSqlIdentifier(step.table)) {
          throw new Error(`Invalid dropIndex table name ${step.table}`);
        }
        if (step.name && !isSqlIdentifier(step.name)) {
          throw new Error(`Invalid migration index name ${step.name}`);
        }
        if (!step.name && (!Array.isArray(step.fields) || step.fields.length === 0)) {
          throw new Error("dropIndex requires either a name or table fields");
        }
        return;
      case "addConstraint":
        if (!isSqlIdentifier(step.table) || !isSqlIdentifier(step.name)) {
          throw new Error(`Invalid addConstraint target ${step.table}.${step.name}`);
        }
        this.validateMigrationConstraint(step.definition);
        return;
      case "dropConstraint":
        if (!isSqlIdentifier(step.table) || !isSqlIdentifier(step.name)) {
          throw new Error(`Invalid dropConstraint target ${step.table}.${step.name}`);
        }
        return;
      case "sql":
        if (
          !(
            typeof step.sql === "string" ||
            (Array.isArray(step.sql) && step.sql.every((statement) => typeof statement === "string"))
          )
        ) {
          throw new Error("Migration sql step must contain a string or string[]");
        }
        return;
      default: {
        const exhaustive: never = step;
        throw new Error(`Unsupported migration step ${(exhaustive as any).kind}`);
      }
    }
  }

  private validateMigrationConstraint(
    definition: DatabaseMigrationConstraintDefinition,
  ): void {
    switch (definition.kind) {
      case "primaryKey":
      case "unique":
        if (!Array.isArray(definition.fields) || definition.fields.length === 0) {
          throw new Error(`Migration constraint ${definition.kind} requires fields`);
        }
        return;
      case "foreignKey":
        if (
          !Array.isArray(definition.fields) ||
          definition.fields.length === 0 ||
          !isSqlIdentifier(definition.referenceTable) ||
          !Array.isArray(definition.referenceFields) ||
          definition.referenceFields.length === 0
        ) {
          throw new Error("Migration foreignKey constraint is invalid");
        }
        return;
      case "check":
        if (typeof definition.expression !== "string" || definition.expression.trim().length === 0) {
          throw new Error("Migration check constraint requires an expression");
        }
        return;
      case "sql":
        if (typeof definition.sql !== "string" || definition.sql.trim().length === 0) {
          throw new Error("Migration sql constraint requires SQL");
        }
        return;
      default: {
        const exhaustive: never = definition;
        throw new Error(`Unsupported constraint kind ${(exhaustive as any).kind}`);
      }
    }
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
      ddl.push(...this.buildTableDdlStatements(tableName, schema.tables[tableName]));
    }

    return ddl;
  }

  private buildTableDdlStatements(
    tableName: string,
    table: TableDefinition,
  ): string[] {
    const ddl: string[] = [];
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
      const primaryKeyName = `pk_${tableName}_${table.primaryKey.join("_")}`;
      ddl.push(
        buildAddConstraintIfMissingStatement(
          tableName,
          primaryKeyName,
          `PRIMARY KEY (${table.primaryKey.map(snakeCase).join(", ")})`,
        ),
      );
    }

    for (const uniqueFields of table.uniqueConstraints ?? []) {
      const uniqueConstraintName = `uq_${tableName}_${uniqueFields.join("_")}`;
      ddl.push(
        buildAddConstraintIfMissingStatement(
          tableName,
          uniqueConstraintName,
          `UNIQUE (${uniqueFields.map(snakeCase).join(", ")})`,
        ),
      );
    }

    for (const foreignKey of table.foreignKeys ?? []) {
      const fkName = `fk_${tableName}_${foreignKey.fields.join("_")}`;
      ddl.push(
        buildAddConstraintIfMissingStatement(
          tableName,
          fkName,
          `FOREIGN KEY (${foreignKey.fields
            .map(snakeCase)
            .join(", ")}) REFERENCES ${foreignKey.tableName} (${foreignKey.referenceFields
            .map(snakeCase)
            .join(", ")})`,
        ),
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
                .map((value, index) =>
                  serializeInitialDataValueForSql(
                    value,
                    table.fields[table.initialData!.fields[index]],
                  ),
                )
                .join(", ")})`,
          )
          .join(", ")} ON CONFLICT DO NOTHING;`,
      );
    }

    return ddl;
  }

  private buildMigrationLedgerDdlStatements(): string[] {
    return [
      `CREATE TABLE IF NOT EXISTS ${SCHEMA_MIGRATION_LEDGER_TABLE} (schema_name VARCHAR(255) NOT NULL, actor_name VARCHAR(255) NOT NULL, version INT NOT NULL, name VARCHAR(255) NOT NULL, checksum TEXT NOT NULL, applied_at TIMESTAMP DEFAULT now(), status VARCHAR(32) NOT NULL DEFAULT 'applied', error_message TEXT DEFAULT NULL, PRIMARY KEY (schema_name, actor_name, version));`,
      `CREATE INDEX IF NOT EXISTS idx_${SCHEMA_MIGRATION_LEDGER_TABLE}_actor_name ON ${SCHEMA_MIGRATION_LEDGER_TABLE} (actor_name);`,
    ];
  }

  private async ensureMigrationLedger(pool: Pool): Promise<void> {
    await this.applyDdlStatements(pool, this.buildMigrationLedgerDdlStatements());
  }

  private async listUserTables(pool: Pool): Promise<string[]> {
    const result = await pool.query<{
      table_name: string;
    }>(
      `SELECT tablename AS table_name FROM pg_tables WHERE schemaname = 'public';`,
    );

    return result.rows
      .map((row) => String(row.table_name ?? "").trim())
      .filter(Boolean);
  }

  private async listAppliedMigrations(
    pool: Pool,
    registration: PostgresActorRegistration,
  ): Promise<AppliedMigrationRecord[]> {
    const result = await pool.query<{
      version: number;
      name: string;
      checksum: string;
      status: string;
      applied_at: Date | string | null;
      error_message: string | null;
    }>(
      `SELECT version, name, checksum, status, applied_at, error_message
       FROM ${SCHEMA_MIGRATION_LEDGER_TABLE}
       WHERE schema_name = $1 AND actor_name = $2
       ORDER BY version ASC;`,
      [registration.databaseName, registration.actorName],
    );

    return result.rows.map((row) => ({
      version: Number(row.version),
      name: String(row.name ?? ""),
      checksum: String(row.checksum ?? ""),
      status: String(row.status ?? ""),
      appliedAt: row.applied_at ? new Date(row.applied_at).toISOString() : null,
      errorMessage:
        typeof row.error_message === "string" ? row.error_message : null,
    }));
  }

  private async recordMigrationStatus(
    pool: Pool,
    registration: PostgresActorRegistration,
    migration: DatabaseMigrationDefinition,
    checksum: string,
    status: "applied" | "baselined" | "failed",
    error: string | null = null,
  ): Promise<void> {
    await pool.query(
      `INSERT INTO ${SCHEMA_MIGRATION_LEDGER_TABLE}
        (schema_name, actor_name, version, name, checksum, applied_at, status, error_message)
       VALUES ($1, $2, $3, $4, $5, now(), $6, $7)
       ON CONFLICT (schema_name, actor_name, version)
       DO UPDATE SET
         name = EXCLUDED.name,
         checksum = EXCLUDED.checksum,
         applied_at = EXCLUDED.applied_at,
         status = EXCLUDED.status,
         error_message = EXCLUDED.error_message;`,
      [
        registration.databaseName,
        registration.actorName,
        migration.version,
        migration.name,
        checksum,
        status,
        error,
      ],
    );
  }

  private buildMigrationConstraintSql(
    definition: DatabaseMigrationConstraintDefinition,
  ): string {
    switch (definition.kind) {
      case "primaryKey":
        return `PRIMARY KEY (${definition.fields.map(snakeCase).join(", ")})`;
      case "unique":
        return `UNIQUE (${definition.fields.map(snakeCase).join(", ")})`;
      case "foreignKey":
        return `FOREIGN KEY (${definition.fields
          .map(snakeCase)
          .join(", ")}) REFERENCES ${definition.referenceTable} (${definition.referenceFields
          .map(snakeCase)
          .join(", ")})${definition.onDelete ? ` ON DELETE ${definition.onDelete.toUpperCase()}` : ""}`;
      case "check":
        return `CHECK (${definition.expression})`;
      case "sql":
        return definition.sql;
    }
  }

  private buildMigrationStepStatements(
    step: DatabaseMigrationStep,
  ): string[] {
    switch (step.kind) {
      case "createTable":
        return this.buildTableDdlStatements(step.table, step.definition);
      case "dropTable":
        return [
          `DROP TABLE ${step.ifExists === false ? "" : "IF EXISTS "}${step.table}${step.cascade ? " CASCADE" : ""};`,
        ];
      case "addColumn":
        return [
          `ALTER TABLE ${step.table} ADD COLUMN ${step.ifNotExists === false ? "" : "IF NOT EXISTS "}${this.fieldDefinitionToSql(step.column, step.definition)};`,
        ];
      case "dropColumn":
        return [
          `ALTER TABLE ${step.table} DROP COLUMN ${step.ifExists === false ? "" : "IF EXISTS "}${snakeCase(step.column)}${step.cascade ? " CASCADE" : ""};`,
        ];
      case "alterColumn": {
        const statements: string[] = [];
        if (step.setType) {
          statements.push(
            `ALTER TABLE ${step.table} ALTER COLUMN ${snakeCase(step.column)} TYPE ${step.setType.toUpperCase()}${step.using ? ` USING ${step.using}` : ""};`,
          );
        }
        if (step.dropDefault) {
          statements.push(
            `ALTER TABLE ${step.table} ALTER COLUMN ${snakeCase(step.column)} DROP DEFAULT;`,
          );
        } else if (step.setDefault !== undefined) {
          statements.push(
            `ALTER TABLE ${step.table} ALTER COLUMN ${snakeCase(step.column)} SET DEFAULT ${serializeFieldDefaultForSql(step.setDefault, { type: step.setType ?? "text" } as FieldDefinition)};`,
          );
        }
        if (step.setNotNull) {
          statements.push(
            `ALTER TABLE ${step.table} ALTER COLUMN ${snakeCase(step.column)} SET NOT NULL;`,
          );
        }
        if (step.dropNotNull) {
          statements.push(
            `ALTER TABLE ${step.table} ALTER COLUMN ${snakeCase(step.column)} DROP NOT NULL;`,
          );
        }
        return statements;
      }
      case "renameColumn":
        return [
          `ALTER TABLE ${step.table} RENAME COLUMN ${snakeCase(step.from)} TO ${snakeCase(step.to)};`,
        ];
      case "renameTable":
        return [`ALTER TABLE ${step.from} RENAME TO ${step.to};`];
      case "addIndex": {
        const indexName =
          step.name ?? `idx_${step.table}_${step.fields.map(snakeCase).join("_")}`;
        return [
          `CREATE ${step.unique ? "UNIQUE " : ""}INDEX IF NOT EXISTS ${indexName} ON ${step.table} (${step.fields
            .map(snakeCase)
            .join(", ")});`,
        ];
      }
      case "dropIndex": {
        const indexName =
          step.name ??
          (step.table && step.fields
            ? `idx_${step.table}_${step.fields.map(snakeCase).join("_")}`
            : null);
        if (!indexName) {
          throw new Error("dropIndex requires either a name or table+fields");
        }
        return [`DROP INDEX ${step.ifExists === false ? "" : "IF EXISTS "}${indexName};`];
      }
      case "addConstraint":
        return [
          buildAddConstraintIfMissingStatement(
            step.table,
            step.name,
            this.buildMigrationConstraintSql(step.definition),
          ),
        ];
      case "dropConstraint":
        return [
          `ALTER TABLE ${step.table} DROP CONSTRAINT ${step.ifExists === false ? "" : "IF EXISTS "}${step.name};`,
        ];
      case "sql":
        return Array.isArray(step.sql) ? step.sql : [step.sql];
    }
  }

  private async executeSqlStatements(
    client: Pool | PoolClient,
    statements: string[],
  ): Promise<void> {
    for (const sql of statements) {
      await client.query(sql);
    }
  }

  private async applySchemaMigrations(
    pool: Pool,
    registration: PostgresActorRegistration,
  ): Promise<{
    schemaVersionApplied: number;
    lastMigrationVersion: number | null;
    lastMigrationName: string | null;
    lastMigrationChecksum: string | null;
    lastMigrationAppliedAt: string | null;
    pendingBaselineMigrations: DatabaseMigrationDefinition[];
  }> {
    const migrations = registration.schema.migrations ?? [];
    const schemaVersionTarget = normalizePositiveInteger(
      registration.schema.version,
      1,
      1,
    );
    await this.ensureMigrationLedger(pool);
    if (migrations.length === 0) {
      return {
        schemaVersionApplied: schemaVersionTarget,
        lastMigrationVersion: null,
        lastMigrationName: null,
        lastMigrationChecksum: null,
        lastMigrationAppliedAt: null,
        pendingBaselineMigrations: [],
      };
    }

    const migrationPolicy = resolveDatabaseMigrationPolicy(registration.schema);
    const existingTables = await this.listUserTables(pool);
    const hasUserTables = existingTables.some(
      (tableName) => tableName !== SCHEMA_MIGRATION_LEDGER_TABLE,
    );
    const appliedRows = await this.listAppliedMigrations(pool, registration);
    const appliedByVersion = new Map<number, AppliedMigrationRecord>();
    for (const row of appliedRows) {
      appliedByVersion.set(row.version, row);
    }

    for (const migration of migrations) {
      const checksum = computeDatabaseMigrationChecksum(migration);
      const existing = appliedByVersion.get(migration.version);
      if (!existing) {
        continue;
      }
      if (existing.name !== migration.name || existing.checksum !== checksum) {
        throw new Error(
          `Migration drift detected for ${registration.actorName} version ${migration.version}`,
        );
      }
    }

    if (hasUserTables && appliedRows.length === 0) {
      const adoptExistingVersion = migrationPolicy.adoptExistingVersion;

      if (adoptExistingVersion !== null) {
        const adoptTarget = normalizePositiveInteger(
          adoptExistingVersion,
          schemaVersionTarget,
          1,
        );
        const adoptedMigrations = migrations.filter(
          (migration) => migration.version <= adoptTarget,
        );

        if (
          adoptedMigrations.length === 0 ||
          !adoptedMigrations.some(
            (migration) => migration.version === adoptTarget,
          )
        ) {
          throw new Error(
            `Migration adoption version ${adoptTarget} is not defined for ${registration.actorName}`,
          );
        }

        for (const migration of adoptedMigrations) {
          const checksum = computeDatabaseMigrationChecksum(migration);
          await this.recordMigrationStatus(
            pool,
            registration,
            migration,
            checksum,
            "baselined",
            null,
          );
          appliedByVersion.set(migration.version, {
            version: migration.version,
            name: migration.name,
            checksum,
            status: "baselined",
            appliedAt: new Date().toISOString(),
            errorMessage: null,
          });
        }
      } else {
        throw new Error(
          `Database ${registration.databaseName} already has tables but no migration ledger entries for ${registration.actorName}`,
        );
      }
    }

    if (!hasUserTables && migrationPolicy.baselineOnEmpty) {
      const lastMigration = migrations[migrations.length - 1] ?? null;
      const appliedAt = new Date().toISOString();
      for (const migration of migrations) {
        await this.recordMigrationStatus(
          pool,
          registration,
          migration,
          computeDatabaseMigrationChecksum(migration),
          "baselined",
          null,
        );
      }
      return {
        schemaVersionApplied: schemaVersionTarget,
        lastMigrationVersion: lastMigration?.version ?? null,
        lastMigrationName: lastMigration?.name ?? null,
        lastMigrationChecksum: lastMigration
          ? computeDatabaseMigrationChecksum(lastMigration)
          : null,
        lastMigrationAppliedAt: appliedAt,
        pendingBaselineMigrations: migrations,
      };
    }

    let lastMigrationVersion: number | null = null;
    let lastMigrationName: string | null = null;
    let lastMigrationChecksum: string | null = null;
    let lastMigrationAppliedAt: string | null = null;

    for (const migration of migrations) {
      if (appliedByVersion.has(migration.version)) {
        const existing = appliedByVersion.get(migration.version)!;
        lastMigrationVersion = existing.version;
        lastMigrationName = existing.name;
        lastMigrationChecksum = existing.checksum;
        lastMigrationAppliedAt = existing.appliedAt;
        continue;
      }

      const checksum = computeDatabaseMigrationChecksum(migration);
      const destructive = migration.steps.some((step) =>
        ["dropTable", "dropColumn", "dropConstraint"].includes(step.kind),
      );
      if (destructive && !migrationPolicy.allowDestructive) {
        throw new Error(
          `Migration ${migration.version} (${migration.name}) contains destructive steps but allowDestructive is false`,
        );
      }

      const transactionalMode =
        migration.transaction === "required"
          ? "per_migration"
          : migration.transaction === "none"
            ? "none"
            : migrationPolicy.transactionalMode;

      const client =
        transactionalMode === "per_migration" ? await pool.connect() : null;

      try {
        if (client) {
          await client.query("BEGIN");
        }
        for (const step of migration.steps) {
          const statements = this.buildMigrationStepStatements(step);
          await this.executeSqlStatements(client ?? pool, statements);
        }
        if (client) {
          await client.query("COMMIT");
        }
        await this.recordMigrationStatus(
          pool,
          registration,
          migration,
          checksum,
          "applied",
          null,
        );
        lastMigrationVersion = migration.version;
        lastMigrationName = migration.name;
        lastMigrationChecksum = checksum;
        lastMigrationAppliedAt = new Date().toISOString();
      } catch (error) {
        if (client) {
          await client.query("ROLLBACK").catch(() => undefined);
        }
        await this.recordMigrationStatus(
          pool,
          registration,
          migration,
          checksum,
          "failed",
          errorMessage(error),
        );
        throw error;
      } finally {
        client?.release();
      }
    }

    return {
      schemaVersionApplied: schemaVersionTarget,
      lastMigrationVersion,
      lastMigrationName,
      lastMigrationChecksum,
      lastMigrationAppliedAt,
      pendingBaselineMigrations: [],
    };
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
      definition += ` DEFAULT ${serializeFieldDefaultForSql(field.default, field)}`;
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
          validateInputContext: shouldValidateGeneratedDbTaskInput(registration),
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

    Cadenza.createTask(
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
      `Macro upsert task for ${tableName}`,
      {
        concurrency: GENERATED_POSTGRES_WRITE_TASK_CONCURRENCY,
        timeout: GENERATED_POSTGRES_WRITE_TASK_TIMEOUT_MS,
        getTagCallback: () => `upsert:${registration.actorToken}:${tableName}`,
        isMeta: registration.options.isMeta,
        isSubMeta: registration.options.isMeta,
        validateInputContext: shouldValidateGeneratedDbTaskInput(registration),
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
              context.queryData = mergeTriggerQueryData(
                context,
                trigger.queryData,
              );
              payloadModifiedByTriggers = true;
            }
          }
        }

        const initialOperationPayload = resolveOperationPayload(context);
        let operationPayload = initialOperationPayload;
        if (tableName === "actor_session_state" && op === "insert") {
          operationPayload = recoverActorSessionInsertPayload(
            context,
            operationPayload,
          );
        }
        const actorSessionInsertData = operationPayload.data;
        const actorSessionInsertMissingData = isMissingInsertData(
          actorSessionInsertData,
        );

        if (
          tableName === "actor_session_state" &&
          op === "insert" &&
          actorSessionInsertMissingData
        ) {
          logActorSessionTrace("empty_insert_payload", {
            taskName,
            localTaskName: context.__localTaskName ?? null,
            deputyExecId:
              context.__deputyExecId ??
              context.__metadata?.__deputyExecId ??
              null,
            inquirySourceTaskExecutionId:
              context.__inquirySourceTaskExecutionId ??
              context.__metadata?.__inquirySourceTaskExecutionId ??
              null,
            inquirySourceRoutineExecutionId:
              context.__inquirySourceRoutineExecutionId ??
              context.__metadata?.__inquirySourceRoutineExecutionId ??
              null,
            signalName:
              context.__signalName ??
              context.__metadata?.__signalName ??
              null,
            inquiryName:
              context.__inquiryName ??
              context.__metadata?.__inquiryName ??
              null,
            serviceName:
              context.__serviceName ??
              context.__metadata?.__serviceName ??
              null,
            localServiceName:
              context.__localServiceName ??
              context.__metadata?.__localServiceName ??
              null,
            dataKeys:
              context.data &&
              typeof context.data === "object" &&
              !Array.isArray(context.data)
                ? Object.keys(context.data as Record<string, unknown>)
                : [],
            payloadDataType:
              actorSessionInsertData === null
                ? "null"
                : Array.isArray(actorSessionInsertData)
                  ? "array"
                  : typeof actorSessionInsertData,
            payloadDataKeys:
              actorSessionInsertData &&
              typeof actorSessionInsertData === "object" &&
              !Array.isArray(actorSessionInsertData)
                ? Object.keys(actorSessionInsertData as Record<string, unknown>)
                : [],
            queryDataKeys:
              context.queryData &&
              typeof context.queryData === "object" &&
              !Array.isArray(context.queryData)
                ? Object.keys(context.queryData as Record<string, unknown>)
                : [],
            rootKeys: Object.keys(context ?? {}).slice(0, 24),
            initialPayloadMissingData: isMissingInsertData(
              initialOperationPayload.data,
            ),
          });
        }
        const shouldDebugAuthoritySync = shouldDebugAuthoritySyncPayload(
          tableName,
          operationPayload,
        );

        if (shouldDebugAuthoritySync) {
          logAuthoritySyncDebug("input", {
            tableName,
            operation: op,
            summary: buildAuthoritySyncDebugSummary(operationPayload, context),
          });
        }

        try {
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
        } catch (error) {
          if (shouldDebugAuthoritySync) {
            logAuthoritySyncDebug("validation_failed", {
              tableName,
              operation: op,
              summary: buildAuthoritySyncDebugSummary(operationPayload, context),
              error: error instanceof Error ? error.message : String(error),
            });
          }
          throw error;
        }

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

        if (shouldDebugAuthoritySync) {
          logAuthoritySyncDebug("result", {
            tableName,
            operation: op,
            summary: buildAuthoritySyncDebugSummary(operationPayload, context),
            resultKeys:
              result && typeof result === "object" ? Object.keys(result) : [],
          });
        }

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
          const isRetryableExecutionObservabilityFailure =
            context.__retryableExecutionObservabilityForeignKeyError === true;

          if (isRetryableExecutionObservabilityFailure) {
            Cadenza.log(
              `Retryable execution observability insert still failed after retries in ${taskName}`,
              {
                tableName,
                error: context.__error,
                taskName: context.data?.taskName ?? context.filter?.taskName ?? null,
                inquiryName: context.data?.name ?? null,
                executionTraceId:
                  context.data?.executionTraceId ??
                  context.data?.execution_trace_id ??
                  null,
                routineExecutionId:
                  context.data?.routineExecutionId ??
                  context.data?.routine_execution_id ??
                  null,
                taskExecutionId:
                  context.data?.uuid ?? context.data?.taskExecutionId ?? null,
              },
              "warning",
            );
          } else {
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
        }

        delete context.queryData;
        delete context.data;
        delete context.filter;
        delete context.fields;
        delete context.joins;
        delete context.sort;
        delete context.limit;
        delete context.offset;
        delete context.__transientDatabaseError;
        delete context.__retryableExecutionObservabilityForeignKeyError;

        return context;
      },
      { mode: op === "query" ? "read" : "write" },
    );

    const taskOptions = {
      isMeta: registration.options.isMeta,
      isSubMeta: registration.options.isMeta,
      validateInputContext: shouldValidateGeneratedDbTaskInput(registration),
      inputSchema: schema,
    };

    const task = (
      op === "insert"
        ? Cadenza.createTask(
            taskName,
            databaseTaskFunction,
            `Auto-generated ${op} task for ${tableName} (PostgresActor)`,
            {
              ...taskOptions,
              concurrency: resolveGeneratedInsertTaskConcurrency(tableName),
              timeout: GENERATED_POSTGRES_WRITE_TASK_TIMEOUT_MS,
              getTagCallback: (context?: AnyObject) =>
                resolveGeneratedTaskTag(
                  tableName,
                  registration.actorToken,
                  context,
                  op,
                ),
            },
          )
        : Cadenza.createThrottledTask(
            taskName,
            databaseTaskFunction,
            (context?: AnyObject) =>
              resolveGeneratedTaskTag(
                tableName,
                registration.actorToken,
                context,
                op,
              ),
            `Auto-generated ${op} task for ${tableName} (PostgresActor)`,
            taskOptions,
          )
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
      if (!registration.intentNames.has(intent.name)) {
        registration.intentNames.add(intent.name);
      }
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
        camelCasedRow[camelCase(key)] = normalizeQueryResultValue(value);
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
      if (isSubOperationPayload(value)) {
        resolved[key] = await this.executeSubOperation(registration, value);
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
        const tableFields = registration.schema.tables[operation.table]?.fields ?? {};
        const params = keys.map((key) =>
          serializeFieldValueForQuery(
            row[key],
            this.getFieldDefinitionForKey(tableFields, key),
          ),
        );
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
