import type { AnyObject } from "@cadenza.io/core";

export const LOCAL_ROUTINE_PERSISTENCE_KEYS = [
  "__traceCreatedByRunner",
  "__routineCreatedByRunner",
  "__routineName",
  "__routineVersion",
  "__routineCreatedAt",
  "__routineIsMeta",
] as const;

const BULKY_EXECUTION_PERSISTENCE_KEYS = [
  "rows",
  "joinedContexts",
  "serviceInstances",
  "serviceInstanceLeases",
  "serviceInstanceTransports",
  "serviceManifests",
  "tasks",
  "helpers",
  "globals",
  "signals",
  "intents",
  "actors",
  "routines",
  "directionalTaskMaps",
  "actorTaskMaps",
  "taskToRoutineMaps",
  "taskToHelperMaps",
  "helperToHelperMaps",
  "taskToGlobalMaps",
  "helperToGlobalMaps",
  "signalToTaskMaps",
  "intentToTaskMaps",
] as const;

export type RoutinePersistenceMetadata = {
  createdByRunner: boolean;
  routineName: string | null;
  routineVersion: number | null;
  routineCreatedAt: string | null;
  routineIsMeta: boolean;
};

function readHintValue(
  source: AnyObject | undefined,
  key: (typeof LOCAL_ROUTINE_PERSISTENCE_KEYS)[number],
): unknown {
  if (!source || typeof source !== "object") {
    return undefined;
  }

  if (source[key] !== undefined) {
    return source[key];
  }

  if (source.__metadata && typeof source.__metadata === "object") {
    return (source.__metadata as AnyObject)[key];
  }

  return undefined;
}

export function stripLocalRoutinePersistenceHints<T extends AnyObject>(
  input: T | undefined,
): T {
  const context = (input && typeof input === "object"
    ? { ...input }
    : {}) as AnyObject;

  for (const key of LOCAL_ROUTINE_PERSISTENCE_KEYS) {
    delete context[key];
  }

  if (context.__metadata && typeof context.__metadata === "object") {
    const nextMetadata = { ...(context.__metadata as AnyObject) };
    for (const key of LOCAL_ROUTINE_PERSISTENCE_KEYS) {
      delete nextMetadata[key];
    }
    context.__metadata = nextMetadata;
  }

  return context as T;
}

function summarizeQueryData(
  queryData: AnyObject | undefined,
): AnyObject | undefined {
  if (!queryData || typeof queryData !== "object") {
    return undefined;
  }

  const summary: AnyObject = {};

  if (queryData.data && typeof queryData.data === "object") {
    summary.dataKeys = Object.keys(queryData.data as AnyObject).sort();
  }
  if (queryData.filter && typeof queryData.filter === "object") {
    summary.filterKeys = Object.keys(queryData.filter as AnyObject).sort();
  }
  if (queryData.onConflict && typeof queryData.onConflict === "object") {
    const onConflict = queryData.onConflict as AnyObject;
    summary.onConflictTarget = Array.isArray(onConflict.target)
      ? [...onConflict.target]
      : onConflict.target ?? null;
  }

  return Object.keys(summary).length > 0 ? summary : undefined;
}

export function sanitizeExecutionPersistenceContext<T extends AnyObject>(
  input: T | undefined,
): T {
  const context = stripLocalRoutinePersistenceHints(input) as AnyObject;

  for (const key of BULKY_EXECUTION_PERSISTENCE_KEYS) {
    const value = context[key];
    if (value === undefined) {
      continue;
    }

    const countKey = `${key}Count`;
    if (context[countKey] === undefined) {
      if (Array.isArray(value)) {
        context[countKey] = value.length;
      } else if (value && typeof value === "object") {
        context[countKey] = Object.keys(value as AnyObject).length;
      }
    }

    delete context[key];
  }

  if (context.queryData && typeof context.queryData === "object") {
    const queryDataSummary = summarizeQueryData(context.queryData as AnyObject);
    if (queryDataSummary) {
      context.queryData = queryDataSummary;
    } else {
      delete context.queryData;
    }
  }

  return context as T;
}

export function sanitizeExecutionPersistenceResultPayload<T extends AnyObject>(
  input: T | undefined,
): T {
  const payload = (input && typeof input === "object"
    ? { ...input }
    : {}) as AnyObject;

  const resultContext =
    payload.resultContext && typeof payload.resultContext === "object"
      ? sanitizeExecutionPersistenceContext(
          payload.resultContext as Record<string, unknown>,
        )
      : payload.resultContext;
  const metaResultContext =
    payload.metaResultContext && typeof payload.metaResultContext === "object"
      ? sanitizeExecutionPersistenceContext(
          payload.metaResultContext as Record<string, unknown>,
        )
      : payload.metaResultContext;
  const snakeResultContext =
    payload.result_context && typeof payload.result_context === "object"
      ? sanitizeExecutionPersistenceContext(
          payload.result_context as Record<string, unknown>,
        )
      : payload.result_context;
  const snakeMetaResultContext =
    payload.meta_result_context && typeof payload.meta_result_context === "object"
      ? sanitizeExecutionPersistenceContext(
          payload.meta_result_context as Record<string, unknown>,
        )
      : payload.meta_result_context;

  if (resultContext !== undefined) {
    payload.resultContext = resultContext;
  }
  if (metaResultContext !== undefined) {
    payload.metaResultContext = metaResultContext;
  }
  if (snakeResultContext !== undefined) {
    payload.result_context = snakeResultContext;
  }
  if (snakeMetaResultContext !== undefined) {
    payload.meta_result_context = snakeMetaResultContext;
  }

  return payload as T;
}

export function splitRoutinePersistenceContext(
  input: AnyObject | undefined,
): {
  context: AnyObject;
  metaContext: AnyObject;
} {
  const sanitized = sanitizeExecutionPersistenceContext(input);

  return {
    context: Object.fromEntries(
      Object.entries(sanitized).filter(([key]) => !key.startsWith("__")),
    ),
    metaContext: Object.fromEntries(
      Object.entries(sanitized).filter(([key]) => key.startsWith("__")),
    ),
  };
}

export function resolveRoutinePersistenceMetadata(
  source: AnyObject | undefined,
): RoutinePersistenceMetadata {
  const routineVersionCandidate = readHintValue(source, "__routineVersion");

  return {
    createdByRunner: readHintValue(source, "__routineCreatedByRunner") === true,
    routineName:
      typeof readHintValue(source, "__routineName") === "string"
        ? String(readHintValue(source, "__routineName"))
        : null,
    routineVersion:
      typeof routineVersionCandidate === "number"
        ? routineVersionCandidate
        : routineVersionCandidate === null
          ? null
          : Number.isFinite(Number(routineVersionCandidate))
            ? Number(routineVersionCandidate)
            : null,
    routineCreatedAt:
      typeof readHintValue(source, "__routineCreatedAt") === "string"
        ? String(readHintValue(source, "__routineCreatedAt"))
        : null,
    routineIsMeta: readHintValue(source, "__routineIsMeta") === true,
  };
}
