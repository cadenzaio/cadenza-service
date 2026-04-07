import type { AnyObject } from "@cadenza.io/core";

export const LOCAL_ROUTINE_PERSISTENCE_KEYS = [
  "__traceCreatedByRunner",
  "__routineCreatedByRunner",
  "__routineName",
  "__routineVersion",
  "__routineCreatedAt",
  "__routineIsMeta",
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

export function splitRoutinePersistenceContext(
  input: AnyObject | undefined,
): {
  context: AnyObject;
  metaContext: AnyObject;
} {
  const sanitized = stripLocalRoutinePersistenceHints(input);

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
