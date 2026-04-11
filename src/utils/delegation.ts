import type { AnyObject } from "@cadenza.io/core";
import { v4 as uuid } from "uuid";
import { stripLocalRoutinePersistenceHints } from "./routinePersistence";

const ROOT_METADATA_PASSTHROUGH_KEYS = [
  "__executionTraceId",
  "__inquiryId",
  "__inquirySourceTaskName",
  "__inquirySourceTaskVersion",
  "__inquirySourceTaskExecutionId",
  "__inquirySourceRoutineExecutionId",
] as const;
const DELEGATION_REQUEST_SNAPSHOT_KEY = "__delegationRequestContext";
const DELEGATION_FAILURE_CONTEXT_KEYS = [
  "__remoteRoutineName",
  "__serviceName",
  "__timeout",
  "__localTaskName",
  "__localTaskVersion",
  "__localServiceName",
  "__localRoutineExecId",
  "__previousTaskExecutionId",
  "__fetchId",
  "fetchId",
  "__routeKey",
  "routeKey",
  "__instance",
  "__transportId",
  "__transportOrigin",
  "__transportProtocols",
  "__transportProtocol",
  "__retries",
  "__triedInstances",
  "__delegationRequestContext",
  "__metadata",
  "serviceName",
  "serviceInstanceId",
  "serviceTransportId",
  "serviceOrigin",
  "transportProtocols",
  "transportProtocol",
] as const;

function isPlainObject(value: unknown): value is AnyObject {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return false;
  }

  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}

function cloneDelegationValue<T>(value: T): T {
  if (value instanceof Date) {
    return new Date(value.getTime()) as T;
  }

  if (Array.isArray(value)) {
    return value.map((item) => cloneDelegationValue(item)) as T;
  }

  if (isPlainObject(value)) {
    const clone: AnyObject = {};
    for (const [key, nestedValue] of Object.entries(value)) {
      clone[key] = cloneDelegationValue(nestedValue);
    }
    return clone as T;
  }

  return value;
}

function buildDelegationRequestSnapshot(context: AnyObject): AnyObject {
  const snapshot: AnyObject = {};

  for (const [key, value] of Object.entries(context)) {
    if (key === DELEGATION_REQUEST_SNAPSHOT_KEY || key === "task" || key === "routine") {
      continue;
    }

    snapshot[key] = cloneDelegationValue(value);
  }

  return snapshot;
}

export function hoistDelegationMetadataFields<T extends AnyObject>(
  input: T | undefined,
  metadataInput?: AnyObject,
): T {
  const context =
    input && typeof input === "object" ? ({ ...input } as T) : ({} as T);
  const mutableContext = context as AnyObject;
  const metadata =
    metadataInput && typeof metadataInput === "object"
      ? metadataInput
      : context.__metadata && typeof context.__metadata === "object"
        ? context.__metadata
        : {};

  for (const key of ROOT_METADATA_PASSTHROUGH_KEYS) {
    if (
      (mutableContext[key] === undefined || mutableContext[key] === null) &&
      metadata[key] !== undefined &&
      metadata[key] !== null
    ) {
      mutableContext[key] = metadata[key];
    }
  }

  return context;
}

export function attachDelegationRequestSnapshot<T extends AnyObject>(
  input: T | undefined,
): T {
  const context =
    input && typeof input === "object" ? ({ ...input } as T) : ({} as T);
  const mutableContext = context as AnyObject;

  if (
    mutableContext[DELEGATION_REQUEST_SNAPSHOT_KEY] === undefined &&
    (typeof mutableContext.__remoteRoutineName === "string" ||
      typeof mutableContext.__serviceName === "string")
  ) {
    mutableContext[DELEGATION_REQUEST_SNAPSHOT_KEY] =
      buildDelegationRequestSnapshot(mutableContext);
  }

  return context;
}

export function restoreDelegationRequestSnapshot<T extends AnyObject>(
  input: T | undefined,
): T {
  const context =
    input && typeof input === "object" ? ({ ...input } as T) : ({} as T);
  const mutableContext = context as AnyObject;
  const snapshotCandidate =
    mutableContext[DELEGATION_REQUEST_SNAPSHOT_KEY] ??
    (mutableContext.__metadata &&
    typeof mutableContext.__metadata === "object"
      ? (mutableContext.__metadata as AnyObject)[DELEGATION_REQUEST_SNAPSHOT_KEY]
      : undefined);
  const snapshot =
    snapshotCandidate && typeof snapshotCandidate === "object"
      ? (snapshotCandidate as AnyObject)
      : null;

  const looksLikeDelegationResult =
    mutableContext.__status !== undefined ||
    mutableContext.__success !== undefined ||
    mutableContext.rowCount !== undefined ||
    mutableContext.__nextNodes !== undefined ||
    mutableContext.__isDeputy === true ||
    mutableContext.errored === true ||
    mutableContext.failed === true ||
    mutableContext.timedOut === true ||
    mutableContext.__error !== undefined ||
    mutableContext.error !== undefined ||
    mutableContext.__inquiryMeta !== undefined;

  if (!snapshot || !looksLikeDelegationResult) {
    return context;
  }

  const restoredContext: AnyObject = buildDelegationRequestSnapshot(snapshot);

  if (mutableContext.__retries !== undefined) {
    restoredContext.__retries = mutableContext.__retries;
  }

  if (mutableContext.__triedInstances !== undefined) {
    restoredContext.__triedInstances = cloneDelegationValue(
      mutableContext.__triedInstances,
    );
  }

  restoredContext[DELEGATION_REQUEST_SNAPSHOT_KEY] =
    buildDelegationRequestSnapshot(snapshot);

  return restoredContext as T;
}

export function stripDelegationRequestSnapshot<T extends AnyObject>(
  input: T | undefined,
): T {
  const context =
    input && typeof input === "object" ? ({ ...input } as T) : ({} as T);
  delete (context as AnyObject)[DELEGATION_REQUEST_SNAPSHOT_KEY];
  return context;
}

export function buildDelegationFailureContext<T extends AnyObject>(
  signalName: string,
  input: T | undefined,
  error: unknown,
): T & {
  __signalName: string;
  __error: string;
  errored: true;
} {
  const source =
    input && typeof input === "object" ? ({ ...input } as AnyObject) : {};
  const slimContext: AnyObject = {};

  for (const key of DELEGATION_FAILURE_CONTEXT_KEYS) {
    if (source[key] !== undefined) {
      slimContext[key] = cloneDelegationValue(source[key]);
    }
  }

  if (
    slimContext[DELEGATION_REQUEST_SNAPSHOT_KEY] === undefined &&
    source.__metadata &&
    typeof source.__metadata === "object" &&
    (source.__metadata as AnyObject)[DELEGATION_REQUEST_SNAPSHOT_KEY] !== undefined
  ) {
    slimContext[DELEGATION_REQUEST_SNAPSHOT_KEY] = cloneDelegationValue(
      (source.__metadata as AnyObject)[DELEGATION_REQUEST_SNAPSHOT_KEY],
    );
  }

  return {
    __signalName: signalName,
    __error:
      error instanceof Error ? error.message : String(error ?? "Unknown error"),
    errored: true,
    ...slimContext,
  } as T & {
    __signalName: string;
    __error: string;
    errored: true;
  };
}

export function stripTransportSelectionRoutingContext<T extends AnyObject>(
  input: T | undefined,
): T {
  const context = stripLocalRoutinePersistenceHints(
    input && typeof input === "object" ? ({ ...input } as T) : ({} as T),
  );
  const mutableContext = context as AnyObject;

  delete mutableContext.__signalEmission;
  delete mutableContext.__signalEmissionId;
  delete mutableContext.__routineExecId;
  delete mutableContext.__traceCreatedBySignalBroker;

  if (mutableContext.__metadata && typeof mutableContext.__metadata === "object") {
    delete (mutableContext.__metadata as AnyObject).__traceCreatedBySignalBroker;
  }

  return context;
}

export function ensureDelegationContextMetadata<T extends AnyObject>(
  input: T | undefined,
): T & {
  __deputyExecId: string;
  __metadata: AnyObject;
} {
  const rawContext =
    input && typeof input === "object" ? ({ ...input } as T) : ({} as T);
  const metadata =
    rawContext.__metadata && typeof rawContext.__metadata === "object"
      ? { ...rawContext.__metadata }
      : {};
  const context = hoistDelegationMetadataFields(rawContext, metadata);
  const deputyExecId =
    typeof metadata.__deputyExecId === "string" &&
    metadata.__deputyExecId.length > 0
      ? metadata.__deputyExecId
      : typeof rawContext.__deputyExecId === "string" &&
          rawContext.__deputyExecId.length > 0
        ? context.__deputyExecId
        : uuid();

  return {
    ...context,
    __deputyExecId: deputyExecId,
    __metadata: {
      ...metadata,
      __deputyExecId: deputyExecId,
    },
  };
}
