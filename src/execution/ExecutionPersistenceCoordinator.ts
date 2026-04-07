export const EXECUTION_PERSISTENCE_BUNDLE_SIGNAL =
  "global.meta.execution_persistence.bundle_requested";

export type ExecutionPersistenceEnsureEntityType =
  | "execution_trace"
  | "routine_execution"
  | "signal_emission"
  | "inquiry"
  | "task_execution";

export type ExecutionPersistenceUpdateEntityType =
  | "routine_execution"
  | "task_execution"
  | "inquiry";

export type ExecutionPersistenceEntityType =
  | ExecutionPersistenceEnsureEntityType
  | ExecutionPersistenceUpdateEntityType;

export interface ExecutionPersistenceEnsureEvent {
  kind: "ensure";
  entityType: ExecutionPersistenceEnsureEntityType;
  entityId: string;
  data: Record<string, any>;
  deps: string[];
}

export interface ExecutionPersistenceUpdateEvent {
  kind: "update";
  entityType: ExecutionPersistenceUpdateEntityType;
  entityId: string;
  data: Record<string, any>;
  filter: Record<string, any>;
  deps: string[];
}

export type ExecutionPersistenceEvent =
  | ExecutionPersistenceEnsureEvent
  | ExecutionPersistenceUpdateEvent;

export interface ExecutionPersistenceBundle {
  traceId: string;
  ensures: ExecutionPersistenceEnsureEvent[];
  updates: ExecutionPersistenceUpdateEvent[];
}

function readString(value: unknown): string | null {
  return typeof value === "string" && value.trim().length > 0 ? value : null;
}

function readRecord(value: unknown): Record<string, any> | null {
  return value && typeof value === "object" && !Array.isArray(value)
    ? ({ ...(value as Record<string, any>) } as Record<string, any>)
    : null;
}

function normalizeRoutineExecutionTraceFields(
  data: Record<string, any> | null,
): Record<string, any> | null {
  if (!data) {
    return null;
  }

  const normalizedData = { ...data };
  const traceId = readString(
    normalizedData.execution_trace_id ?? normalizedData.executionTraceId,
  );
  const metaContext =
    readRecord(normalizedData.meta_context) ?? readRecord(normalizedData.metaContext);
  const isMeta =
    normalizedData.is_meta === true || normalizedData.isMeta === true;
  const serviceName = readString(
    normalizedData.service_name ?? normalizedData.serviceName,
  );
  const serviceInstanceId = readString(
    normalizedData.service_instance_id ?? normalizedData.serviceInstanceId,
  );

  if (traceId) {
    normalizedData.execution_trace_id = traceId;
  }
  if (metaContext) {
    normalizedData.meta_context = metaContext;
  }
  if (normalizedData.is_meta !== undefined || normalizedData.isMeta !== undefined) {
    normalizedData.is_meta = isMeta;
  }
  if (serviceName) {
    normalizedData.service_name = serviceName;
  }
  if (serviceInstanceId) {
    normalizedData.service_instance_id = serviceInstanceId;
  } else if (
    normalizedData.serviceInstanceId === null ||
    normalizedData.service_instance_id === null
  ) {
    normalizedData.service_instance_id = null;
  }

  delete normalizedData.executionTraceId;
  delete normalizedData.traceId;
  delete normalizedData.metaContext;
  delete normalizedData.isMeta;
  delete normalizedData.serviceName;
  delete normalizedData.serviceInstanceId;
  return normalizedData;
}

function stripExecutionTraceServiceInstanceFields(
  data: Record<string, any> | null,
): Record<string, any> | null {
  if (!data) {
    return null;
  }

  const normalizedData = { ...data };
  delete normalizedData.serviceInstanceId;
  delete normalizedData.service_instance_id;
  return normalizedData;
}

function normalizeEnsureData(
  entityType: ExecutionPersistenceEnsureEntityType,
  data: Record<string, any> | null,
): Record<string, any> | null {
  switch (entityType) {
    case "execution_trace":
      return stripExecutionTraceServiceInstanceFields(data);
    case "routine_execution":
      return normalizeRoutineExecutionTraceFields(data);
    default:
      return data;
  }
}

export function buildExecutionPersistenceDependency(
  entityType: ExecutionPersistenceEnsureEntityType,
  entityId: string | null | undefined,
): string | null {
  const normalizedEntityId = readString(entityId);
  return normalizedEntityId ? `${entityType}:${normalizedEntityId}` : null;
}

function resolveEnsureEntityId(
  entityType: ExecutionPersistenceEnsureEntityType,
  data: Record<string, any>,
): string | null {
  switch (entityType) {
    default:
      return readString(data.uuid);
  }
}

function resolveUpdateEntityId(
  _entityType: ExecutionPersistenceUpdateEntityType,
  data: Record<string, any>,
  filter: Record<string, any>,
): string | null {
  return readString(
    filter.uuid ??
      filter.taskExecutionId ??
      filter.task_execution_id ??
      data.uuid ??
      data.taskExecutionId ??
      data.task_execution_id,
  );
}

function dedupeDependencies(values: Array<string | null | undefined>): string[] {
  return Array.from(
    new Set(values.filter((value): value is string => typeof value === "string")),
  );
}

function resolveTraceIdFromData(data: Record<string, any> | null): string | null {
  if (!data) {
    return null;
  }

  return readString(
    data.traceId ??
      data.executionTraceId ??
      data.execution_trace_id ??
      data.metaContext?.__executionTraceId ??
      data.metaContext?.__metadata?.__executionTraceId ??
      data.meta_context?.__executionTraceId ??
      data.meta_context?.__metadata?.__executionTraceId,
  );
}

export function buildExecutionPersistenceEnsureEvent(
  entityType: ExecutionPersistenceEnsureEntityType,
  data: Record<string, any> | null | undefined,
  deps: Array<string | null | undefined> = [],
): ExecutionPersistenceEnsureEvent | null {
  const normalizedData = normalizeEnsureData(entityType, readRecord(data));
  if (!normalizedData) {
    return null;
  }

  const entityId = resolveEnsureEntityId(entityType, normalizedData);
  if (!entityId) {
    return null;
  }

  return {
    kind: "ensure",
    entityType,
    entityId,
    data: normalizedData,
    deps: dedupeDependencies(deps),
  };
}

export function buildExecutionPersistenceUpdateEvent(
  entityType: ExecutionPersistenceUpdateEntityType,
  data: Record<string, any> | null | undefined,
  filter: Record<string, any> | null | undefined,
  deps: Array<string | null | undefined> = [],
): ExecutionPersistenceUpdateEvent | null {
  const normalizedData = readRecord(data);
  const normalizedFilter = readRecord(filter);
  if (!normalizedData || !normalizedFilter) {
    return null;
  }

  const entityId = resolveUpdateEntityId(
    entityType,
    normalizedData,
    normalizedFilter,
  );
  if (!entityId) {
    return null;
  }

  return {
    kind: "update",
    entityType,
    entityId,
    data: normalizedData,
    filter: normalizedFilter,
    deps: dedupeDependencies(deps),
  };
}

export function createExecutionPersistenceBundle(input: {
  traceId?: string | null;
  ensures?: Array<ExecutionPersistenceEnsureEvent | null | undefined>;
  updates?: Array<ExecutionPersistenceUpdateEvent | null | undefined>;
}): ExecutionPersistenceBundle | null {
  const ensures = (input.ensures ?? []).filter(
    (event): event is ExecutionPersistenceEnsureEvent => Boolean(event),
  );
  const updates = (input.updates ?? []).filter(
    (event): event is ExecutionPersistenceUpdateEvent => Boolean(event),
  );

  if (ensures.length === 0 && updates.length === 0) {
    return null;
  }

  const traceId =
    readString(input.traceId) ??
    ensures
      .map((event) => resolveTraceIdFromData(event.data))
      .find((value): value is string => Boolean(value)) ??
    updates
      .map((event) => resolveTraceIdFromData(event.data))
      .find((value): value is string => Boolean(value));

  if (!traceId) {
    return null;
  }

  return {
    traceId,
    ensures,
    updates,
  };
}
