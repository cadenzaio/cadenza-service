import Cadenza from "../../Cadenza";
import {
  buildExecutionPersistenceDependency,
  buildExecutionPersistenceEnsureEvent,
  buildExecutionPersistenceUpdateEvent,
  createExecutionPersistenceBundle,
  EXECUTION_PERSISTENCE_BUNDLE_SIGNAL,
} from "../../execution/ExecutionPersistenceCoordinator";
import { formatTimestamp } from "../../utils/tools";
import { isMetaIntentName } from "../../utils/inquiry";
import {
  resolveRoutinePersistenceMetadata,
  sanitizeExecutionPersistenceContext,
  sanitizeExecutionPersistenceResultPayload,
  splitRoutinePersistenceContext,
} from "../../utils/routinePersistence";
import { registerActorSessionPersistenceTasks } from "./registerActorSessionPersistence";
import type { Task } from "@cadenza.io/core";
import GraphSyncController, {
  isBootstrapLocalOnlyActorName,
} from "./GraphSyncController";

const INQUIRY_TRACE_ENABLED =
  process.env.CADENZA_INQUIRY_TRACE === "1" ||
  process.env.CADENZA_INQUIRY_TRACE === "true";
const TRACED_INQUIRY_METADATA_SIGNALS = new Set([
  "global.meta.graph_metadata.inquiry_created",
  "global.meta.graph_metadata.inquiry_updated",
]);
function createLocalGraphMetadataTask(
  name: string,
  taskFunction: (ctx: Record<string, any>, emit?: any) => any,
  description?: string,
  options: Record<string, unknown> = {},
) {
  return Cadenza.createMetaTask(name, taskFunction, description, {
    register: false,
    isHidden: true,
    ...options,
  });
}

function buildDatabaseTriggerContext(
  data?: Record<string, unknown> | null,
  filter?: Record<string, unknown> | null,
  extra: Record<string, unknown> = {},
  queryExtra: Record<string, unknown> = {},
): Record<string, unknown> {
  const nextData =
    data && typeof data === "object" ? { ...data } : undefined;
  const nextFilter =
    filter && typeof filter === "object" ? { ...filter } : undefined;
  const queryData: Record<string, unknown> = { ...queryExtra };

  if (nextData !== undefined) {
    queryData.data = nextData;
  }

  if (nextFilter !== undefined) {
    queryData.filter = nextFilter;
  }

  return {
    ...extra,
    ...(nextData !== undefined ? { data: nextData } : {}),
    ...(nextFilter !== undefined ? { filter: nextFilter } : {}),
    ...(Object.keys(queryData).length > 0 ? { queryData } : {}),
  };
}

function resolveTaskFromMetadataContext(
  ctx: Record<string, any>,
): Task | undefined {
  const taskName = String(
    ctx?.taskName ??
      ctx?.data?.taskName ??
      ctx?.data?.task_name ??
      ctx?.filter?.taskName ??
      ctx?.filter?.task_name ??
      "",
  );

  return taskName ? Cadenza.get(taskName) : undefined;
}

function resolveTaskByName(name: unknown): Task | undefined {
  const taskName = String(name ?? "");
  return taskName ? Cadenza.get(taskName) : undefined;
}

function resolveHelperFromMetadataContext(
  ctx: Record<string, any>,
): any {
  const toolRuntime = Cadenza as typeof Cadenza & {
    getHelper?: (name: string) => unknown;
  };
  const helperName = String(
    ctx?.helperName ??
      ctx?.data?.helperName ??
      ctx?.data?.helper_name ??
      ctx?.filter?.helperName ??
      ctx?.filter?.helper_name ??
      "",
  );

  return helperName ? toolRuntime.getHelper?.(helperName) : undefined;
}

function resolveGlobalFromMetadataContext(
  ctx: Record<string, any>,
): any {
  const toolRuntime = Cadenza as typeof Cadenza & {
    getGlobal?: (name: string) => unknown;
  };
  const globalName = String(
    ctx?.globalName ??
      ctx?.data?.globalName ??
      ctx?.data?.global_name ??
      ctx?.filter?.globalName ??
      ctx?.filter?.global_name ??
      "",
  );

  return globalName ? toolRuntime.getGlobal?.(globalName) : undefined;
}

function resolvePredecessorTaskFromMetadataContext(
  ctx: Record<string, any>,
): Task | undefined {
  return resolveTaskByName(
    ctx?.predecessorTaskName ??
      ctx?.data?.predecessorTaskName ??
      ctx?.data?.predecessor_task_name ??
      ctx?.filter?.predecessorTaskName ??
      ctx?.filter?.predecessor_task_name,
  );
}

function sanitizePersistedTaskSourceFields(
  task: Task | undefined,
  data: Record<string, unknown>,
): Record<string, unknown> {
  if (!task?.isMeta && !task?.isDeputy) {
    return data;
  }

  return {
    ...data,
    functionString: "",
    function_string: "",
    tagIdGetter: null,
    tag_id_getter: null,
  };
}

function shouldSkipDirectTaskMetadata(task?: Task): boolean {
  return !task || !task.register || task.isHidden || task.isDeputy;
}

function isManagedRouteRecoveryTaskError(errorMessage: unknown): boolean {
  if (typeof errorMessage !== "string") {
    return false;
  }

  return (
    errorMessage.includes("No routeable internal transport available") &&
    errorMessage.includes("Waiting for authority route updates before retrying")
  );
}

function isLocallyHandledIntentName(intentName: string): boolean {
  const observer = Cadenza.inquiryBroker.inquiryObservers.get(intentName);
  if (!observer) {
    return false;
  }

  for (const task of observer.tasks) {
    if (task.register && !task.isHidden && !task.isDeputy) {
      return true;
    }
  }

  return false;
}

function resolveIntentDefinition(intentName: string): Record<string, any> | undefined {
  const intent = Cadenza.inquiryBroker.intents.get(intentName);
  return intent && typeof intent === "object"
    ? (intent as Record<string, any>)
    : undefined;
}

function shouldSkipDirectIntentMetadata(intentName: string): boolean {
  if (!intentName) {
    return true;
  }

  if (!isLocallyHandledIntentName(intentName)) {
    return true;
  }

  if (GraphSyncController.instance.registeredIntentDefinitions.has(intentName)) {
    return true;
  }

  const intentDefinition = resolveIntentDefinition(intentName);
  return (
    intentDefinition?.registered === true ||
    intentDefinition?.registrationRequested === true
  );
}

function shouldPersistBusinessTaskExecution(task?: Task): boolean {
  return !!task && task.register && !task.isHidden && !task.isMeta && !task.isSubMeta && !task.isDeputy;
}

function shouldEmitDirectPrimitiveMetadata(): boolean {
  return Cadenza.hasCompletedBootstrapSync();
}

function shouldSkipDirectActorMetadata(actorName: unknown): boolean {
  return (
    typeof actorName === "string" &&
    actorName.trim().length > 0 &&
    isBootstrapLocalOnlyActorName(actorName.trim())
  );
}

function normalizeExecutionObservabilityServiceInstanceId(
  value: unknown,
): string | null {
  return typeof value === "string" && value.trim().length > 0 ? value : null;
}

function resolveExecutionObservabilityServiceInstanceId(
  candidate?: unknown,
): string | null {
  const localServiceInstanceId =
    normalizeExecutionObservabilityServiceInstanceId(
      Cadenza.serviceRegistry.serviceInstanceId,
    );
  const candidateServiceInstanceId =
    normalizeExecutionObservabilityServiceInstanceId(candidate);

  if (
    candidateServiceInstanceId &&
    localServiceInstanceId &&
    candidateServiceInstanceId !== localServiceInstanceId
  ) {
    return candidateServiceInstanceId;
  }

  return Cadenza.hasCompletedBootstrapSync() ? localServiceInstanceId : null;
}

function shouldPersistBusinessInquiry(ctx: Record<string, any>): boolean {
  const inquiryName = String(
    ctx?.data?.name ?? ctx?.inquiry ?? ctx?.data?.metadata?.inquiryMeta?.inquiry ?? "",
  );

  if (!inquiryName) {
    return false;
  }

  return !isMetaIntentName(inquiryName) && ctx?.data?.isMeta !== true && ctx?.data?.is_meta !== true;
}

function shouldPersistBusinessInquiryCompletion(ctx: Record<string, any>): boolean {
  const insertData =
    ctx?.insertData && typeof ctx.insertData === "object" ? ctx.insertData : null;

  if (insertData) {
    return shouldPersistBusinessInquiry({ data: insertData });
  }

  const inquiryName = String(ctx?.data?.metadata?.inquiryMeta?.inquiry ?? "");
  return inquiryName.length > 0 && !isMetaIntentName(inquiryName);
}

function shouldPersistExecutionTrace(ctx: Record<string, any>): boolean {
  return ctx?.data?.isMeta !== true && ctx?.data?.is_meta !== true;
}

function shouldPersistRoutineExecution(ctx: Record<string, any>): boolean {
  if (ctx?.data?.isMeta === true || ctx?.data?.is_meta === true) {
    return false;
  }

  const routineTask = resolveTaskByName(ctx?.data?.name);
  if (routineTask) {
    return shouldPersistBusinessTaskExecution(routineTask);
  }

  return true;
}

function shouldPersistTaskExecutionMetadata(
  ctx: Record<string, any>,
): boolean {
  const task = resolveTaskFromMetadataContext(ctx);
  return shouldPersistBusinessTaskExecution(task);
}

function shouldPersistTaskExecutionMap(
  ctx: Record<string, any>,
): boolean {
  return (
    shouldPersistBusinessTaskExecution(resolveTaskFromMetadataContext(ctx)) &&
    shouldPersistBusinessTaskExecution(resolvePredecessorTaskFromMetadataContext(ctx))
  );
}

function logInquiryMetadataTrace(
  event: string,
  signalName: string,
  ctx: Record<string, any>,
): void {
  if (!INQUIRY_TRACE_ENABLED || !TRACED_INQUIRY_METADATA_SIGNALS.has(signalName)) {
    return;
  }

  const data = ctx?.data && typeof ctx.data === "object" ? ctx.data : {};
  const filter = ctx?.filter && typeof ctx.filter === "object" ? ctx.filter : {};
  console.log("[CADENZA_INQUIRY_TRACE]", event, {
    localServiceName: Cadenza.serviceRegistry.serviceName,
    signalName,
    inquiryName:
      data.name ??
      data.metadata?.inquiryMeta?.inquiry ??
      ctx?.inquiry ??
      null,
    inquiryId: data.uuid ?? filter.uuid ?? null,
    fulfilledAt: data.fulfilledAt ?? data.fulfilled_at ?? null,
    routineExecutionId:
      data.routineExecutionId ??
      data.routine_execution_id ??
      null,
  });
}

export default class GraphMetadataController {
  private static _instance: GraphMetadataController;
  public static get instance(): GraphMetadataController {
    if (!this._instance) this._instance = new GraphMetadataController();
    return this._instance;
  }

  constructor() {
    const buildOnConflictDoNothing = (target: string[]) => ({
      target,
      action: {
        do: "nothing" as const,
      },
    });
    const observabilityInsertOnConflict = buildOnConflictDoNothing(["uuid"]);

    createLocalGraphMetadataTask("Handle task creation", (ctx) => {
      if (!shouldEmitDirectPrimitiveMetadata()) {
        return false;
      }

      const taskName = String(ctx.data?.name ?? ctx.data?.taskName ?? "");
      const task = taskName ? Cadenza.get(taskName) : undefined;
      const onConflict = buildOnConflictDoNothing([
        "name",
        "service_name",
        "version",
      ]);

      if (
        shouldSkipDirectTaskMetadata(task) ||
        task?.registered ||
        (task as any)?.registrationRequested
      ) {
        return false;
      }

      if (task) {
        (task as any).registrationRequested = true;
      }

      return buildDatabaseTriggerContext(
        sanitizePersistedTaskSourceFields(task, {
          ...ctx.data,
          serviceName: Cadenza.serviceRegistry.serviceName,
        }),
        undefined,
        { onConflict },
        { onConflict },
      );
    })
      .doOn("meta.task.created")
      .emits("global.meta.graph_metadata.task_created");

    createLocalGraphMetadataTask("Handle task update", (ctx) => {
      if (!shouldEmitDirectPrimitiveMetadata()) {
        return false;
      }

      const task = resolveTaskFromMetadataContext(ctx as Record<string, any>);

      if (shouldSkipDirectTaskMetadata(task)) {
        return false;
      }

      return buildDatabaseTriggerContext(
        (ctx.data as Record<string, unknown> | undefined) ?? undefined,
        {
          ...ctx.filter,
          serviceName: Cadenza.serviceRegistry.serviceName,
        },
      );
    })
      .doOn("meta.task.layer_index_changed", "meta.task.destroyed")
      .emits("global.meta.graph_metadata.task_updated");

    createLocalGraphMetadataTask("Handle task relationship creation", (ctx) => {
      if (!shouldEmitDirectPrimitiveMetadata()) {
        return false;
      }

      const taskName = ctx.data?.taskName ?? ctx.data?.task_name;
      const predecessorTaskName =
        ctx.data?.predecessorTaskName ?? ctx.data?.predecessor_task_name;
      const task = taskName ? Cadenza.get(taskName) : undefined;
      const predecessorTask = predecessorTaskName
        ? Cadenza.get(predecessorTaskName)
        : undefined;

      if (
        shouldSkipDirectTaskMetadata(task) ||
        shouldSkipDirectTaskMetadata(predecessorTask) ||
        !task?.registered ||
        !predecessorTask?.registered
      ) {
        return false;
      }

      return buildDatabaseTriggerContext({
        ...ctx.data,
        serviceName: Cadenza.serviceRegistry.serviceName,
        predecessorServiceName: Cadenza.serviceRegistry.serviceName,
      });
    })
      .doOn("meta.task.relationship_added")
      .emits("global.meta.graph_metadata.task_relationship_created");

    createLocalGraphMetadataTask("Handle task error", (ctx) => {
      if (isManagedRouteRecoveryTaskError(ctx.data?.errorMessage)) {
        return false;
      }

      Cadenza.log(`Error in task ${ctx.data.taskName}`, ctx.data, "error");
    }).doOn("meta.node.errored");

    createLocalGraphMetadataTask("Handle task signal observation", (ctx) => {
      if (!shouldEmitDirectPrimitiveMetadata()) {
        return false;
      }

      const signalName = String(
        ctx.signalName ?? ctx.data?.signalName ?? "",
      ).split(":")[0];
      const task = resolveTaskFromMetadataContext(ctx as Record<string, any>);

      if (shouldSkipDirectTaskMetadata(task)) {
        return false;
      }

      if (task?.registered && task.registeredSignals.has(signalName)) {
        return false;
      }

      const isGlobal = signalName.startsWith("global.");
      return buildDatabaseTriggerContext({
        ...ctx.data,
        signalName,
        isGlobal,
        serviceName: Cadenza.serviceRegistry.serviceName,
      });
    })
      .doOn("meta.task.observed_signal")
      .emits("global.meta.graph_metadata.task_signal_observed");

    createLocalGraphMetadataTask("Handle task signal attachment", (ctx) => {
      if (!shouldEmitDirectPrimitiveMetadata()) {
        return false;
      }

      const task = resolveTaskFromMetadataContext(ctx as Record<string, any>);

      if (shouldSkipDirectTaskMetadata(task)) {
        return false;
      }

      return buildDatabaseTriggerContext(
        (ctx.data as Record<string, unknown> | undefined) ?? undefined,
        {
          ...ctx.filter,
          serviceName: Cadenza.serviceRegistry.serviceName,
        },
      );
    })
      .doOn("meta.task.attached_signal")
      .emits("global.meta.graph_metadata.task_attached_signal");

    createLocalGraphMetadataTask("Handle task intent association", (ctx) => {
      if (!shouldEmitDirectPrimitiveMetadata()) {
        return false;
      }

      const intentName = String(ctx.data?.intentName ?? "");
      const task = resolveTaskFromMetadataContext(ctx as Record<string, any>);

      if (shouldSkipDirectTaskMetadata(task)) {
        return false;
      }

      if (
        task?.registered &&
        ((task as any).__registeredIntents as Set<string> | undefined)?.has(
          intentName,
        )
      ) {
        return false;
      }

      return buildDatabaseTriggerContext({
        ...(ctx.data as Record<string, unknown> | undefined),
        intentName,
        serviceName: Cadenza.serviceRegistry.serviceName,
      });
    })
      .doOn("meta.task.intent_associated")
      .emits("global.meta.graph_metadata.task_intent_associated");

    const buildHelperMetadataContext = (ctx: Record<string, any>) => {
      if (!shouldEmitDirectPrimitiveMetadata()) {
        return false;
      }

      const helper = resolveHelperFromMetadataContext(ctx);
      if (!helper) {
        return false;
      }

      return buildDatabaseTriggerContext({
        ...(ctx.data as Record<string, unknown> | undefined),
        serviceName: Cadenza.serviceRegistry.serviceName,
      });
    };

    createLocalGraphMetadataTask("Handle helper creation", buildHelperMetadataContext)
      .doOn("meta.helper.created")
      .emits("global.meta.graph_metadata.helper_created");

    createLocalGraphMetadataTask("Handle helper update", buildHelperMetadataContext)
      .doOn("meta.helper.updated")
      .emits("global.meta.graph_metadata.helper_updated");

    const buildGlobalMetadataContext = (ctx: Record<string, any>) => {
      if (!shouldEmitDirectPrimitiveMetadata()) {
        return false;
      }

      const globalDefinition = resolveGlobalFromMetadataContext(ctx);
      if (!globalDefinition) {
        return false;
      }

      return buildDatabaseTriggerContext({
        ...(ctx.data as Record<string, unknown> | undefined),
        serviceName: Cadenza.serviceRegistry.serviceName,
      });
    };

    createLocalGraphMetadataTask("Handle global creation", buildGlobalMetadataContext)
      .doOn("meta.global.created")
      .emits("global.meta.graph_metadata.global_created");

    createLocalGraphMetadataTask("Handle global update", buildGlobalMetadataContext)
      .doOn("meta.global.updated")
      .emits("global.meta.graph_metadata.global_updated");

    createLocalGraphMetadataTask("Handle task helper association", (ctx) => {
      if (!shouldEmitDirectPrimitiveMetadata()) {
        return false;
      }

      const task = resolveTaskFromMetadataContext(ctx as Record<string, any>);
      if (shouldSkipDirectTaskMetadata(task)) {
        return false;
      }

      return buildDatabaseTriggerContext({
        ...(ctx.data as Record<string, unknown> | undefined),
        serviceName: Cadenza.serviceRegistry.serviceName,
      });
    })
      .doOn("meta.task.helper_associated")
      .emits("global.meta.graph_metadata.task_helper_associated");

    createLocalGraphMetadataTask("Handle helper helper association", (ctx) => {
      if (!shouldEmitDirectPrimitiveMetadata()) {
        return false;
      }

      const helper = resolveHelperFromMetadataContext(ctx as Record<string, any>);
      if (!helper) {
        return false;
      }

      return buildDatabaseTriggerContext({
        ...(ctx.data as Record<string, unknown> | undefined),
        serviceName: Cadenza.serviceRegistry.serviceName,
      });
    })
      .doOn("meta.helper.helper_associated")
      .emits("global.meta.graph_metadata.helper_helper_associated");

    createLocalGraphMetadataTask("Handle task global association", (ctx) => {
      if (!shouldEmitDirectPrimitiveMetadata()) {
        return false;
      }

      const task = resolveTaskFromMetadataContext(ctx as Record<string, any>);
      if (shouldSkipDirectTaskMetadata(task)) {
        return false;
      }

      return buildDatabaseTriggerContext({
        ...(ctx.data as Record<string, unknown> | undefined),
        serviceName: Cadenza.serviceRegistry.serviceName,
      });
    })
      .doOn("meta.task.global_associated")
      .emits("global.meta.graph_metadata.task_global_associated");

    createLocalGraphMetadataTask("Handle helper global association", (ctx) => {
      if (!shouldEmitDirectPrimitiveMetadata()) {
        return false;
      }

      const helper = resolveHelperFromMetadataContext(ctx as Record<string, any>);
      if (!helper) {
        return false;
      }

      return buildDatabaseTriggerContext({
        ...(ctx.data as Record<string, unknown> | undefined),
        serviceName: Cadenza.serviceRegistry.serviceName,
      });
    })
      .doOn("meta.helper.global_associated")
      .emits("global.meta.graph_metadata.helper_global_associated");

    createLocalGraphMetadataTask("Handle task unsubscribing signal", (ctx) => {
      if (!shouldEmitDirectPrimitiveMetadata()) {
        return false;
      }

      const task = resolveTaskFromMetadataContext(ctx as Record<string, any>);

      if (shouldSkipDirectTaskMetadata(task)) {
        return false;
      }

      return buildDatabaseTriggerContext(
        {
          deleted: true,
        },
        {
          ...ctx.filter,
          serviceName: Cadenza.serviceRegistry.serviceName,
        },
      );
    })
      .doOn("meta.task.unsubscribed_signal")
      .emits("meta.graph_metadata.task_unsubscribed_signal");

    createLocalGraphMetadataTask("Handle task detaching signal", (ctx) => {
      if (!shouldEmitDirectPrimitiveMetadata()) {
        return false;
      }

      const task = resolveTaskFromMetadataContext(ctx as Record<string, any>);

      if (shouldSkipDirectTaskMetadata(task)) {
        return false;
      }

      return buildDatabaseTriggerContext(
        {
          deleted: true,
        },
        {
          ...ctx.filter,
          serviceName: Cadenza.serviceRegistry.serviceName,
        },
      );
    })
      .doOn("meta.task.detached_signal")
      .emits("global.meta.graph_metadata.task_detached_signal");

    createLocalGraphMetadataTask("Handle routine creation", (ctx) => {
      if (!shouldEmitDirectPrimitiveMetadata()) {
        return false;
      }

      return buildDatabaseTriggerContext({
        ...ctx.data,
        serviceName: Cadenza.serviceRegistry.serviceName,
      });
    })
      .doAfter(Cadenza.registry.registerRoutine)
      .emits("global.meta.graph_metadata.routine_created");

    createLocalGraphMetadataTask("Handle routine update", (ctx) => {
      if (!shouldEmitDirectPrimitiveMetadata()) {
        return false;
      }

      return buildDatabaseTriggerContext(
        (ctx.data as Record<string, unknown> | undefined) ?? undefined,
        {
          ...ctx.filter,
          serviceName: Cadenza.serviceRegistry.serviceName,
        },
      );
    })
      .doOn("meta.routine.destroyed")
      .emits("global.meta.graph_metadata.routine_updated");

    createLocalGraphMetadataTask("Handle adding task to routine", (ctx) => {
      if (!shouldEmitDirectPrimitiveMetadata()) {
        return false;
      }

      return buildDatabaseTriggerContext({
        ...ctx.data,
        serviceName: Cadenza.serviceRegistry.serviceName,
      });
    })
      .doOn("meta.routine.task_added")
      .emits("global.meta.graph_metadata.task_added_to_routine");

    createLocalGraphMetadataTask("Handle new trace", (ctx) => {
      if (!shouldPersistExecutionTrace(ctx as Record<string, any>)) {
        return false;
      }

      return buildDatabaseTriggerContext({
        ...ctx.data,
        service_name: Cadenza.serviceRegistry.serviceName,
        service_instance_id: null,
      });
    })
      .doOn("meta.runner.new_trace")
      .emits("global.meta.graph_metadata.execution_trace_created");

    const prepareRoutineExecutionPersistenceTask = createLocalGraphMetadataTask(
      "Prepare routine execution persistence",
      (ctx) => {
        if (!shouldPersistRoutineExecution(ctx as Record<string, any>)) {
          return false;
        }

        const routineData = (ctx.data as Record<string, any> | undefined) ?? {};
        const sanitizedRoutineContext = sanitizeExecutionPersistenceContext(
          (routineData.context as Record<string, any> | undefined) ?? {},
        );
        const sanitizedRoutineMetaContext = sanitizeExecutionPersistenceContext(
          (routineData.metaContext as Record<string, any> | undefined) ?? {},
        );

        const routineExecutionRow: Record<string, any> = {
          ...routineData,
          context: sanitizedRoutineContext,
          metaContext: sanitizedRoutineMetaContext,
          serviceName: Cadenza.serviceRegistry.serviceName,
          serviceInstanceId: resolveExecutionObservabilityServiceInstanceId(),
        };
        delete routineExecutionRow.routineVersion;
        delete routineExecutionRow.routine_version;

        const traceCreatedByRunner =
          sanitizedRoutineMetaContext.__traceCreatedByRunner === true;
        const executionTraceRow =
          traceCreatedByRunner &&
          typeof routineExecutionRow.executionTraceId === "string" &&
          routineExecutionRow.executionTraceId.length > 0
            ? {
                uuid: routineExecutionRow.executionTraceId,
                issuer_type: "service",
                issuer_id:
                  sanitizedRoutineMetaContext.__issuerId ??
                  sanitizedRoutineMetaContext.__metadata?.__issuerId ??
                  null,
                issued_at:
                  routineExecutionRow.created ?? formatTimestamp(Date.now()),
                intent:
                  sanitizedRoutineMetaContext.__intent ??
                  sanitizedRoutineMetaContext.__metadata?.__intent ??
                  null,
                context: {
                  id: String(routineExecutionRow.uuid ?? ""),
                  context: sanitizedRoutineContext,
                },
                is_meta: routineExecutionRow.isMeta === true,
                service_name: Cadenza.serviceRegistry.serviceName,
                service_instance_id: null,
              }
            : null;

        return {
          ...ctx,
          __traceCreatedByRunner: traceCreatedByRunner,
          __routineExecutionRow: routineExecutionRow,
          ...(executionTraceRow ? { __executionTraceRow: executionTraceRow } : {}),
        };
      },
      "Builds ordered execution_trace and routine_execution inserts for a fresh local routine.",
      { concurrency: 100, isSubMeta: true },
    ).doOn("meta.runner.added_tasks");

    const filterRoutineExecutionTraceCreatedTask = createLocalGraphMetadataTask(
      "Filter routine execution trace created",
      (ctx) => ctx.__traceCreatedByRunner === true,
      "Continues only when the local runner created a new execution trace for this routine.",
      { concurrency: 100, isSubMeta: true },
    );

    const filterRoutineExecutionTraceExistingTask = createLocalGraphMetadataTask(
      "Filter routine execution trace existing",
      (ctx) => ctx.__traceCreatedByRunner !== true,
      "Continues only when the routine already belongs to an existing execution trace.",
      { concurrency: 100, isSubMeta: true },
    );

    const prepareExecutionTraceInsertForRoutineTask = createLocalGraphMetadataTask(
      "Prepare execution trace insert for routine execution",
      (ctx) => {
        if (!ctx.__executionTraceRow) {
          return false;
        }

        return {
          ...ctx,
          data: ctx.__executionTraceRow,
          onConflict: observabilityInsertOnConflict,
          queryData: {
            data: ctx.__executionTraceRow,
            onConflict: observabilityInsertOnConflict,
          },
        };
      },
      "Builds the execution_trace insert payload before routine_execution persistence continues.",
      { concurrency: 100, isSubMeta: true },
    );

    const prepareRoutineExecutionInsertTask = createLocalGraphMetadataTask(
      "Prepare routine execution insert",
      (ctx) => {
        if (!ctx.__routineExecutionRow) {
          return false;
        }

        return {
          ...ctx,
          data: ctx.__routineExecutionRow,
          onConflict: observabilityInsertOnConflict,
          queryData: {
            data: ctx.__routineExecutionRow,
            onConflict: observabilityInsertOnConflict,
          },
        };
      },
      "Builds the routine_execution insert payload for a business routine.",
      { concurrency: 100, isSubMeta: true },
    );

    const emitRoutineExecutionPersistenceBundleTask = createLocalGraphMetadataTask(
      "Emit routine execution persistence bundle",
      (ctx) => {
        const routineExecutionRow =
          ctx.__routineExecutionRow && typeof ctx.__routineExecutionRow === "object"
            ? ({ ...ctx.__routineExecutionRow } as Record<string, any>)
            : null;
        const executionTraceRow =
          ctx.__executionTraceRow && typeof ctx.__executionTraceRow === "object"
            ? ({ ...ctx.__executionTraceRow } as Record<string, any>)
            : null;

        if (!routineExecutionRow) {
          return false;
        }

        return createExecutionPersistenceBundle({
          traceId:
            routineExecutionRow.executionTraceId ??
            routineExecutionRow.execution_trace_id ??
            executionTraceRow?.uuid ??
            null,
          ensures: [
            buildExecutionPersistenceEnsureEvent(
              "execution_trace",
              executionTraceRow,
            ),
            buildExecutionPersistenceEnsureEvent(
              "routine_execution",
              routineExecutionRow,
              [
                buildExecutionPersistenceDependency(
                  "execution_trace",
                  routineExecutionRow.executionTraceId ??
                    routineExecutionRow.execution_trace_id,
                ),
              ],
            ),
          ],
        });
      },
      "Emits one authority-routed execution persistence bundle for a routine execution.",
      { concurrency: 100, isSubMeta: true },
    ).emits(EXECUTION_PERSISTENCE_BUNDLE_SIGNAL);

    prepareRoutineExecutionPersistenceTask.then(
      emitRoutineExecutionPersistenceBundleTask,
    );

    createLocalGraphMetadataTask(
      "Handle routine execution started",
      (ctx) => {
        return buildDatabaseTriggerContext(
          (ctx.data as Record<string, unknown> | undefined) ?? undefined,
          (ctx.filter as Record<string, unknown> | undefined) ?? undefined,
        );
      },
      "Handles routine execution started",
      { concurrency: 100, isSubMeta: true },
    )
      .doOn("meta.node.started_routine_execution")
      .emits("global.meta.graph_metadata.routine_execution_started");

    createLocalGraphMetadataTask(
      "Handle routine execution ended",
      (ctx) => {
        const sanitizedData = sanitizeExecutionPersistenceResultPayload({
          ...ctx.data,
          serviceName: Cadenza.serviceRegistry.serviceName,
          serviceInstanceId: resolveExecutionObservabilityServiceInstanceId(),
        });

        return buildDatabaseTriggerContext(
          sanitizedData,
          (ctx.filter as Record<string, unknown> | undefined) ?? undefined,
        );
      },
      "Handles routine execution ended",
      { concurrency: 100, isSubMeta: true },
    )
      .doOn("meta.node.ended_routine_execution")
      .emits("global.meta.graph_metadata.routine_execution_ended");

    const prepareTaskExecutionPersistenceTask = createLocalGraphMetadataTask(
      "Prepare task execution persistence",
      (ctx) => {
        if (!shouldPersistTaskExecutionMetadata(ctx as Record<string, any>)) {
          return false;
        }

        const taskExecutionData = (ctx.data as Record<string, any> | undefined) ?? {};
        const sanitizedTaskContext = sanitizeExecutionPersistenceContext(
          (taskExecutionData.context as Record<string, any> | undefined) ?? {},
        );
        const sanitizedTaskMetaContext = sanitizeExecutionPersistenceContext(
          (taskExecutionData.metaContext as Record<string, any> | undefined) ?? {},
        );
        const routineMetadata = resolveRoutinePersistenceMetadata(
          sanitizedTaskMetaContext,
        );

        const taskExecutionRow: Record<string, any> = {
          ...taskExecutionData,
          context: sanitizedTaskContext,
          metaContext: sanitizedTaskMetaContext,
          previous_task_execution_ids: Array.isArray(
            taskExecutionData.previousExecutionIds?.ids,
          )
            ? taskExecutionData.previousExecutionIds.ids.filter(
                (value: unknown): value is string =>
                  typeof value === "string" && value.trim().length > 0,
              )
            : [],
          serviceName: Cadenza.serviceRegistry.serviceName,
          serviceInstanceId: resolveExecutionObservabilityServiceInstanceId(),
        };
        delete taskExecutionRow.previousExecutionIds;

        const executionTraceRow =
          sanitizedTaskMetaContext.__traceCreatedByRunner === true &&
          typeof taskExecutionData.executionTraceId === "string" &&
          taskExecutionData.executionTraceId.length > 0
            ? {
                uuid: taskExecutionData.executionTraceId,
                issuer_type: "service",
                issuer_id:
                  sanitizedTaskMetaContext.__issuerId ??
                  sanitizedTaskMetaContext.__metadata?.__issuerId ??
                  null,
                issued_at:
                  routineMetadata.routineCreatedAt ??
                  taskExecutionData.created ??
                  formatTimestamp(Date.now()),
                intent:
                  sanitizedTaskMetaContext.__intent ??
                  sanitizedTaskMetaContext.__metadata?.__intent ??
                  null,
                context: {
                  id: String(taskExecutionData.routineExecutionId ?? taskExecutionData.uuid ?? ""),
                  context: sanitizedTaskContext,
                },
                is_meta:
                  routineMetadata.routineIsMeta ||
                  taskExecutionData.isMeta === true,
                service_name: Cadenza.serviceRegistry.serviceName,
                service_instance_id: null,
              }
            : null;

        const routineExecutionRow =
          typeof taskExecutionData.routineExecutionId === "string" &&
          taskExecutionData.routineExecutionId.length > 0 &&
          routineMetadata.createdByRunner &&
          routineMetadata.routineName
            ? {
                uuid: taskExecutionData.routineExecutionId,
                name: routineMetadata.routineName,
                execution_trace_id: taskExecutionData.executionTraceId ?? null,
                context: sanitizedTaskContext,
                meta_context: sanitizedTaskMetaContext,
                created:
                  routineMetadata.routineCreatedAt ??
                  taskExecutionData.created ??
                  formatTimestamp(Date.now()),
                is_meta:
                  routineMetadata.routineIsMeta ||
                  taskExecutionData.isMeta === true,
                service_name: Cadenza.serviceRegistry.serviceName,
                service_instance_id:
                  resolveExecutionObservabilityServiceInstanceId(),
              }
            : null;

        return {
          ...ctx,
          __traceCreatedByRunner:
            sanitizedTaskMetaContext.__traceCreatedByRunner === true,
          __taskExecutionRow: taskExecutionRow,
          ...(executionTraceRow ? { __executionTraceRow: executionTraceRow } : {}),
          ...(routineExecutionRow
            ? { __routineExecutionRow: routineExecutionRow }
            : {}),
        };
      },
      "Builds ordered execution_trace, routine_execution, and task_execution inserts for a business task execution.",
      { concurrency: 100, isSubMeta: true },
    ).doOn("meta.node.scheduled");

    const filterTaskExecutionTraceCreatedTask = createLocalGraphMetadataTask(
      "Filter task execution trace created",
      (ctx) => ctx.__traceCreatedByRunner === true,
      "Continues only when the local runner created a new execution trace for this task execution.",
      { concurrency: 100, isSubMeta: true },
    );

    const filterTaskExecutionTraceExistingTask = createLocalGraphMetadataTask(
      "Filter task execution trace existing",
      (ctx) => ctx.__traceCreatedByRunner !== true,
      "Continues only when the task execution already belongs to an existing execution trace.",
      { concurrency: 100, isSubMeta: true },
    );

    const filterTaskExecutionRoutineCreatedTask = createLocalGraphMetadataTask(
      "Filter task execution routine created",
      (ctx) => Boolean(ctx.__routineExecutionRow),
      "Continues only when the local runner created a new routine for this task execution.",
      { concurrency: 100, isSubMeta: true },
    );

    const filterTaskExecutionRoutineExistingTask = createLocalGraphMetadataTask(
      "Filter task execution routine existing",
      (ctx) => !ctx.__routineExecutionRow,
      "Continues only when the task_execution row can be inserted without creating a new routine_execution row.",
      { concurrency: 100, isSubMeta: true },
    );

    const prepareExecutionTraceInsertForTaskExecutionTask = createLocalGraphMetadataTask(
      "Prepare execution trace insert for task execution",
      (ctx) => {
        if (!ctx.__executionTraceRow) {
          return false;
        }

        return {
          ...ctx,
          data: ctx.__executionTraceRow,
          onConflict: observabilityInsertOnConflict,
          queryData: {
            data: ctx.__executionTraceRow,
            onConflict: observabilityInsertOnConflict,
          },
        };
      },
      "Builds the execution_trace insert payload before task_execution persistence continues.",
      { concurrency: 100, isSubMeta: true },
    );

    const prepareRoutineExecutionInsertForTaskExecutionTask = createLocalGraphMetadataTask(
      "Prepare routine execution insert for task execution",
      (ctx) => {
        if (!ctx.__routineExecutionRow) {
          return false;
        }

        return {
          ...ctx,
          data: ctx.__routineExecutionRow,
          onConflict: observabilityInsertOnConflict,
          queryData: {
            data: ctx.__routineExecutionRow,
            onConflict: observabilityInsertOnConflict,
          },
        };
      },
      "Builds the routine_execution insert payload before task_execution persistence continues.",
      { concurrency: 100, isSubMeta: true },
    );

    const prepareTaskExecutionInsertTask = createLocalGraphMetadataTask(
      "Prepare task execution insert",
      (ctx) => {
        if (!ctx.__taskExecutionRow) {
          return false;
        }

        return {
          ...ctx,
          data: ctx.__taskExecutionRow,
          onConflict: observabilityInsertOnConflict,
          queryData: {
            data: ctx.__taskExecutionRow,
            onConflict: observabilityInsertOnConflict,
          },
        };
      },
      "Builds the task_execution insert payload for a business task execution.",
      { concurrency: 100, isSubMeta: true },
    );

    const continueTaskExecutionAfterTraceTask = createLocalGraphMetadataTask(
      "Continue task execution persistence after trace",
      (ctx) => ctx,
      "Allows the task-execution persistence graph to fan in after optional execution_trace persistence.",
      { concurrency: 100, isSubMeta: true },
    );

    const continueTaskExecutionAfterRoutineTask = createLocalGraphMetadataTask(
      "Continue task execution persistence after routine",
      (ctx) => ctx,
      "Allows the task-execution persistence graph to fan in after optional routine_execution persistence.",
      { concurrency: 100, isSubMeta: true },
    );

    const emitTaskExecutionPersistenceBundleTask = createLocalGraphMetadataTask(
      "Emit task execution persistence bundle",
      (ctx) => {
        const taskExecutionRow =
          ctx.__taskExecutionRow && typeof ctx.__taskExecutionRow === "object"
            ? ({ ...ctx.__taskExecutionRow } as Record<string, any>)
            : null;
        const executionTraceRow =
          ctx.__executionTraceRow && typeof ctx.__executionTraceRow === "object"
            ? ({ ...ctx.__executionTraceRow } as Record<string, any>)
            : null;
        const routineExecutionRow =
          ctx.__routineExecutionRow && typeof ctx.__routineExecutionRow === "object"
            ? ({ ...ctx.__routineExecutionRow } as Record<string, any>)
            : null;

        if (!taskExecutionRow) {
          return false;
        }

        return createExecutionPersistenceBundle({
          traceId:
            taskExecutionRow.executionTraceId ??
            taskExecutionRow.execution_trace_id ??
            executionTraceRow?.uuid ??
            null,
          ensures: [
            buildExecutionPersistenceEnsureEvent(
              "execution_trace",
              executionTraceRow,
            ),
            buildExecutionPersistenceEnsureEvent(
              "routine_execution",
              routineExecutionRow,
              [
                buildExecutionPersistenceDependency(
                  "execution_trace",
                  routineExecutionRow?.executionTraceId ??
                    routineExecutionRow?.execution_trace_id,
                ),
              ],
            ),
            buildExecutionPersistenceEnsureEvent("task_execution", taskExecutionRow, [
              buildExecutionPersistenceDependency(
                "execution_trace",
                taskExecutionRow.executionTraceId ??
                  taskExecutionRow.execution_trace_id,
              ),
              buildExecutionPersistenceDependency(
                "routine_execution",
                taskExecutionRow.routineExecutionId ??
                  taskExecutionRow.routine_execution_id,
              ),
              buildExecutionPersistenceDependency(
                "signal_emission",
                taskExecutionRow.signalEmissionId ??
                  taskExecutionRow.signal_emission_id,
              ),
              buildExecutionPersistenceDependency(
                "inquiry",
                taskExecutionRow.inquiryId ?? taskExecutionRow.inquiry_id,
              ),
            ]),
          ],
        });
      },
      "Emits one authority-routed execution persistence bundle for a task execution.",
      { concurrency: 100, isSubMeta: true },
    ).emits(EXECUTION_PERSISTENCE_BUNDLE_SIGNAL);

    prepareTaskExecutionPersistenceTask.then(emitTaskExecutionPersistenceBundleTask);

    createLocalGraphMetadataTask(
      "Handle task execution mapped",
      (ctx) => {
        if (!shouldPersistTaskExecutionMap(ctx as Record<string, any>)) {
          return false;
        }

        return buildDatabaseTriggerContext(
          (ctx.data as Record<string, unknown> | undefined) ?? undefined,
          (ctx.filter as Record<string, unknown> | undefined) ?? undefined,
        );
      },
      "Handles task execution mapping",
      { concurrency: 100, isSubMeta: true },
    )
      .doOn("meta.node.mapped", "meta.node.detected_previous_task_execution")
      .emits("global.meta.graph_metadata.task_execution_mapped");

    createLocalGraphMetadataTask(
      "Handle task execution started",
      (ctx) => {
        if (!shouldPersistTaskExecutionMetadata(ctx as Record<string, any>)) {
          return false;
        }

        return buildDatabaseTriggerContext(
          (ctx.data as Record<string, unknown> | undefined) ?? undefined,
          (ctx.filter as Record<string, unknown> | undefined) ?? undefined,
        );
      },
      "Handles task execution started",
      { concurrency: 100, isSubMeta: true },
    )
      .doOn("meta.node.started")
      .emits("global.meta.graph_metadata.task_execution_started");

    createLocalGraphMetadataTask(
      "Handle task execution ended",
      (ctx) => {
        if (!shouldPersistTaskExecutionMetadata(ctx as Record<string, any>)) {
          return false;
        }

        const sanitizedData = sanitizeExecutionPersistenceResultPayload({
          ...ctx.data,
          serviceName: Cadenza.serviceRegistry.serviceName,
          serviceInstanceId: resolveExecutionObservabilityServiceInstanceId(),
        });

        return buildDatabaseTriggerContext(
          sanitizedData,
          (ctx.filter as Record<string, unknown> | undefined) ?? undefined,
        );
      },
      "Handles task execution ended",
      { concurrency: 100, isSubMeta: true },
    )
      .doOn("meta.node.ended")
      .emits("global.meta.graph_metadata.task_execution_ended");

    createLocalGraphMetadataTask(
      "Handle inquiry creation",
      (ctx) => {
        if (!shouldPersistBusinessInquiry(ctx as Record<string, any>)) {
          return false;
        }

        logInquiryMetadataTrace(
          "emit_inquiry_created",
          "global.meta.graph_metadata.inquiry_created",
          ctx as Record<string, any>,
        );

        return buildDatabaseTriggerContext({
          ...ctx.data,
          serviceName: Cadenza.serviceRegistry.serviceName,
          serviceInstanceId: resolveExecutionObservabilityServiceInstanceId(),
          isMeta: false,
        });
      },
      "Handles inquiry creation",
      { concurrency: 100, isSubMeta: true },
    )
      .doOn("meta.inquiry_broker.inquiry_started")
      .emits("global.meta.graph_metadata.inquiry_created");

    createLocalGraphMetadataTask(
      "Emit inquiry creation persistence bundle",
      (ctx) => {
        if (!shouldPersistBusinessInquiry(ctx as Record<string, any>)) {
          return false;
        }

        const inquiryData =
          ctx.data && typeof ctx.data === "object"
            ? ({ ...ctx.data } as Record<string, any>)
            : null;
        if (!inquiryData) {
          return false;
        }

        const executionTraceRow =
          typeof inquiryData.executionTraceId === "string" &&
          inquiryData.executionTraceId.length > 0
            ? {
                uuid: inquiryData.executionTraceId,
                issuer_type: "service",
                issuer_id: inquiryData.serviceInstanceId ?? null,
                issued_at:
                  inquiryData.sentAt ??
                  inquiryData.created ??
                  formatTimestamp(Date.now()),
                intent: inquiryData.name ?? null,
                context: {
                  id: String(inquiryData.uuid ?? ""),
                  context: inquiryData.context ?? {},
                },
                is_meta: false,
                service_name: inquiryData.serviceName ?? Cadenza.serviceRegistry.serviceName,
                service_instance_id: null,
              }
            : null;

        return createExecutionPersistenceBundle({
          traceId: inquiryData.executionTraceId ?? executionTraceRow?.uuid ?? null,
          ensures: [
            buildExecutionPersistenceEnsureEvent(
              "execution_trace",
              executionTraceRow,
            ),
            buildExecutionPersistenceEnsureEvent("inquiry", inquiryData, [
              buildExecutionPersistenceDependency(
                "execution_trace",
                inquiryData.executionTraceId ?? inquiryData.execution_trace_id,
              ),
              buildExecutionPersistenceDependency(
                "task_execution",
                inquiryData.taskExecutionId ?? inquiryData.task_execution_id,
              ),
              buildExecutionPersistenceDependency(
                "routine_execution",
                inquiryData.routineExecutionId ?? inquiryData.routine_execution_id,
              ),
            ]),
          ],
        });
      },
      "Emits one authority-routed execution persistence bundle for inquiry creation.",
      { concurrency: 100, isSubMeta: true },
    )
      .doOn("meta.inquiry_broker.inquiry_started")
      .emits(EXECUTION_PERSISTENCE_BUNDLE_SIGNAL);

    createLocalGraphMetadataTask(
      "Prepare inquiry completion persistence",
      (ctx) => {
        if (!shouldPersistBusinessInquiryCompletion(ctx as Record<string, any>)) {
          return false;
        }

        logInquiryMetadataTrace(
          "emit_inquiry_updated",
          "global.meta.graph_metadata.inquiry_updated",
          ctx as Record<string, any>,
        );

        const insertData =
          ctx.insertData && typeof ctx.insertData === "object"
            ? ({ ...ctx.insertData } as Record<string, unknown>)
            : undefined;
        const updateData =
          ctx.data && typeof ctx.data === "object"
            ? ({ ...ctx.data } as Record<string, unknown>)
            : undefined;
        const updateFilter =
          ctx.filter && typeof ctx.filter === "object"
            ? ({ ...ctx.filter } as Record<string, unknown>)
            : undefined;

        return {
          ...ctx,
          ...(insertData ? { __inquiryInsertData: insertData } : {}),
          ...(updateData ? { __inquiryUpdateData: updateData } : {}),
          ...(updateFilter ? { __inquiryUpdateFilter: updateFilter } : {}),
        };
      },
      "Prepares ordered inquiry completion persistence.",
      { concurrency: 100, isSubMeta: true },
    ).doOn("meta.inquiry_broker.inquiry_completed");

    const filterInquiryCompletionHasInsertData = createLocalGraphMetadataTask(
      "Filter inquiry completion has insert data",
      (ctx) => Boolean(ctx.__inquiryInsertData),
      "Continues only when inquiry completion can ensure the inquiry row before update.",
      { concurrency: 100, isSubMeta: true },
    );

    const filterInquiryCompletionMissingInsertData = createLocalGraphMetadataTask(
      "Filter inquiry completion missing insert data",
      (ctx) => !ctx.__inquiryInsertData,
      "Continues only when inquiry completion must fall back to update-only behavior.",
      { concurrency: 100, isSubMeta: true },
    );

    const prepareInquiryInsertTask = createLocalGraphMetadataTask(
      "Prepare inquiry insert for completion",
      (ctx) => {
        if (!ctx.__inquiryInsertData) {
          return false;
        }

        return buildDatabaseTriggerContext(
          ctx.__inquiryInsertData as Record<string, unknown>,
          undefined,
          {
            ...ctx,
            onConflict: observabilityInsertOnConflict,
          },
          {
            onConflict: observabilityInsertOnConflict,
          },
        );
      },
      "Builds the inquiry insert payload before inquiry completion updates.",
      { concurrency: 100, isSubMeta: true },
    );

    const prepareInquiryUpdateTask = createLocalGraphMetadataTask(
      "Prepare inquiry update",
      (ctx) => {
        if (!ctx.__inquiryUpdateData || !ctx.__inquiryUpdateFilter) {
          return false;
        }

        return buildDatabaseTriggerContext(
          ctx.__inquiryUpdateData as Record<string, unknown>,
          ctx.__inquiryUpdateFilter as Record<string, unknown>,
          ctx,
        );
      },
      "Builds the inquiry update payload after ensuring the inquiry row exists.",
      { concurrency: 100, isSubMeta: true },
    );

    const emitInquiryCompletionPersistenceBundleTask = createLocalGraphMetadataTask(
      "Emit inquiry completion persistence bundle",
      (ctx) => {
        const inquiryInsertData =
          ctx.__inquiryInsertData && typeof ctx.__inquiryInsertData === "object"
            ? ({ ...ctx.__inquiryInsertData } as Record<string, any>)
            : null;
        const inquiryUpdateData =
          ctx.__inquiryUpdateData && typeof ctx.__inquiryUpdateData === "object"
            ? ({ ...ctx.__inquiryUpdateData } as Record<string, any>)
            : null;
        const inquiryUpdateFilter =
          ctx.__inquiryUpdateFilter &&
          typeof ctx.__inquiryUpdateFilter === "object"
            ? ({ ...ctx.__inquiryUpdateFilter } as Record<string, any>)
            : null;

        const executionTraceRow =
          inquiryInsertData &&
          typeof inquiryInsertData.executionTraceId === "string" &&
          inquiryInsertData.executionTraceId.length > 0
            ? {
                uuid: inquiryInsertData.executionTraceId,
                issuer_type: "service",
                issuer_id: inquiryInsertData.serviceInstanceId ?? null,
                issued_at:
                  inquiryInsertData.sentAt ??
                  inquiryInsertData.created ??
                  formatTimestamp(Date.now()),
                intent: inquiryInsertData.name ?? null,
                context: {
                  id: String(inquiryInsertData.uuid ?? ""),
                  context: inquiryInsertData.context ?? {},
                },
                is_meta: false,
                service_name:
                  inquiryInsertData.serviceName ?? Cadenza.serviceRegistry.serviceName,
                service_instance_id: null,
              }
            : null;

        return createExecutionPersistenceBundle({
          traceId:
            inquiryInsertData?.executionTraceId ??
            inquiryUpdateData?.executionTraceId ??
            executionTraceRow?.uuid ??
            null,
          ensures: [
            buildExecutionPersistenceEnsureEvent(
              "execution_trace",
              executionTraceRow,
            ),
            buildExecutionPersistenceEnsureEvent("inquiry", inquiryInsertData, [
              buildExecutionPersistenceDependency(
                "execution_trace",
                inquiryInsertData?.executionTraceId ??
                  inquiryInsertData?.execution_trace_id,
              ),
              buildExecutionPersistenceDependency(
                "task_execution",
                inquiryInsertData?.taskExecutionId ??
                  inquiryInsertData?.task_execution_id,
              ),
            ]),
          ],
          updates: [
            buildExecutionPersistenceUpdateEvent(
              "inquiry",
              inquiryUpdateData,
              inquiryUpdateFilter,
              [
                buildExecutionPersistenceDependency(
                  "inquiry",
                  inquiryUpdateFilter?.uuid ?? inquiryInsertData?.uuid,
                ),
              ],
            ),
          ],
        });
      },
      "Emits one authority-routed execution persistence bundle for inquiry completion.",
      { concurrency: 100, isSubMeta: true },
    ).emits(EXECUTION_PERSISTENCE_BUNDLE_SIGNAL);

    Cadenza.get("Prepare inquiry completion persistence")?.then(
      emitInquiryCompletionPersistenceBundleTask,
    );

    createLocalGraphMetadataTask(
      "Handle task execution relationship creation",
      (ctx) => {
        if (!shouldPersistTaskExecutionMap(ctx as Record<string, any>)) {
          return false;
        }

        return buildDatabaseTriggerContext(
          {
            executionCount: "increment",
            lastExecuted: formatTimestamp(Date.now()),
          },
          {
            ...ctx.filter,
            serviceName: Cadenza.serviceRegistry.serviceName,
          },
        );
      },
      "Handles task execution relationship creation",
      { concurrency: 100, isSubMeta: true },
    )
      .doOn("meta.node.mapped", "meta.node.detected_previous_task_execution")
      .emits("global.meta.graph_metadata.relationship_executed");

    createLocalGraphMetadataTask("Handle actor creation", (ctx) => {
      if (!shouldEmitDirectPrimitiveMetadata()) {
        return false;
      }

      if (shouldSkipDirectActorMetadata(ctx?.data?.name)) {
        return false;
      }

      return buildDatabaseTriggerContext({
        ...ctx.data,
        service_name: Cadenza.serviceRegistry.serviceName,
      });
    })
      .doOn("meta.actor.created")
      .emits("global.meta.graph_metadata.actor_created");

    createLocalGraphMetadataTask("Handle actor task association", (ctx) => {
      if (!shouldEmitDirectPrimitiveMetadata()) {
        return false;
      }

      if (shouldSkipDirectActorMetadata(ctx?.data?.actor_name)) {
        return false;
      }

      return buildDatabaseTriggerContext({
        ...ctx.data,
        service_name: Cadenza.serviceRegistry.serviceName,
      });
    })
      .doOn("meta.actor.task_associated")
      .emits("global.meta.graph_metadata.actor_task_associated");

    registerActorSessionPersistenceTasks();

    createLocalGraphMetadataTask("Handle Intent Creation", (ctx) => {
      if (!shouldEmitDirectPrimitiveMetadata()) {
        return false;
      }

      const intentName =
        typeof ctx.data?.name === "string" ? ctx.data.name.trim() : "";
      if (shouldSkipDirectIntentMetadata(intentName)) {
        return false;
      }

      const intentDefinition = resolveIntentDefinition(intentName);
      if (intentDefinition) {
        intentDefinition.registrationRequested = true;
      }

      return buildDatabaseTriggerContext({
        ...ctx.data,
        isMeta: intentName ? isMetaIntentName(intentName) : false,
      });
    })
      .doOn("meta.inquiry_broker.added")
      .emits("global.meta.graph_metadata.intent_created");
  }
}
