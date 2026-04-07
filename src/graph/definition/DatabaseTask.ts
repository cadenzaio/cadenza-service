import DeputyTask from "./DeputyTask";
import { GraphContext, InquiryOptions } from "@cadenza.io/core";
import type {
  AnyObject,
  Schema,
  TaskResult,
  ThrottleTagGetter,
} from "@cadenza.io/core";
import { DbOperationPayload } from "../../types/queryData";
import Cadenza from "../../Cadenza";
import {
  attachDelegationRequestSnapshot,
  hoistDelegationMetadataFields,
  restoreDelegationRequestSnapshot,
  stripDelegationRequestSnapshot,
} from "../../utils/delegation";

const ACTOR_SESSION_TRACE_ENABLED =
  process.env.CADENZA_ACTOR_SESSION_TRACE === "1" ||
  process.env.CADENZA_ACTOR_SESSION_TRACE === "true";
const INSTANCE_TRACE_ENABLED =
  process.env.CADENZA_INSTANCE_DEBUG === "1" ||
  process.env.CADENZA_INSTANCE_DEBUG === "true";
const EXECUTION_OBSERVABILITY_INSERT_ROUTINES = new Set([
  "Insert execution_trace",
  "Insert signal_emission",
  "Insert routine_execution",
  "Insert task_execution",
  "Insert inquiry",
]);

/**
 * Represents a specialized task for delegating database operations. Extends `DeputyTask`.
 * This class is designed to abstract database operation requests, delegating execution to a meta-layer system.
 */
export default class DatabaseTask extends DeputyTask {
  private readonly queryData: DbOperationPayload;

  /**
   * Constructs an instance of the class with the provided parameters, defining
   * various configuration options and behaviors for the task.
   *
   * @param {string} name - The unique name of the task.
   * @param {string} taskName - The specific name of the task.
   * @param {string | undefined} serviceName - The associated service name. Defaults to undefined.
   * @param {string} description - A brief description of the task. Defaults to an empty string.
   * @param {DbOperationPayload} queryData - The data payload for database operations.
   * @param {number} concurrency - The level of concurrency allowed. Defaults to 0.
   * @param {number} timeout - The timeout duration in milliseconds. Defaults to 0.
   * @param {boolean} register - A flag indicating whether to register the task. Defaults to true.
   * @param {boolean} isUnique - Indicates if the task instance is unique. Defaults to false.
   * @param {boolean} isMeta - Indicates if the task is meta. Defaults to false.
   * @param {boolean} isSubMeta - Indicates if the task is a sub-meta task. Defaults to false.
   * @param {boolean} isHidden - Indicates if the task is hidden. Defaults to false.
   * @param {ThrottleTagGetter | undefined} getTagCallback - A callback used for throttling. Defaults to undefined.
   * @param {SchemaDefinition | undefined} inputSchema - The schema definition for input validation. Defaults to undefined.
   * @param {boolean} validateInputContext - Whether to validate the input context. Defaults to false.
   * @param {SchemaDefinition | undefined} outputSchema - The schema definition for output validation. Defaults to undefined.
   * @param {boolean} validateOutputContext - Whether to validate the output context. Defaults to false.
   * @param {number} retryCount - The maximum number of retry attempts. Defaults to 0.
   * @param {number} retryDelay - The delay between retries in milliseconds. Defaults to 0.
   * @param {number} retryDelayMax - The maximum delay between retries in milliseconds. Defaults to 0.
   * @param {number} retryDelayFactor - The factor for exponential backoff. Defaults to 1.
   * @return {void}
   */
  constructor(
    name: string,
    taskName: string,
    serviceName: string | undefined = undefined,
    description: string = "",
    queryData: DbOperationPayload,
    concurrency: number = 0,
    timeout: number = 0,
    register: boolean = true,
    isUnique: boolean = false,
    isMeta: boolean = false,
    isSubMeta: boolean = false,
    isHidden: boolean = false,
    getTagCallback: ThrottleTagGetter | undefined = undefined,
    inputSchema: Schema | undefined = undefined,
    validateInputContext: boolean = false,
    outputSchema: Schema | undefined = undefined,
    validateOutputContext: boolean = false,
    retryCount: number = 0,
    retryDelay: number = 0,
    retryDelayMax: number = 0,
    retryDelayFactor: number = 1,
  ) {
    super(
      name,
      taskName,
      serviceName,
      description,
      concurrency,
      timeout,
      register,
      isUnique,
      isMeta,
      isSubMeta,
      isHidden,
      getTagCallback,
      inputSchema,
      validateInputContext,
      outputSchema,
      validateOutputContext,
      retryCount,
      retryDelay,
      retryDelayMax,
      retryDelayFactor,
    );
    this.queryData = queryData;
  }

  /**
   * Executes the specified task within the given context.
   *
   * @param {GraphContext} context - The execution context for the current task, which includes data and metadata required for processing.
   * @param {(signal: string, ctx: AnyObject) => void} emit - A function used to send signals or events during task execution.
   * @param inquire
   * @param {(progress: number) => void} progressCallback - A function to report execution progress as a percentage (0-100).
   * @param {{ nodeId: string; routineExecId: string }} nodeData - An object containing identifiers for the current node and routine execution.
   * @return {TaskResult} The result of the task execution.
   */
  execute(
    context: GraphContext,
    emit: (signal: string, ctx: AnyObject) => void,
    inquire: (
      inquiry: string,
      context: AnyObject,
      options: InquiryOptions,
    ) => Promise<AnyObject>,
    progressCallback: (progress: number) => void,
    nodeData: { nodeId: string; routineExecId: string },
  ): TaskResult {
    const initialFullContext = {
      ...context.getFullContext(),
    };
    const hasExplicitOperationPayload =
      initialFullContext.data !== undefined ||
      initialFullContext.queryData !== undefined ||
      initialFullContext.batch !== undefined ||
      initialFullContext.transaction !== undefined ||
      initialFullContext.onConflict !== undefined ||
      initialFullContext.filter !== undefined ||
      initialFullContext.fields !== undefined ||
      initialFullContext.joins !== undefined ||
      initialFullContext.sort !== undefined ||
      initialFullContext.limit !== undefined ||
      initialFullContext.offset !== undefined;
    const hasStaleDelegationIdentity =
      (typeof initialFullContext.__remoteRoutineName === "string" &&
        initialFullContext.__remoteRoutineName !== this.remoteRoutineName) ||
      (typeof initialFullContext.__localTaskName === "string" &&
        initialFullContext.__localTaskName !== this.name);
    const hasResolverOwnedContext =
      (typeof initialFullContext.__resolverRequestId === "string" &&
        initialFullContext.__resolverRequestId.length > 0) ||
      (initialFullContext.__resolverQueryData !== undefined &&
        initialFullContext.__resolverQueryData !== null);
    const shouldPreferCurrentContext =
      hasResolverOwnedContext ||
      (hasExplicitOperationPayload && hasStaleDelegationIdentity);
    const rawContext = shouldPreferCurrentContext
      ? attachDelegationRequestSnapshot(
          stripDelegationRequestSnapshot(initialFullContext),
        )
      : restoreDelegationRequestSnapshot(
          attachDelegationRequestSnapshot(initialFullContext),
        );
    const metadata =
      rawContext.__metadata && typeof rawContext.__metadata === "object"
        ? rawContext.__metadata
        : context.getMetadata();
    const ctx = {
      ...rawContext,
    };

    if (EXECUTION_OBSERVABILITY_INSERT_ROUTINES.has(this.remoteRoutineName)) {
      delete ctx.__signalEmission;
      delete ctx.__routineExecId;
      delete ctx.__localRoutineExecId;
      delete ctx.__previousTaskExecutionId;
      if (ctx.__metadata && typeof ctx.__metadata === "object") {
        delete (ctx.__metadata as AnyObject).__routineExecId;
        delete (ctx.__metadata as AnyObject).__localRoutineExecId;
        delete (ctx.__metadata as AnyObject).__previousTaskExecutionId;
      }
    }

    delete ctx.__metadata;
    const isResolverExecution =
      typeof ctx.__resolverRequestId === "string" &&
      ctx.__resolverRequestId.length > 0;
    const dynamicQueryData =
      isResolverExecution
        ? {}
        : ctx.__resolverQueryData && typeof ctx.__resolverQueryData === "object"
        ? ctx.__resolverQueryData
        : ctx.queryData ?? {};
    delete ctx.queryData;
    const nextQueryData: DbOperationPayload = {
      ...this.queryData,
      data: {
        ...ctx.data,
      },
      ...dynamicQueryData,
    };

    const deputyContext = attachDelegationRequestSnapshot(
      stripDelegationRequestSnapshot(
        hoistDelegationMetadataFields({
          ...ctx,
          __timeout: this.timeout,
          __localTaskName: this.name,
          __localTaskVersion: this.version,
          __localServiceName: Cadenza.serviceRegistry.serviceName,
          __previousTaskExecutionId: nodeData.nodeId,
          __remoteRoutineName: this.remoteRoutineName,
          __serviceName: this.serviceName,
          __executionTraceId: metadata.__executionTraceId ?? null,
          __localRoutineExecId:
            metadata.__routineExecId ?? metadata.__metadata?.__routineExecId,
          __metadata: {
            ...metadata,
            __skipRemoteExecution:
              metadata.__skipRemoteExecution ?? ctx.__skipRemoteExecution ?? false,
            __blockRemoteExecution:
              metadata.__blockRemoteExecution ?? ctx.__blockRemoteExecution ?? false,
            __deputyTaskName: this.name,
          },
          data: nextQueryData.data ?? ctx.data,
          batch: nextQueryData.batch ?? ctx.batch,
          transaction: nextQueryData.transaction ?? ctx.transaction,
          onConflict: Object.prototype.hasOwnProperty.call(nextQueryData, "onConflict")
            ? nextQueryData.onConflict
            : undefined,
          filter: nextQueryData.filter ?? ctx.filter,
          fields: nextQueryData.fields ?? ctx.fields,
          queryData: nextQueryData,
        }),
      ),
    );

    if (
      INSTANCE_TRACE_ENABLED &&
      this.remoteRoutineName === "Insert service_instance"
    ) {
      console.log("[CADENZA_INSTANCE_DEBUG] database_task_execute", {
        localServiceName: Cadenza.serviceRegistry.serviceName,
        localTaskName: this.name,
        remoteRoutineName: this.remoteRoutineName,
        hasDelegationSnapshot:
          (rawContext as AnyObject).__delegationRequestContext !== undefined,
        dataKeys:
          ctx.data && typeof ctx.data === "object" && !Array.isArray(ctx.data)
            ? Object.keys(ctx.data)
            : [],
        rootOnConflictTarget: Array.isArray((ctx as AnyObject).onConflict?.target)
          ? ((ctx as AnyObject).onConflict.target as unknown[])
          : null,
        queryOnConflictTarget: Array.isArray(nextQueryData.onConflict?.target)
          ? (nextQueryData.onConflict.target as unknown[])
          : null,
        queryDataKeys: Object.keys(nextQueryData),
        queryDataDataKeys:
          nextQueryData.data &&
          typeof nextQueryData.data === "object" &&
          !Array.isArray(nextQueryData.data)
            ? Object.keys(nextQueryData.data as AnyObject)
            : [],
        rootKeys: Object.keys(rawContext),
      });
    }

    if (
      ACTOR_SESSION_TRACE_ENABLED &&
      this.remoteRoutineName === "Insert actor_session_state"
    ) {
      console.log("[CADENZA_ACTOR_SESSION_TRACE] database_task_execute", {
        localServiceName: Cadenza.serviceRegistry.serviceName,
        localTaskName: this.name,
        remoteRoutineName: this.remoteRoutineName,
        hadSnapshotBeforeRestore:
          initialFullContext.__delegationRequestContext !== undefined,
        beforeRestoreHasData: initialFullContext.data !== undefined,
        beforeRestoreHasQueryData: initialFullContext.queryData !== undefined,
        beforeRestoreLooksLikeResult:
          initialFullContext.__status !== undefined ||
          initialFullContext.__success !== undefined ||
          initialFullContext.rowCount !== undefined ||
          initialFullContext.__isDeputy === true,
        afterRestoreHasData: rawContext.data !== undefined,
        afterRestoreHasQueryData: rawContext.queryData !== undefined,
        outgoingHasData: deputyContext.data !== undefined,
        outgoingHasQueryData: deputyContext.queryData !== undefined,
        rootKeys: Object.keys(rawContext),
      });
    }

    return this.taskFunction(deputyContext, emit, inquire, progressCallback);
  }
}
