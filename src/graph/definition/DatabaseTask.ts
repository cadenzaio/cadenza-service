import DeputyTask from "./DeputyTask";
import { GraphContext } from "@cadenza.io/core";
import type {
  AnyObject,
  SchemaDefinition,
  TaskResult,
  ThrottleTagGetter,
} from "@cadenza.io/core";
import { DbOperationPayload } from "../../types/queryData";
import Cadenza from "../../Cadenza";

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
    inputSchema: SchemaDefinition | undefined = undefined,
    validateInputContext: boolean = false,
    outputSchema: SchemaDefinition | undefined = undefined,
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
   * @param {(progress: number) => void} progressCallback - A function to report execution progress as a percentage (0-100).
   * @param {{ nodeId: string; routineExecId: string }} nodeData - An object containing identifiers for the current node and routine execution.
   * @return {TaskResult} The result of the task execution.
   */
  execute(
    context: GraphContext,
    emit: (signal: string, ctx: AnyObject) => void,
    progressCallback: (progress: number) => void,
    nodeData: { nodeId: string; routineExecId: string },
  ): TaskResult {
    const ctx = context.getContext();
    const metadata = context.getMetadata();
    const dynamicQueryData = ctx.queryData ?? {};
    delete ctx.queryData;

    const deputyContext = {
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
        __deputyTaskName: this.name,
      },
      queryData: {
        ...this.queryData,
        data: {
          ...ctx.data,
        },
        ...dynamicQueryData,
      },
    };

    return this.taskFunction(deputyContext, emit, progressCallback);
  }
}
