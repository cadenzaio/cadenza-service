import { GraphContext, Task } from "@cadenza.io/core";
import type {
  AnyObject,
  SchemaDefinition,
  TaskResult,
  ThrottleTagGetter,
} from "@cadenza.io/core";
import { v4 as uuid } from "uuid";
import Cadenza from "../../Cadenza";

export default class SignalTransmissionTask extends Task {
  readonly isDeputy: boolean = true;

  protected readonly signalName: string;
  protected readonly serviceName: string;

  /**
   * Constructs a DatabaseTask to execute a database operation on a remote service.
   * @param name - The local name of the Task.
   * @param signalName - The name of the signal to transmit to the service.
   * @param serviceName - The target database service name (optional, defaults to 'DatabaseService').
   * @param description - A description of the task's purpose (default: '').
   * @param concurrency - The maximum number of concurrent executions (default: 0, unlimited).
   * @param timeout - Timeout in milliseconds (default: 0, handled by engine).
   * @param register - Whether to register the task in the registry (default: true).
   * @param isUnique
   * @param isMeta
   * @param isSubMeta
   * @param isHidden
   * @param getTagCallback - Callback for dynamic tagging, e.g., 'return "default"'.
   * @param inputSchema - Input schema definition.
   * @param validateInputContext - Whether to validate the input context (default: false).
   * @param outputSchema - Output schema definition.
   * @param validateOutputContext - Whether to validate the output context (default: false).
   * @param retryCount
   * @param retryDelay
   * @param retryDelayMax
   * @param retryDelayFactor
   * @emits {meta.deputy.created} - Emitted on construction with task and service details.
   * @note Fallbacks via `.doOnFail` externally; timeouts managed by the engine.
   */
  constructor(
    name: string,
    signalName: string,
    serviceName: string,
    description: string = "",
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
    const taskFunction = (
      context: AnyObject,
      emit: (signal: string, ctx: AnyObject) => void,
    ): Promise<TaskResult> => {
      return new Promise((resolve, reject) => {
        const processId = uuid();

        context.__routineExecId = processId;
        emit("meta.signal_transmission.requested", context);

        // Ephemeral meta-task for resolution
        Cadenza.createEphemeralMetaTask(
          `Resolve signal transmission for ${this.signalName}`,
          (responseCtx) => {
            if (responseCtx?.errored) {
              reject(new Error(responseCtx.__error));
            } else {
              resolve(responseCtx);
            }
          },
          `Ephemeral resolver for signal transmission ${processId}`,
          {
            isSubMeta: true,
            register: false,
          },
        ).doOn(
          `meta.socket_client.transmitted:${processId}`,
          `meta.fetch.transmitted:${processId}`,
        );
      });
    };

    super(
      name,
      taskFunction,
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

    this.serviceName = serviceName;
    this.signalName = signalName;

    this.emit("meta.deputy.created", {
      localTaskName: this.name,
      localTaskVersion: this.version,
      serviceName: this.serviceName,
      communicationType: "signal",
      signalName: this.signalName,
    });
  }

  /**
   * Triggers the database operation delegation flow via a signal to the meta-layer.
   * @param context - The GraphContext containing execution data.
   * @param emit
   * @param progressCallback - Callback to update progress (invoked by meta-layer).
   * @returns A Promise resolving with the task result or rejecting on error.
   * @emits {meta.deputy.executed} - Emitted with context including queryData to initiate delegation.
   * @edge Engine handles timeout and error, triggering `.doOnFail` if chained.
   * @note The resolution and progress are managed by ephemeral meta-tasks.
   */
  execute(
    context: GraphContext,
    emit: (signal: string, ctx: AnyObject) => void,
    progressCallback: (progress: number) => void,
  ): TaskResult {
    const ctx = context.getContext();
    const metadata = context.getMetadata();

    const deputyContext = {
      __localTaskName: this.name,
      __serviceName: this.serviceName,
      __executionTraceId: metadata.__executionTraceId ?? null,
      __localRoutineExecId:
        metadata.__routineExecId ?? metadata.__metadata?.__routineExecId,
      __metadata: {
        ...metadata,
        __deputyTaskName: this.name,
      },
      __signalName: this.signalName,
      ...ctx,
    };

    return this.taskFunction(deputyContext, emit, progressCallback);
  }
}
