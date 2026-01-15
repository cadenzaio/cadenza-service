import { GraphContext, InquiryOptions, Task } from "@cadenza.io/core";
import type {
  AnyObject,
  SchemaDefinition,
  TaskResult,
  ThrottleTagGetter,
} from "@cadenza.io/core";
import { v4 as uuid } from "uuid";
import Cadenza from "../../Cadenza";

/**
 * Represents a task responsible for transmitting signals to a remote service
 * through the meta-layer for operations delegation.
 *
 * @class
 * @extends Task
 */
export default class SignalTransmissionTask extends Task {
  readonly isDeputy: boolean = true;

  protected readonly signalName: string;
  protected readonly serviceName: string;

  /**
   * Constructs a new instance of the class and initializes it with the provided parameters.
   *
   * @param {string} name - The name of the task being created.
   * @param {string} signalName - The name of the signal associated with this task.
   * @param {string} serviceName - The name of the service associated with this task.
   * @param {string} [description=""] - An optional description of the task.
   * @param {number} [concurrency=0] - The maximum allowed concurrency for this task.
   * @param {number} [timeout=0] - The maximum execution time for this task, in milliseconds.
   * @param {boolean} [register=true] - Whether the task should be registered upon creation.
   * @param {boolean} [isUnique=false] - Indicates if the task should enforce uniqueness.
   * @param {boolean} [isMeta=false] - Specifies if the task is a meta-task.
   * @param {boolean} [isSubMeta=false] - Specifies if the task is a sub-meta-task.
   * @param {boolean} [isHidden=false] - Indicates if the task is hidden from visibility.
   * @param {ThrottleTagGetter|undefined} [getTagCallback=undefined] - A callback for tag throttling logic.
   * @param {SchemaDefinition|undefined} [inputSchema=undefined] - An optional schema for validating input data.
   * @param {boolean} [validateInputContext=false] - Whether to validate the input context against the input schema.
   * @param {SchemaDefinition|undefined} [outputSchema=undefined] - An optional schema for validating output data.
   * @param {boolean} [validateOutputContext=false] - Whether to validate the output context against the output schema.
   * @param {number} [retryCount=0] - The number of retry attempts allowed in case of task failure.
   * @param {number} [retryDelay=0] - The initial delay before retrying a failed task, in milliseconds.
   * @param {number} [retryDelayMax=0] - The maximum delay between retry attempts, in milliseconds.
   * @param {number} [retryDelayFactor=1] - A multiplier applied to retry delay for exponential backoff.
   * @return {void} Does not return a value.
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
    isMeta: boolean = true,
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
    const taskFunction = (context: AnyObject): TaskResult => {
      context.__routineExecId = uuid();
      return context;
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

    this.doOn(signalName);
    this.then(Cadenza.serviceRegistry.getBalancedInstance);
  }

  /**
   * Executes the given task function within the provided execution context.
   *
   * @param {GraphContext} context - The context object providing the graph execution environment and metadata.
   * @param {Function} emit - A function to emit signals with the provided name and context.
   * @param inquire
   * @param {Function} progressCallback - A callback function to report the progress of the task execution as a number between 0 and 1.
   * @return {TaskResult} The result of the executed task function.
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
  ): TaskResult {
    const ctx = context.getContext();
    const metadata = context.getMetadata();

    const deputyContext = {
      __localTaskName: this.name,
      __localServiceName: Cadenza.serviceRegistry.serviceName,
      __serviceName: this.serviceName,
      __executionTraceId: metadata.__executionTraceId ?? null,
      __localRoutineExecId:
        metadata.__routineExecId ?? metadata.__metadata?.__routineExecId,
      __metadata: {
        ...metadata,
        __deputyTaskName: this.name,
      },
      __signalName: this.signalName,
      __signalEmissionId: metadata.__signalEmission?.uuid,
      ...ctx,
    };

    return this.taskFunction(deputyContext, emit, inquire, progressCallback);
  }
}
