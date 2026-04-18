import Cadenza from "../Cadenza";
import {
  buildExecutionPersistenceDependency,
  buildExecutionPersistenceEnsureEvent,
  createExecutionPersistenceBundle,
  EXECUTION_PERSISTENCE_BUNDLE_SIGNAL,
} from "../execution/ExecutionPersistenceCoordinator";
import { decomposeSignalName } from "../utils/tools";
import { v4 as uuid } from "uuid";
import {
  resolveRoutinePersistenceMetadata,
  sanitizeExecutionPersistenceContext,
  splitRoutinePersistenceContext,
} from "../utils/routinePersistence";

function resolveExecutionObservabilityServiceInstanceId(): string | null {
  return Cadenza.hasCompletedBootstrapSync()
    ? (Cadenza.serviceRegistry.serviceInstanceId ?? null)
    : null;
}

function buildSignalDatabaseTriggerContext(
  data: Record<string, unknown>,
): Record<string, unknown> {
  const onConflict = {
    target: ["name"],
    action: {
      do: "nothing" as const,
    },
  };

  return {
    data: { ...data },
    onConflict,
    queryData: {
      data: { ...data },
      onConflict,
    },
  };
}

function isForeignSignalEmissionOrigin(
  signalEmission: Record<string, any>,
): boolean {
  const localServiceName = Cadenza.serviceRegistry.serviceName ?? null;
  const localServiceInstanceId = Cadenza.serviceRegistry.serviceInstanceId ?? null;
  const originServiceName =
    typeof signalEmission.serviceName === "string"
      ? signalEmission.serviceName
      : typeof signalEmission.service_name === "string"
        ? signalEmission.service_name
        : null;
  const originServiceInstanceId =
    typeof signalEmission.serviceInstanceId === "string"
      ? signalEmission.serviceInstanceId
      : typeof signalEmission.service_instance_id === "string"
        ? signalEmission.service_instance_id
        : null;

  if (originServiceInstanceId && localServiceInstanceId) {
    return originServiceInstanceId !== localServiceInstanceId;
  }

  if (originServiceName && localServiceName) {
    return originServiceName !== localServiceName;
  }

  return false;
}

/**
 * SignalController is a singleton class that manages signal registration, transmission,
 * and metadata handling within a service instance.
 *
 * This class utilizes the Cadenza framework to handle various tasks related to signals,
 * such as registering signals, transmitting signals to remote services, adding metadata,
 * and maintaining clean-forwarding and processing of signals.
 *
 * Features:
 * - Ensures signals are properly registered and metadata is propagated.
 * - Handles foreign signal registration for cross-service communication.
 * - Forwards signal observations and manages their metadata.
 * - Adds metadata during signal emission and consumption.
 * - Implements a meta-task-based system for handling complex workflows.
 *
 * Constructor initializes the necessary meta-tasks and tasks required for signal management.
 */
export default class SignalController {
  private static _instance: SignalController;
  public static get instance(): SignalController {
    if (!this._instance) this._instance = new SignalController();
    return this._instance;
  }

  /**
   * Constructor method for initializing the signal registration and data transmission process.
   * This method defines multiple meta tasks to manage signals, forwarding, and adding metadata
   * for service instances in a distributed system.
   *
   * Some key functionalities include:
   * - Registering signals from a service instance.
   * - Handling foreign signal registration from remote services.
   * - Forwarding signal observations between services.
   * - Adding metadata to signal emissions for better traceability.
   * - Adding metadata to signal consumption events.
   *
   * It serves as an initializer for signals and tasks.
  */
  constructor() {
    Cadenza.createMetaTask(
      "Handle Signal Registration",
      (ctx, emit) => {
        if (!Cadenza.hasCompletedBootstrapSync()) {
          return false;
        }

        const { signalName } = ctx;
        const signalObserver = (Cadenza.signalBroker as any).signalObservers?.get(
          signalName,
        );

        if (signalObserver?.registered || signalObserver?.registrationRequested) {
          return false;
        }

        if (signalObserver) {
          signalObserver.registrationRequested = true;
        }

        const { isMeta, isGlobal, domain, action } =
          decomposeSignalName(signalName);

        emit(
          "global.meta.signal_controller.signal_added",
          buildSignalDatabaseTriggerContext({
            name: signalName,
            is_global: isGlobal,
            domain,
            action,
            is_meta: isMeta,
          }),
        );

        return ctx;
      },
      "Handles signal registration from a service instance",
    )
      .doOn("meta.signal_broker.added")
      .attachSignal("global.meta.signal_controller.signal_added");

    Cadenza.createMetaTask(
      "Prepare signal emission persistence",
      (ctx, emit) => {
        const signalEmission = ctx.__signalEmission;
        delete ctx.__signalEmission;

        if (!signalEmission) {
          return false;
        }

        if (
          typeof signalEmission.signalName === "string" &&
          signalEmission.signalName.trim().length > 0 &&
          !(Cadenza.signalBroker as any).signalObservers?.has(signalEmission.signalName)
        ) {
          Cadenza.signalBroker.addSignal(signalEmission.signalName);
        } else {
          const signalObserver = (Cadenza.signalBroker as any).signalObservers?.get(
            signalEmission.signalName,
          );

          if (
            signalObserver &&
            signalObserver.registered !== true &&
            signalObserver.registrationRequested !== true &&
            Cadenza.hasCompletedBootstrapSync()
          ) {
            signalObserver.registrationRequested = true;
            const { isMeta, isGlobal, domain, action } = decomposeSignalName(
              signalEmission.signalName,
            );
            emit(
              "global.meta.signal_controller.signal_added",
              buildSignalDatabaseTriggerContext({
                name: signalEmission.signalName,
                is_global: isGlobal,
                domain,
                action,
                is_meta: isMeta,
              }),
            );
          }
        }

        const traceContext = { ...ctx };
        delete traceContext.__traceCreatedBySignalBroker;
        const sanitizedTraceContext =
          sanitizeExecutionPersistenceContext(traceContext);
        const routineMetadata = resolveRoutinePersistenceMetadata(traceContext);
        const { context: routineContext, metaContext: routineMetaContext } =
          splitRoutinePersistenceContext(traceContext);
        const traceCreatedBySignalBroker =
          ctx.__traceCreatedBySignalBroker === true ||
          signalEmission.__traceCreatedBySignalBroker === true;

        const signalEmissionRow = {
          uuid: signalEmission.uuid,
          signal_name: signalEmission.signalName,
          signal_tag: signalEmission.signalTag,
          task_name: signalEmission.taskName,
          task_version: signalEmission.taskVersion,
          task_execution_id: signalEmission.taskExecutionId,
          is_meta: signalEmission.isMeta,
          is_metric: signalEmission.isMetric ?? false,
          routine_execution_id: signalEmission.routineExecutionId,
          execution_trace_id: signalEmission.executionTraceId,
          context: sanitizedTraceContext,
          metadata: signalEmission,
          service_name: Cadenza.serviceRegistry.serviceName,
          service_instance_id: resolveExecutionObservabilityServiceInstanceId(),
        };

        if (signalEmission.isMeta === true) {
          return false;
        }

        if (isForeignSignalEmissionOrigin(signalEmission as Record<string, any>)) {
          return false;
        }

        const executionTraceRow =
          traceCreatedBySignalBroker
            ? {
                uuid: signalEmission.executionTraceId,
                issuer_type: "service",
                issuer_id:
                  traceContext.__metadata?.__issuerId ??
                  traceContext.__issuerId ??
                  null,
                issued_at: signalEmission.emittedAt,
                intent:
                  traceContext.__metadata?.__intent ??
                  traceContext.__intent ??
                  null,
                context: {
                  id: uuid(),
                  context: sanitizedTraceContext,
                },
                is_meta: signalEmission.isMeta,
                service_name: Cadenza.serviceRegistry.serviceName,
                service_instance_id: null,
              }
            : null;

        const routineExecutionRow =
          signalEmission.routineExecutionId &&
          routineMetadata.createdByRunner &&
          routineMetadata.routineName &&
          traceContext.__isSubMeta !== true
            ? {
                uuid: signalEmission.routineExecutionId,
                name: routineMetadata.routineName,
                execution_trace_id: signalEmission.executionTraceId,
                context: routineContext,
                meta_context: routineMetaContext,
                created:
                  routineMetadata.routineCreatedAt ?? signalEmission.emittedAt,
                is_meta: routineMetadata.routineIsMeta,
                service_name: Cadenza.serviceRegistry.serviceName,
                service_instance_id:
                  resolveExecutionObservabilityServiceInstanceId(),
              }
            : null;

        return {
          ...traceContext,
          __traceCreatedBySignalBroker: traceCreatedBySignalBroker,
          __signalEmissionRow: signalEmissionRow,
          ...(routineExecutionRow
            ? { __routineExecutionRow: routineExecutionRow }
            : {}),
          ...(executionTraceRow
            ? { __executionTraceRow: executionTraceRow }
            : {}),
        };
      },
      "",
      { isSubMeta: true, concurrency: 100 },
    ).doOn("sub_meta.signal_broker.emitting_signal");

    const emitSignalEmissionPersistenceBundleTask = Cadenza.createMetaTask(
      "Emit signal emission persistence bundle",
      (ctx) => {
        const signalEmissionRow =
          ctx.__signalEmissionRow && typeof ctx.__signalEmissionRow === "object"
            ? ({ ...ctx.__signalEmissionRow } as Record<string, any>)
            : null;
        const executionTraceRow =
          ctx.__executionTraceRow && typeof ctx.__executionTraceRow === "object"
            ? ({ ...ctx.__executionTraceRow } as Record<string, any>)
            : null;
        const routineExecutionRow =
          ctx.__routineExecutionRow && typeof ctx.__routineExecutionRow === "object"
            ? ({ ...ctx.__routineExecutionRow } as Record<string, any>)
            : null;

        if (!signalEmissionRow) {
          return false;
        }

        return (
          createExecutionPersistenceBundle({
          traceId:
            signalEmissionRow.executionTraceId ??
            signalEmissionRow.execution_trace_id ??
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
            buildExecutionPersistenceEnsureEvent("signal_emission", signalEmissionRow, [
              buildExecutionPersistenceDependency(
                "execution_trace",
                signalEmissionRow.executionTraceId ??
                  signalEmissionRow.execution_trace_id,
              ),
              buildExecutionPersistenceDependency(
                "routine_execution",
                signalEmissionRow.routineExecutionId ??
                  signalEmissionRow.routine_execution_id,
              ),
              buildExecutionPersistenceDependency(
                "task_execution",
                signalEmissionRow.taskExecutionId ??
                  signalEmissionRow.task_execution_id,
              ),
            ]),
          ],
          }) ?? false
        );
      },
      "Emits one authority-routed execution persistence bundle for a signal emission.",
      { isSubMeta: true, concurrency: 100 },
    ).emits(EXECUTION_PERSISTENCE_BUNDLE_SIGNAL);

    Cadenza.get("Prepare signal emission persistence")?.then(
      emitSignalEmissionPersistenceBundleTask,
    );
  }
}
