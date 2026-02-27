import { GraphRoutine, Task } from "@cadenza.io/core";
import type { AnyObject } from "@cadenza.io/core";
import Cadenza from "../Cadenza";
import { isBrowser } from "../utils/environment";
import { InquiryResponderDescriptor } from "../types/inquiry";
import {
  isMetaIntentName,
  META_RUNTIME_TRANSPORT_DIAGNOSTICS_INTENT,
} from "../utils/inquiry";

export interface ServiceInstanceDescriptor {
  uuid: string;
  address: string;
  port: number;
  serviceName: string;
  numberOfRunningGraphs?: number;
  isPrimary: boolean;
  isActive: boolean;
  isNonResponsive: boolean;
  isBlocked: boolean;
  health: AnyObject;
  exposed: boolean;
  clientCreated?: boolean;
  isFrontend: boolean;
}

export interface DeputyDescriptor {
  serviceName: string;
  remoteRoutineName?: string;
  signalName?: string;
  localTaskName: string;
  communicationType: string;
}

interface RemoteIntentDeputyDescriptor {
  key: string;
  intentName: string;
  serviceName: string;
  remoteTaskName: string;
  remoteTaskVersion: number;
  localTaskName: string;
  localTask: Task;
}

/**
 * The ServiceRegistry class is a singleton that manages the registration and lifecycle of
 * service instances, deputies, and remote signals in a distributed service architecture.
 * It handles various tasks such as instance updates, remote signal registration,
 * service status synchronization, and error/event broadcasting.
 */
export default class ServiceRegistry {
  private static _instance: ServiceRegistry;
  public static get instance(): ServiceRegistry {
    if (!this._instance) this._instance = new ServiceRegistry();
    return this._instance;
  }

  private instances: Map<string, ServiceInstanceDescriptor[]> = new Map();
  private deputies: Map<string, DeputyDescriptor[]> = new Map();
  private remoteSignals: Map<string, Set<string>> = new Map();
  private remoteIntents: Map<string, Set<string>> = new Map();
  private remoteIntentDeputiesByKey: Map<string, RemoteIntentDeputyDescriptor> =
    new Map();
  private remoteIntentDeputiesByTask: Map<Task, RemoteIntentDeputyDescriptor> =
    new Map();
  serviceName: string | null = null;
  serviceInstanceId: string | null = null;
  numberOfRunningGraphs: number = 0;
  useSocket: boolean = false;
  retryCount: number = 3;

  handleInstanceUpdateTask: Task;
  handleGlobalSignalRegistrationTask: Task;
  handleGlobalIntentRegistrationTask: Task;
  handleSocketStatusUpdateTask: Task;
  fullSyncTask: GraphRoutine;
  getAllInstances: Task;
  doForEachInstance: Task;
  deleteInstance: Task;
  getBalancedInstance: Task;
  getInstanceById: Task;
  getInstancesByServiceName: Task;
  handleDeputyRegistrationTask: Task;
  getStatusTask: Task;
  insertServiceTask: Task;
  insertServiceInstanceTask: Task;
  handleServiceNotRespondingTask: Task;
  handleServiceHandshakeTask: Task;
  collectTransportDiagnosticsTask: Task;

  private buildRemoteIntentDeputyKey(map: {
    intentName: string;
    serviceName: string;
    taskName: string;
    taskVersion?: number;
  }): string {
    return `${map.intentName}|${map.serviceName}|${map.taskName}|${map.taskVersion ?? 1}`;
  }

  private normalizeIntentMaps(ctx: AnyObject): Array<{
    intentName: string;
    serviceName: string;
    taskName: string;
    taskVersion: number;
    deleted?: boolean;
  }> {
    if (Array.isArray((ctx as any).intentToTaskMaps)) {
      return (ctx as any).intentToTaskMaps
        .map((m: any) => ({
          intentName: m.intentName ?? m.intent_name,
          serviceName: m.serviceName ?? m.service_name,
          taskName: m.taskName ?? m.task_name,
          taskVersion: m.taskVersion ?? m.task_version ?? 1,
          deleted: !!m.deleted,
        }))
        .filter((m: any) => m.intentName && m.serviceName && m.taskName);
    }

    const single =
      (ctx as any).intentToTaskMap ??
      (ctx as any).data ??
      ((ctx as any).intentName ? ctx : undefined);

    if (!single) return [];

    const normalized = {
      intentName: single.intentName ?? single.intent_name,
      serviceName: single.serviceName ?? single.service_name,
      taskName: single.taskName ?? single.task_name,
      taskVersion: single.taskVersion ?? single.task_version ?? 1,
      deleted: !!single.deleted,
    };

    if (!normalized.intentName || !normalized.serviceName || !normalized.taskName)
      return [];

    return [normalized];
  }

  private registerRemoteIntentDeputy(map: {
    intentName: string;
    serviceName: string;
    taskName: string;
    taskVersion: number;
  }) {
    if (!this.serviceName || map.serviceName === this.serviceName) {
      return;
    }

    const key = this.buildRemoteIntentDeputyKey(map);
    if (this.remoteIntentDeputiesByKey.has(key)) {
      return;
    }

    const deputyTaskName = `Inquire ${map.intentName} via ${map.serviceName} (${map.taskName} v${map.taskVersion})`;

    const deputyTask = isMetaIntentName(map.intentName)
      ? Cadenza.createMetaDeputyTask(map.taskName, map.serviceName, {
          register: false,
          isHidden: true,
          retryCount: 1,
          retryDelay: 50,
          retryDelayFactor: 1.2,
        })
      : Cadenza.createDeputyTask(map.taskName, map.serviceName, {
          register: false,
          isHidden: true,
          retryCount: 1,
          retryDelay: 50,
          retryDelayFactor: 1.2,
        });

    deputyTask.respondsTo(map.intentName);

    if (!this.remoteIntents.has(map.serviceName)) {
      this.remoteIntents.set(map.serviceName, new Set());
    }
    this.remoteIntents.get(map.serviceName)!.add(map.intentName);

    const descriptor: RemoteIntentDeputyDescriptor = {
      key,
      intentName: map.intentName,
      serviceName: map.serviceName,
      remoteTaskName: map.taskName,
      remoteTaskVersion: map.taskVersion,
      localTaskName: deputyTask.name || deputyTaskName,
      localTask: deputyTask,
    };

    this.remoteIntentDeputiesByKey.set(key, descriptor);
    this.remoteIntentDeputiesByTask.set(deputyTask, descriptor);
  }

  private unregisterRemoteIntentDeputy(map: {
    intentName: string;
    serviceName: string;
    taskName: string;
    taskVersion?: number;
  }) {
    const key = this.buildRemoteIntentDeputyKey(map);
    const descriptor = this.remoteIntentDeputiesByKey.get(key);
    if (!descriptor) {
      return;
    }

    const task = descriptor.localTask;
    if (task) {
      Cadenza.inquiryBroker.unsubscribe(descriptor.intentName, task);
      task.destroy();
    }

    this.remoteIntentDeputiesByTask.delete(descriptor.localTask);
    this.remoteIntentDeputiesByKey.delete(key);

    this.remoteIntents.get(descriptor.serviceName)?.delete(descriptor.intentName);
    if (!this.remoteIntents.get(descriptor.serviceName)?.size) {
      this.remoteIntents.delete(descriptor.serviceName);
    }

    const deputies = this.deputies.get(descriptor.serviceName);
    if (deputies) {
      this.deputies.set(
        descriptor.serviceName,
        deputies.filter((d) => d.localTaskName !== descriptor.localTaskName),
      );

      if (this.deputies.get(descriptor.serviceName)?.length === 0) {
        this.deputies.delete(descriptor.serviceName);
      }
    }
  }

  public getInquiryResponderDescriptor(task: Task): InquiryResponderDescriptor {
    const remote = this.remoteIntentDeputiesByTask.get(task);

    if (remote) {
      return {
        isRemote: true,
        serviceName: remote.serviceName,
        taskName: remote.remoteTaskName,
        taskVersion: remote.remoteTaskVersion,
        localTaskName: remote.localTaskName,
      };
    }

    return {
      isRemote: false,
      serviceName: this.serviceName ?? "UnknownService",
      taskName: task.name,
      taskVersion: task.version,
      localTaskName: task.name,
    };
  }

  /**
   * Initializes a private constructor for managing service instances, remote signals,
   * service health, and handling updates or synchronization tasks. The constructor
   * creates a variety of meta tasks that process different lifecycle events and
   * service-related updates in a distributed service registry model.
   *
   * @return {Object} An instance of the constructed class with initialized tasks
   *                  and state management necessary to process service-related events.
   */
  private constructor() {
    Cadenza.defineIntent({
      name: META_RUNTIME_TRANSPORT_DIAGNOSTICS_INTENT,
      description:
        "Gather transport diagnostics across all services and communication clients.",
      input: {
        type: "object",
        properties: {
          detailLevel: {
            type: "string",
            constraints: {
              oneOf: ["summary", "full"],
            },
          },
          includeErrorHistory: {
            type: "boolean",
          },
          errorHistoryLimit: {
            type: "number",
            constraints: {
              min: 1,
              max: 200,
            },
          },
        },
      },
      output: {
        type: "object",
        properties: {
          transportDiagnostics: {
            type: "object",
          },
        },
      },
    });

    this.handleInstanceUpdateTask = Cadenza.createMetaTask(
      "Handle Instance Update",
      (ctx, emit) => {
        const { serviceInstance } = ctx;
        const {
          uuid,
          serviceName,
          address,
          port,
          exposed,
          isFrontend,
          deleted,
        } = serviceInstance;
        if (uuid === this.serviceInstanceId) return;

        if (deleted) {
          const indexToDelete =
            this.instances.get(serviceName)?.findIndex((i) => i.uuid === uuid) ??
            -1;
          if (indexToDelete >= 0) {
            this.instances.get(serviceName)?.splice(indexToDelete, 1);
          }

          if (this.instances.get(serviceName)?.length === 0) {
            this.instances.delete(serviceName);
          } else if (
            this.instances
              .get(serviceName)
              ?.filter((i) => i.address === address && i.port === port)
              .length === 0
          ) {
            emit(`meta.socket_shutdown_requested:${address}_${port}`, {});
            emit(`meta.fetch.destroy_requested:${address}_${port}`, {});
          }

          return;
        }

        if (!this.instances.has(serviceName))
          this.instances.set(serviceName, []);
        const instances = this.instances.get(serviceName)!;
        const existing = instances.find((i) => i.uuid === uuid);

        if (existing) {
          Object.assign(existing, serviceInstance); // Update
        } else {
          instances.push(serviceInstance);
        }

        if (this.serviceName === serviceName) {
          return false;
        }

        if (
          (!isFrontend &&
            (this.deputies.has(serviceName) ||
              this.remoteIntents.has(serviceName))) ||
          this.remoteSignals.has(serviceName)
        ) {
          const clientCreated = instances?.some(
            (i) =>
              i.address === address &&
              i.port === port &&
              i.clientCreated &&
              i.isActive,
          );

          if (!clientCreated) {
            const communicationTypes = Array.from(
              new Set(
                this.deputies
                  .get(serviceName)
                  ?.map((d) => d.communicationType) ?? [],
              ),
            );

            if (
              !communicationTypes.includes("signal") &&
              this.remoteSignals.has(serviceName)
            ) {
              communicationTypes.push("signal");
            }

            emit("meta.service_registry.dependee_registered", {
              serviceName: serviceName,
              serviceInstanceId: uuid,
              serviceAddress: address,
              servicePort: port,
              protocol: exposed ? "https" : "http",
              communicationTypes,
            });

            instances
              ?.filter(
                (i: any) =>
                  i.address === address &&
                  i.port === port &&
                  i.isActive,
              )
              .forEach((i: any) => {
                i.clientCreated = true;
              });
          }
        }

        return true;
      },
      "Handles instance updates to service instances",
    )
      .emits("meta.service_registry.service_discovered")
      .doOn(
        "meta.initializing_service",
        "global.meta.service_instance.inserted",
        "global.meta.service_instance.updated",
        "meta.service_instance.inserted",
        "meta.service_instance.updated",
        "meta.socket_client.status_received",
      )
      .attachSignal(
        "meta.service_registry.dependee_registered",
        "meta.socket_shutdown_requested",
        "meta.fetch.destroy_requested",
      );

    Cadenza.createMetaTask("Split service instances", function* (ctx: any) {
      if (!ctx.serviceInstances) {
        return;
      }

      for (const serviceInstance of ctx.serviceInstances) {
        yield { serviceInstance };
      }
    })
      .doOn(
        "meta.service_registry.registered_global_signals",
        "meta.service_registry.registered_global_intents",
      )
      .then(this.handleInstanceUpdateTask);

    this.handleGlobalSignalRegistrationTask = Cadenza.createMetaTask(
      "Handle global Signal Registration",
      (ctx) => {
        const { signalToTaskMaps } = ctx;
        const sortedSignalToTaskMap = signalToTaskMaps.sort(
          (a: any, b: any) => {
            if (a.deleted && !b.deleted) return -1;
            if (!a.deleted && b.deleted) return 1;
            return 0;
          },
        );

        const locallyEmittedSignals = Cadenza.signalBroker
          .listEmittedSignals()
          .filter((s: any) => s.startsWith("global."));

        for (const map of sortedSignalToTaskMap) {
          if (map.deleted) {
            this.remoteSignals.get(map.serviceName)?.delete(map.signalName);

            if (!this.remoteSignals.get(map.serviceName)?.size) {
              this.remoteSignals.delete(map.serviceName);
            }

            Cadenza.get(
              `Transmit signal: ${map.signalName} to ${map.serviceName}`,
            )?.destroy();
            continue;
          }

          if (locallyEmittedSignals.includes(map.signalName)) {
            if (!this.remoteSignals.get(map.serviceName)) {
              this.remoteSignals.set(map.serviceName, new Set());
            }

            if (!this.remoteSignals.get(map.serviceName)?.has(map.signalName)) {
              Cadenza.createSignalTransmissionTask(
                map.signalName,
                map.serviceName,
              );
            }

            this.remoteSignals.get(map.serviceName)?.add(map.signalName);
          }
        }

        return true;
      },
      "Handles registration of remote signals",
    )
      .emits("meta.service_registry.registered_global_signals")
      .doOn("global.meta.cadenza_db.gathered_sync_data");

    this.handleGlobalIntentRegistrationTask = Cadenza.createMetaTask(
      "Handle global intent registration",
      (ctx) => {
        const intentToTaskMaps = this.normalizeIntentMaps(ctx);
        const sorted = intentToTaskMaps.sort((a, b) => {
          if (a.deleted && !b.deleted) return -1;
          if (!a.deleted && b.deleted) return 1;
          return 0;
        });

        for (const map of sorted) {
          if (map.deleted) {
            this.unregisterRemoteIntentDeputy(map);
            continue;
          }

          Cadenza.inquiryBroker.addIntent({
            name: map.intentName,
          });

          this.registerRemoteIntentDeputy(map);
        }

        return true;
      },
      "Handles registration of remote inquiry intent responders",
    )
      .emits("meta.service_registry.registered_global_intents")
      .doOn(
        "global.meta.cadenza_db.gathered_sync_data",
        "global.meta.graph_metadata.task_intent_associated",
      );

    this.handleServiceNotRespondingTask = Cadenza.createMetaTask(
      "Handle service not responding",
      (ctx, emit) => {
        const { serviceName, serviceAddress, servicePort } = ctx;
        const serviceInstances = this.instances.get(serviceName);
        const instances = serviceInstances?.filter(
          (i) => i.address === serviceAddress && i.port === servicePort,
        );

        Cadenza.log(
          "Service not responding.",
          {
            serviceName,
            serviceAddress,
            servicePort,
            instances,
          },
          "warning",
          serviceName,
        );

        for (const instance of instances ?? []) {
          instance.isActive = false;
          instance.isNonResponsive = true;
          emit("global.meta.service_registry.service_not_responding", {
            data: {
              isActive: false,
              isNonResponsive: true,
            },
            filter: {
              uuid: instance.uuid,
            },
          });
        }

        return true;
      },
      "Handles service not responding",
    )
      .doOn(
        "meta.fetch.handshake_failed",
        "meta.fetch.handshake_failed.*",
        "meta.socket_client.disconnected",
        "meta.socket_client.disconnected.*",
      )
      .attachSignal("global.meta.service_registry.service_not_responding");

    this.handleServiceHandshakeTask = Cadenza.createMetaTask(
      "Handle service handshake",
      (ctx, emit) => {
        const { serviceName, serviceInstanceId, serviceAddress, servicePort } =
          ctx;
        const serviceInstances = this.instances.get(serviceName);
        const instance = serviceInstances?.find(
          (i) => i.uuid === serviceInstanceId,
        );

        if (!instance) {
          return false;
        }

        instance.isActive = true;
        instance.isNonResponsive = false;
        emit("global.meta.service_registry.service_handshake", {
          data: {
            isActive: instance.isActive,
            isNonResponsive: instance.isNonResponsive,
          },
          filter: {
            uuid: instance.uuid,
          },
        });

        const instancesToDelete = serviceInstances?.filter(
          (i) =>
            i.uuid !== serviceInstanceId &&
            i.address === serviceAddress &&
            i.port === servicePort,
        );

        for (const i of instancesToDelete ?? []) {
          const indexToDelete = this.instances.get(serviceName)?.indexOf(i) ?? -1;
          if (indexToDelete >= 0) {
            this.instances.get(serviceName)?.splice(indexToDelete, 1);
          }
          emit("global.meta.service_registry.deleted", {
            data: {
              isActive: false,
              isNonResponsive: false,
              deleted: true,
            },
            filter: {
              uuid: i.uuid,
            },
          });
        }

        return true;
      },
      "Handles service handshake",
    )
      .doOn("meta.fetch.handshake_complete")
      .attachSignal(
        "global.meta.service_registry.service_handshake",
        "global.meta.service_registry.deleted",
      );

    this.handleSocketStatusUpdateTask = Cadenza.createMetaTask(
      "Handle Socket Status Update",
      (ctx) => {
        const instanceId = ctx.__serviceInstanceId;
        const serviceName = ctx.__serviceName;
        const instances = this.instances.get(serviceName);
        const instance = instances?.find((i) => i.uuid === instanceId);
        if (instance) {
          instance.health = ctx.health;
          instance.numberOfRunningGraphs = ctx.numberOfRunningGraphs;
        }
        return true;
      },
      "Handles status update from socket broadcast",
    ).doOn("meta.socket_client.status_received");

    const mergeSyncDataTask = Cadenza.createUniqueMetaTask(
      "Merge sync data",
      (ctx) => {
        let joinedContext: any = {};
        ctx.joinedContexts.forEach((ctx: any) => {
          joinedContext = { ...joinedContext, ...ctx };
        });
        return joinedContext;
      },
    )
      .emits("meta.service_registry.initial_sync_complete")
      .then(
        this.handleGlobalSignalRegistrationTask,
        this.handleGlobalIntentRegistrationTask,
      );

    this.fullSyncTask = Cadenza.createMetaRoutine("Full sync", [
      Cadenza.createCadenzaDBQueryTask("signal_to_task_map", {
        filter: {
          isGlobal: true,
        },
        fields: ["signal_name", "service_name", "deleted"],
      }).then(mergeSyncDataTask),
      Cadenza.createCadenzaDBQueryTask("intent_to_task_map", {
        fields: [
          "intent_name",
          "task_name",
          "task_version",
          "service_name",
          "deleted",
        ],
      }).then(mergeSyncDataTask),
      Cadenza.createCadenzaDBQueryTask("service_instance", {
        filter: {
          deleted: false,
          is_active: true,
          is_non_responsive: false,
          is_blocked: false,
        },
        fields: [
          "uuid",
          "address",
          "port",
          "service_name",
          "is_active",
          "is_non_responsive",
          "is_blocked",
          "health",
          "exposed",
          "created",
          "is_frontend",
        ],
      }).then(mergeSyncDataTask),
    ]).doOn("meta.sync_requested");

    this.getInstanceById = Cadenza.createMetaTask(
      "Get instance by id",
      (context) => {
        const { __id } = context;
        let instance;
        for (const instances of this.instances.values()) {
          instance = instances.find((i) => i.uuid === __id);
          if (instance) break;
        }
        return { ...context, __instance: instance };
      },
      "Gets instance by id.",
    );

    this.getInstancesByServiceName = Cadenza.createMetaTask(
      "Get instances by name",
      (context) => {
        const { __serviceName } = context;
        const instances = this.instances.get(__serviceName);
        if (!instances) {
          return false;
        }

        return { ...context, __instances: instances };
      },
      "Gets instances by name.",
    );

    this.handleDeputyRegistrationTask = Cadenza.createMetaTask(
      "Handle Deputy Registration",
      (ctx) => {
        const { serviceName } = ctx;

        if (!this.deputies.has(serviceName)) this.deputies.set(serviceName, []);

        this.deputies.get(serviceName)!.push({
          serviceName,
          remoteRoutineName: ctx.remoteRoutineName,
          signalName: ctx.signalName,
          localTaskName: ctx.localTaskName,
          communicationType: ctx.communicationType,
        });
      },
    ).doOn("meta.deputy.created");

    this.getAllInstances = Cadenza.createMetaTask(
      "Get all instances",
      (context) => ({
        ...context,
        __instances: Array.from(this.instances.values()).flat(),
      }),
      "Gets all instances.",
    );

    this.doForEachInstance = Cadenza.createMetaTask(
      "Do for each instance",
      function* (context: AnyObject) {
        // @ts-ignore
        for (const instances of this.instances.values()) {
          for (const instance of instances) {
            yield { ...context, __instance: instance };
          }
        }
      }.bind(this),
      "Yields each instance for branching.",
    );

    this.deleteInstance = Cadenza.createMetaTask(
      "Delete instance",
      (context) => {
        const { __id } = context;
        this.instances.delete(__id);
        return context;
      },
      "Deletes instance.",
    ).doOn("global.meta.service_instance.deleted");

    this.getBalancedInstance = Cadenza.createMetaTask(
      "Get balanced instance",
      (context, emit) => {
        const { __serviceName, __triedInstances, __retries, __broadcast } =
          context;
        let retries = __retries ?? 0;
        let triedInstances = __triedInstances ?? [];

        const instances = this.instances
          .get(__serviceName)
          ?.filter((i) => i.isActive && !i.isNonResponsive && !i.isBlocked)
          .sort((a, b) => a.numberOfRunningGraphs! - b.numberOfRunningGraphs!);

        if (!instances || instances.length === 0 || retries > this.retryCount) {
          context.errored = true;
          context.__error = `No active instances for ${__serviceName}. Retries: ${retries}. ${this.instances.get(
            __serviceName,
          )}`;
          emit(
            `meta.service_registry.load_balance_failed:${context.__metadata.__deputyExecId}`,
            context,
          );
          return context;
        }

        if (__broadcast || instances[0].isFrontend) {
          for (const instance of instances) {
            const socketKey = instance.isFrontend
              ? instance.address
              : `${instance.address}_${instance.port}`;
            emit(
              `meta.service_registry.selected_instance_for_socket:${socketKey}`,
              context,
            );
          }

          return context;
        }

        let instancesToTry = instances.filter(
          (i) => !__triedInstances?.includes(i.uuid),
        );

        if (instancesToTry.length === 0) {
          if (this.useSocket) {
            emit(
              `meta.service_registry.socket_failed:${context.__fetchId}`,
              context,
            );
          }
          retries++;
          instancesToTry = instances;
          triedInstances = [];
        }

        let selected = instancesToTry[0];
        if (retries > 0) {
          selected =
            instancesToTry[Math.floor(Math.random() * instancesToTry.length)];
        }

        context.__instance = selected.uuid;
        context.__fetchId = `${selected.address}_${selected.port}`;
        context.__triedInstances = triedInstances;
        context.__triedInstances.push(selected.uuid);
        context.__retries = retries;

        if (this.useSocket) {
          emit(
            `meta.service_registry.selected_instance_for_socket:${context.__fetchId}`,
            context,
          );
        } else {
          emit(
            `meta.service_registry.selected_instance_for_fetch:${context.__fetchId}`,
            context,
          );
        }

        return context;
      },
      "Gets a balanced instance for load balancing",
    )
      .doOn(
        "meta.deputy.delegation_requested",
        "meta.signal_transmission.requested",
        "meta.socket_client.delegate_failed",
        "meta.fetch.delegate_failed",
        "meta.socket_client.signal_transmission_failed",
      )
      .attachSignal(
        "meta.service_registry.load_balance_failed",
        "meta.service_registry.selected_instance_for_socket",
        "meta.service_registry.selected_instance_for_fetch",
        "meta.service_registry.socket_failed",
      );

    this.getStatusTask = Cadenza.createMetaTask("Get status", (ctx) => {
      if (!this.serviceName) {
        return {
          __status: "error",
          __error: "No service name defined",
          errored: true,
        };
      }

      if (!this.serviceInstanceId) {
        return {
          __status: "error",
          __error: "No service instance id defined",
          errored: true,
        };
      }

      const self = this.instances
        .get(this.serviceName)
        ?.find((i) => i.uuid === this.serviceInstanceId);

      return {
        ...ctx,
        __status: "ok",
        __numberOfRunningGraphs: self?.numberOfRunningGraphs ?? 0,
        __health: self?.health ?? {},
        __active: self?.isActive ?? false,
      };
    }).doOn("meta.socket.status_check_requested");

    this.collectTransportDiagnosticsTask = Cadenza.createMetaTask(
      "Collect transport diagnostics",
      async (ctx) => {
        const inquiryResult = await Cadenza.inquire(
          META_RUNTIME_TRANSPORT_DIAGNOSTICS_INTENT,
          {
            detailLevel: ctx.detailLevel,
            includeErrorHistory: ctx.includeErrorHistory,
            errorHistoryLimit: ctx.errorHistoryLimit,
          },
          ctx.inquiryOptions ?? ctx.__inquiryOptions ?? {},
        );

        return {
          ...ctx,
          ...inquiryResult,
        };
      },
      "Collects distributed transport diagnostics using inquiry responders.",
    )
      .doOn("meta.service_registry.transport_diagnostics_requested")
      .emits("meta.service_registry.transport_diagnostics_collected")
      .emitsOnFail("meta.service_registry.transport_diagnostics_failed");

    this.insertServiceTask = Cadenza.createCadenzaDBInsertTask(
      "service",
      {
        onConflict: {
          target: ["name"],
          action: {
            do: "nothing",
          },
        },
      },
      {
        // validateInputContext: true,
        inputSchema: {
          type: "object",
          properties: {
            data: {
              type: "object",
              properties: {
                name: {
                  type: "string",
                },
                description: {
                  type: "string",
                },
                display_name: {
                  type: "string",
                },
                is_meta: {
                  type: "boolean",
                },
              },
              required: ["name"],
            },
          },
          required: ["data"],
        },
        outputSchema: {
          type: "object",
          properties: {
            __serviceName: {
              type: "string",
            },
          },
          required: ["__serviceName"],
        },
        retryCount: 100,
        retryDelay: 10000,
        retryDelayMax: 60000,
        retryDelayFactor: 1.3,
      },
    )
      .emits("meta.service_registry.service_inserted")
      .emitsOnFail("meta.service_registry.service_insertion_failed");

    this.insertServiceInstanceTask = Cadenza.createCadenzaDBInsertTask(
      "serviceInstance",
      {},
      {
        inputSchema: {
          type: "object",
          properties: {
            uuid: {
              type: "string",
            },
            address: {
              type: "string",
            },
            port: {
              type: "number",
            },
            process_pid: {
              type: "number",
            },
            is_primary: {
              type: "boolean",
            },
            service_name: {
              type: "string",
            },
            is_active: {
              type: "boolean",
            },
            is_non_responsive: {
              type: "boolean",
            },
            is_blocked: {
              type: "boolean",
            },
            exposed: {
              type: "boolean",
            },
          },
          required: [
            "id",
            "address",
            "port",
            "process_pid",
            "service_name",
            "exposed",
          ],
        },
        // validateInputContext: true,
        outputSchema: {
          type: "object",
          properties: {
            id: {
              type: "string",
            },
          },
          required: ["id"],
        },
        // validateOutputContext: true,
        retryCount: 5,
        retryDelay: 1000,
      },
    )
      .doOn("global.meta.rest.network_configured", "meta.rest.browser_detected")
      .then(
        Cadenza.createMetaTask(
          "Setup service",
          (ctx) => {
            const { serviceInstance, data, __useSocket, __retryCount } = ctx;
            this.serviceInstanceId = serviceInstance?.uuid ?? data?.uuid;
            this.instances.set(
              data?.service_name ?? serviceInstance?.service_name,
              [{ ...(serviceInstance ?? data) }],
            );
            this.useSocket = __useSocket;
            this.retryCount = __retryCount;
            console.log("SETUP SERVICE", this.serviceInstanceId);
            return true;
          },
          "Sets service instance id after insertion",
        ).emits("meta.service_registry.instance_inserted"),
      );

    Cadenza.createMetaTask(
      "Handle service creation",
      (ctx) => {
        if (!ctx.__cadenzaDBConnect) {
          ctx.__skipRemoteExecution = true;
        }

        if (isBrowser) {
          Cadenza.createMetaTask("Prepare for signal sync", () => {
            return {};
          })
            // .doAfter(this.fullSyncTask)
            .then(
              Cadenza.createCadenzaDBQueryTask("signal_registry", {
                fields: ["name"],
                filter: {
                  global: true,
                },
              }).then(
                Cadenza.createMetaTask(
                  // TODO this is outdated. Fix it.
                  "Create signal transmission tasks",
                  (ctx, emit) => {
                    const signalRegistry = ctx.signalRegistry;
                    for (const signal of signalRegistry) {
                      emit("meta.service_registry.foreign_signal_registered", {
                        __emitterSignalName: signal.name,
                        __listenerServiceName: signal.serviceName,
                      });
                    }

                    return true;
                  },
                ).then(
                  Cadenza.createMetaTask("Connect to services", (ctx, emit) => {
                    const services: string[] = Array.from(
                      new Set(
                        ctx.signalRegistry.map((s: any) => s.serviceName),
                      ),
                    );
                    for (const service of services) {
                      const instances = this.instances
                        .get(service)!
                        .filter((i) => i.isActive);
                      for (const instance of instances) {
                        if (instance.clientCreated) continue;
                        const address = instance.address;
                        const port = instance.port;

                        const clientCreated = instances?.some(
                          (i) =>
                            i.address === address &&
                            i.port === port &&
                            i.clientCreated &&
                            i.isActive,
                        );

                        if (!clientCreated) {
                          emit("meta.service_registry.dependee_registered", {
                            serviceName: service,
                            serviceInstanceId: instance.uuid,
                            serviceAddress: address,
                            servicePort: port,
                            protocol: instance.exposed ? "https" : "http",
                            communicationTypes: ["signal"],
                          });
                        }

                        instance.clientCreated = true;
                        instances.forEach((i) => {
                          if (i.address === address && i.port === port) {
                            i.clientCreated = true;
                          }
                        });
                      }
                    }
                    return {};
                  }),
                ),
              ),
            );
        }

        return ctx;
      },
      "Handles the request to create a service instance",
    )
      .doOn("meta.create_service_requested")
      .then(this.insertServiceTask);
  }

  reset() {
    this.instances.clear();
    this.deputies.clear();
    this.remoteSignals.clear();
    this.remoteIntents.clear();
    this.remoteIntentDeputiesByKey.clear();
    this.remoteIntentDeputiesByTask.clear();
  }
}
