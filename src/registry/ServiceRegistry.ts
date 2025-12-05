import { GraphRoutine, Task } from "@cadenza.io/core";
import type { AnyObject } from "@cadenza.io/core";
import Cadenza from "../Cadenza";
import { isBrowser } from "../utils/environment";

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
  serviceName: string | null = null;
  serviceInstanceId: string | null = null;
  numberOfRunningGraphs: number = 0;
  useSocket: boolean = false;
  retryCount: number = 3;

  handleInstanceUpdateTask: Task;
  handleGlobalSignalRegistrationTask: Task;
  getRemoteSignalsTask: Task;
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
    this.handleInstanceUpdateTask = Cadenza.createMetaTask(
      "Handle Instance Update",
      (ctx, emit) => {
        const { serviceInstance } = ctx;
        const { uuid, serviceName, address, port, exposed, isFrontend } =
          serviceInstance;
        if (uuid === this.serviceInstanceId) return;

        if (!this.instances.has(serviceName))
          this.instances.set(serviceName, []);
        const instances = this.instances.get(serviceName)!;
        const existing = instances.find((i) => i.uuid === uuid);

        if (existing) {
          Object.assign(existing, serviceInstance); // Update
        } else {
          instances.push(serviceInstance);
        }

        if (
          (!isFrontend && this.deputies.has(serviceName)) ||
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
                  i.clientCreated &&
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
      .attachSignal("meta.service_registry.dependee_registered");

    Cadenza.createMetaTask("Split service instances", function* (ctx: any) {
      if (!ctx.serviceInstances) {
        return;
      }

      for (const serviceInstance of ctx.serviceInstances) {
        yield { serviceInstance };
      }
    })
      .doOn("meta.service_registry.registered_global_signals")
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

        console.log("signalToTaskMap", sortedSignalToTaskMap);

        const locallyEmittedSignals = Cadenza.broker
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

    this.getRemoteSignalsTask = Cadenza.createMetaTask(
      "Get remote signals",
      (ctx) => {
        const { serviceName } = ctx;
        return {
          remoteSignals: this.remoteSignals.get(serviceName) ?? [],
          ...ctx,
        };
      },
      "Gets remote signals",
    ).doOn(
      "meta.register_remote_signals_requested",
      "meta.fetch.handshake_complete",
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
      .doOn("meta.fetch.handshake_failed", "meta.socket_client.disconnected")
      .attachSignal("global.meta.service_registry.service_not_responding");

    this.handleServiceHandshakeTask = Cadenza.createMetaTask(
      "Handle service handshake",
      (ctx, emit) => {
        const { serviceName, serviceAddress, servicePort } = ctx;
        const serviceInstances = this.instances.get(serviceName);
        const instances = serviceInstances?.filter(
          (i) => i.address === serviceAddress && i.port === servicePort,
        );
        for (const instance of instances ?? []) {
          instance.isActive = true;
          instance.isNonResponsive = true;
          emit("global.meta.service_registry.service_handshake", {
            data: {
              isActive: instance.isActive,
              isNonResponsive: instance.isNonResponsive,
            },
            filter: {
              uuid: instance.uuid,
            },
          });
        }
        return true;
      },
      "Handles service handshake",
    )
      .doOn("meta.fetch.handshake_complete")
      .attachSignal("global.meta.service_registry.service_handshake");

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
        console.log("Full sync joinedContext", joinedContext);
        return joinedContext;
      },
    ).then(this.handleGlobalSignalRegistrationTask);

    this.fullSyncTask = Cadenza.createMetaRoutine("Full sync", [
      Cadenza.createCadenzaDBQueryTask("signal_to_task_map", {
        fields: ["signal_name", "service_name", "deleted"],
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
    )
      .doOn("meta.deputy.created")
      .emits("meta.service_registry.deputy_registered");

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
          context.__error = "No active instances";
          emit(
            `meta.service_registry.load_balance_failed:${context.__metadata.__deputyExecId}`,
            context,
          );
          return context;
        }

        if (__broadcast || instances[0].isFrontend) {
          for (const instance of instances) {
            emit(
              `meta.service_registry.selected_instance_for_socket:${instance.address}`,
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
  }
}
