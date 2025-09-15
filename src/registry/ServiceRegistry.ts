import { AnyObject, Task } from "@cadenza.io/core";
import Cadenza from "../Cadenza";

export interface ServiceInstanceDescriptor {
  id: string;
  address: string;
  port: number;
  serviceName: string;
  numberOfRunningGraphs?: number;
  isActive: boolean;
  isNonResponsive: boolean;
  isBlocked: boolean;
  health: AnyObject;
  exposed: boolean;
}

export interface DeputyDescriptor {
  __serviceName: string;
  __remoteRoutineName: string;
  __localTaskName: string;
}

export default class ServiceRegistry {
  private static _instance: ServiceRegistry;
  public static get instance(): ServiceRegistry {
    if (!this._instance) this._instance = new ServiceRegistry();
    return this._instance;
  }

  private instances: Map<string, ServiceInstanceDescriptor[]> = new Map();
  private deputies: Map<string, DeputyDescriptor> = new Map();
  serviceName: string | null = null;
  serviceInstanceId: string | null = null;
  useSocket: boolean = false;
  retryCount: number = 3;

  handleInstanceUpdateTask: Task;
  handleSocketStatusUpdateTask: Task;
  fullSyncTask: Task;
  getAllInstances: Task;
  doForEachInstance: Task;
  deleteInstance: Task;
  getBalancedInstance: Task;
  updateInstanceId: Task;
  getInstanceById: Task;
  getInstancesByServiceName: Task;
  handleDeputyRegistrationTask: Task;
  getStatusTask: Task;
  insertServiceTask: Task;
  insertServiceInstanceTask: Task;

  private constructor() {
    this.handleInstanceUpdateTask = Cadenza.createMetaTask(
      "Handle Instance Update",
      (ctx, emit) => {
        const { serviceInstance } = ctx;
        const { id, serviceName, address, port, exposed } = serviceInstance;
        if (!this.instances.has(serviceName))
          this.instances.set(serviceName, []);
        const instances = this.instances.get(serviceName)!;
        const existing = instances.find((i) => i.id === id);
        if (existing) {
          Object.assign(existing, serviceInstance); // Update
        } else {
          if (this.deputies.has(serviceName)) {
            emit("meta.service_registry.dependee_registered", {
              __serviceName: serviceName,
              __serviceInstanceId: id,
              __serviceAddress: address,
              __servicePort: port,
              __protocol: exposed ? "https" : "http",
            });
          }
          instances.push(serviceInstance); // Insert
        }

        return true;
      },
      "Handles instance update from DB signal",
    )
      .emits("meta.service_registry.service_discovered")
      .doOn(
        "meta.initializing_service",
        "CadenzaDB.meta.service_instance.inserted",
        "CadenzaDB.meta.service_instance.updated",
      );

    this.handleSocketStatusUpdateTask = Cadenza.createMetaTask(
      "Handle Socket Status Update",
      (ctx) => {
        const instanceId = ctx.__serviceInstanceId;
        const serviceName = ctx.__serviceName;
        const instances = this.instances.get(serviceName);
        const instance = instances?.find((i) => i.id === instanceId);
        if (instance) {
          instance.health = ctx.health;
          instance.numberOfRunningGraphs = ctx.numberOfRunningGraphs;
        }
        return true;
      },
      "Handles status update from socket broadcast",
    ).doOn("meta.socket_client.status_received");

    this.fullSyncTask = Cadenza.createCadenzaDBQueryTask("service_instance", {
      filter: {
        deleted: false,
        is_active: true,
        is_non_responsive: false,
        is_blocked: false,
      },
      fields: [
        "id",
        "address",
        "port",
        "service_name",
        "is_active",
        "is_non_responsive",
        "is_blocked",
        "health",
        "exposed",
      ],
    })
      .doOn(
        "meta.service_registry_sync_requested",
        "meta.service_registry.instance_inserted",
      )
      .then(
        Cadenza.createMetaTask("Split service instances", function* (ctx) {
          const { serviceInstances } = ctx;
          if (!serviceInstances) return;
          for (const serviceInstance of serviceInstances) {
            yield serviceInstance;
          }
        }).then(this.handleInstanceUpdateTask),
      );

    this.updateInstanceId = Cadenza.createMetaTask(
      "Update instance id",
      (context) => {
        const { __id, __oldId } = context;
        const instance = this.instances.get(__oldId);
        if (!instance) return context;
        this.instances.set(__id, instance);
        this.instances.delete(__oldId);
        return context;
      },
      "Updates instance id.",
    ).doOn("meta.service.global_id_set");

    this.getInstanceById = Cadenza.createMetaTask(
      "Get instance by id",
      (context) => {
        const { __id } = context;
        let instance;
        for (const instances of this.instances.values()) {
          instance = instances.find((i) => i.id === __id);
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
      (ctx, emit) => {
        const { __serviceName } = ctx;

        this.deputies.set(__serviceName, {
          __serviceName,
          __remoteRoutineName: ctx.__remoteRoutineName,
          __localTaskName: ctx.__localTaskName,
        });

        for (const instance of this.instances.get(__serviceName)!) {
          emit(`meta.service_registry.dependee_registered:${instance.id}`, {
            __serviceName,
            __serviceInstanceId: instance.id,
            __serviceAddress: instance.address,
            __servicePort: instance.port,
            __protocol: instance.exposed ? "https" : "http",
          });
        }
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
    ).doOn("CadenzaDB.meta.service_instance.deleted");

    this.getBalancedInstance = Cadenza.createMetaTask(
      "Get balanced instance",
      (context, emit) => {
        const { __serviceName, __triedInstances, __retries } = context;
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
            `meta.service_registry.load_balance_failed:${context.__deputyExecId}`,
            context,
          );
          return context;
        }

        let instancesToTry = instances.filter(
          (i) => !__triedInstances?.includes(i.id),
        );

        if (instancesToTry.length === 0) {
          if (this.useSocket) {
            emit(
              `meta.service_registry.socket_failed:${context.__instance}`,
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

        context.__instance = selected.id;
        context.__triedInstances = triedInstances;
        context.__triedInstances.push(selected.id);
        context.__retries = retries;

        if (this.useSocket) {
          emit(
            `meta.service_registry.selected_instance_for_socket:${context.__instance}`,
            context,
          );
        } else {
          emit(
            `meta.service_registry.selected_instance_for_fetch:${context.__instance}`,
            context,
          );
        }

        return context;
      },
      "Gets a balanced instance for load balancing",
    ).doOn(
      "meta.deputy.delegation_requested",
      "meta.signal_transmission.requested",
      "meta.socket_client.delegate_failed",
      "meta.fetch.delegate_failed",
      "meta.socket_client.signal_transmission_failed",
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
        ?.find((i) => i.id === this.serviceInstanceId);

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
      {},
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
      .then(
        Cadenza.createMetaTask(
          "Set service name",
          ({ __serviceName }) => {
            this.serviceName = __serviceName;
            return true;
          },
          "Sets service name after insertion",
        ),
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
            id: {
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
      .doOn("meta.rest.network_configured")
      .then(
        Cadenza.createMetaTask(
          "Setup service",
          (ctx) => {
            const { serviceInstance, data, __useSocket, __retryCount } = ctx;
            this.serviceInstanceId = data?.id ?? serviceInstance?.id;
            this.instances.set(
              data?.service_name ?? serviceInstance?.serviceName,
              [{ ...(data ?? serviceInstance) }],
            );
            this.useSocket = __useSocket;
            this.retryCount = __retryCount;
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

        console.log("service creation");

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
