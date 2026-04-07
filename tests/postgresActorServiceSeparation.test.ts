import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { GraphContext } from "@cadenza.io/core";

import Cadenza from "../src/Cadenza";
import DatabaseController from "../src/database/DatabaseController";
import RestController from "../src/network/RestController";
import ServiceRegistry from "../src/registry/ServiceRegistry";
import SignalController from "../src/signals/SignalController";
import SocketController from "../src/network/SocketController";
import type { DatabaseSchemaDefinition } from "../src/types/database";

async function waitForCondition(
  predicate: () => boolean | Promise<boolean>,
  timeoutMs = 1_000,
  pollIntervalMs = 10,
): Promise<void> {
  const startedAt = Date.now();

  while (Date.now() - startedAt < timeoutMs) {
    if (await predicate()) {
      return;
    }

    await new Promise((resolve) => setTimeout(resolve, pollIntervalMs));
  }

  throw new Error("Condition not met within timeout");
}

function resetRuntimeState() {
  try {
    Cadenza.reset();
  } catch {
    // Ignore first-run reset errors before bootstrap.
  }

  (Cadenza as any).isBootstrapped = false;
  (Cadenza as any).serviceCreated = false;
  (Cadenza as any).warnedInvalidMetaIntentResponderKeys = new Set();
  (DatabaseController as any)._instance = undefined;
  (RestController as any)._instance = undefined;
  (ServiceRegistry as any)._instance = undefined;
  (SignalController as any)._instance = undefined;
  (SocketController as any)._instance = undefined;
}

const schema: DatabaseSchemaDefinition = {
  version: 1,
  tables: {
    telemetry: {
      fields: {
        uuid: {
          type: "uuid",
          primary: true,
          required: true,
        },
      },
    },
  },
};

describe("PostgresActor and database service separation", () => {
  let consoleLogSpy: ReturnType<typeof vi.spyOn>;
  let consoleWarnSpy: ReturnType<typeof vi.spyOn>;
  let consoleErrorSpy: ReturnType<typeof vi.spyOn>;

  beforeEach(() => {
    consoleLogSpy = vi.spyOn(console, "log").mockImplementation(() => {});
    consoleWarnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
    consoleErrorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    resetRuntimeState();
    Cadenza.bootstrap();
    Cadenza.setMode("production");
  });

  afterEach(() => {
    resetRuntimeState();
    consoleLogSpy.mockRestore();
    consoleWarnSpy.mockRestore();
    consoleErrorSpy.mockRestore();
    vi.restoreAllMocks();
  });

  it("registers PostgresActors by actor identity, not owner service identity", () => {
    const controller = DatabaseController.instance;

    const readActor = controller.createPostgresActor(
      "OrdersReadModel",
      schema,
      "Read model actor",
      {
        databaseName: "orders_read",
        ownerServiceName: "AppService",
      },
    );
    const writeActor = controller.createPostgresActor(
      "OrdersWriteModel",
      schema,
      "Write model actor",
      {
        databaseName: "orders_write",
        ownerServiceName: "AppService",
      },
    );

    expect(readActor.actorName).toBe("OrdersReadModelPostgresActor");
    expect(writeActor.actorName).toBe("OrdersWriteModelPostgresActor");
    expect(readActor.ownerServiceName).toBe("AppService");
    expect(writeActor.ownerServiceName).toBe("AppService");
    expect(readActor.actorToken).not.toBe(writeActor.actorToken);
  });

  it("creates a PostgresActor without creating or mutating the enclosing service", () => {
    const controller = DatabaseController.instance;
    const requestSetupSpy = vi
      .spyOn(controller, "requestPostgresActorSetup")
      .mockImplementation(() => undefined);
    const createServiceSpy = vi
      .spyOn(Cadenza, "createCadenzaService")
      .mockImplementation(() => undefined);

    (Cadenza as any).serviceCreated = true;
    Cadenza.serviceRegistry.serviceName = "AppService";

    Cadenza.createPostgresActor("OrdersReadModel", schema, "Read model actor");

    expect(createServiceSpy).not.toHaveBeenCalled();
    expect(requestSetupSpy).toHaveBeenCalledOnce();
    expect(Cadenza.serviceRegistry.serviceName).toBe("AppService");
    expect(Cadenza.getActor("OrdersReadModelPostgresActor")).toBeDefined();
  });

  it("creates a database service only after the PostgresActor setup completes", async () => {
    const controller = DatabaseController.instance;
    const registration = {
      actorName: "MetricsDBPostgresActor",
      actorToken: "metrics-db-postgres-actor",
      actorKey: "metrics_db",
      databaseName: "metrics_db",
      ownerServiceName: "MetricsDB",
      setupSignal: "meta.postgres_actor.setup_requested.metrics-db-postgres-actor",
      setupDoneSignal: "meta.postgres_actor.setup_done.metrics-db-postgres-actor",
      setupFailedSignal: "meta.postgres_actor.setup_failed.metrics-db-postgres-actor",
      actor: {} as any,
      schema,
      description: "Metrics DB actor",
      options: {
        databaseName: "metrics_db",
        ownerServiceName: "MetricsDB",
      },
      tasksGenerated: false,
      intentNames: new Set<string>(),
    };

    vi.spyOn(controller, "createPostgresActor").mockReturnValue(registration as any);
    const requestSetupSpy = vi
      .spyOn(controller, "requestPostgresActorSetup")
      .mockImplementation(() => registration as any);
    const createServiceSpy = vi
      .spyOn(Cadenza, "createCadenzaService")
      .mockImplementation(() => undefined);

    Cadenza.createDatabaseService("MetricsDB", schema, "Metrics DB service");

    expect(requestSetupSpy).toHaveBeenCalledOnce();
    expect(createServiceSpy).not.toHaveBeenCalled();

    Cadenza.emit(registration.setupDoneSignal, {
      actorName: registration.actorName,
      databaseName: registration.databaseName,
    });

    await waitForCondition(() => createServiceSpy.mock.calls.length === 1);

    expect(createServiceSpy).toHaveBeenCalledWith(
      "MetricsDB",
      "Metrics DB service",
      expect.objectContaining({
        databaseName: "metrics_db",
        isDatabase: true,
        ownerServiceName: "MetricsDB",
      }),
    );
  });

  it("emits snake_case service registration payloads during service bootstrap", async () => {
    const emitSpy = vi.spyOn(Cadenza, "emit");
    Cadenza.createCadenzaService("MetricsDB", "Metrics DB service", {
      cadenzaDB: {
        connect: false,
      },
    });

    await waitForCondition(() =>
      emitSpy.mock.calls.some(
        ([signalName]) => signalName === "meta.create_service_requested",
      ),
    );

    const createServiceCall = emitSpy.mock.calls.find(
      ([signalName]) => signalName === "meta.create_service_requested",
    );

    expect(createServiceCall?.[1]).toMatchObject({
      data: {
        name: "MetricsDB",
        description: "Metrics DB service",
        display_name: "",
        is_meta: false,
      },
      __registrationData: {
        name: "MetricsDB",
        description: "Metrics DB service",
        display_name: "",
        is_meta: false,
      },
    });
  });

  it("disables generated db task input validation by default for meta actors", () => {
    const controller = DatabaseController.instance;

    const registration = controller.createPostgresActor(
      "MetaRegistry",
      schema,
      "Meta registry actor",
      {
        databaseName: "meta_registry",
        ownerServiceName: "CadenzaDB",
        isMeta: true,
      },
    );

    expect(() =>
      (controller as unknown as {
        generateDatabaseTasks: (value: unknown) => void;
      }).generateDatabaseTasks(registration),
    ).not.toThrow();

    expect(Cadenza.get("Insert telemetry")?.validateInputContext).toBe(false);
    expect(Cadenza.get("Query telemetry")?.validateInputContext).toBe(false);
  });

  it("keeps generated db task input validation enabled for non-meta actors", () => {
    const controller = DatabaseController.instance;

    const registration = controller.createPostgresActor(
      "BusinessMetrics",
      schema,
      "Business metrics actor",
      {
        databaseName: "business_metrics",
        ownerServiceName: "MetricsService",
      },
    );

    expect(() =>
      (controller as unknown as {
        generateDatabaseTasks: (value: unknown) => void;
      }).generateDatabaseTasks(registration),
    ).not.toThrow();

    expect(Cadenza.get("Insert telemetry")?.validateInputContext).toBe(true);
    expect(Cadenza.get("Query telemetry")?.validateInputContext).toBe(true);
  });

  it("does not create a database service when the PostgresActor setup fails", async () => {
    const controller = DatabaseController.instance;
    const registration = {
      actorName: "FailingDBPostgresActor",
      actorToken: "failing-db-postgres-actor",
      actorKey: "failing_db",
      databaseName: "failing_db",
      ownerServiceName: "FailingDB",
      setupSignal: "meta.postgres_actor.setup_requested.failing-db-postgres-actor",
      setupDoneSignal: "meta.postgres_actor.setup_done.failing-db-postgres-actor",
      setupFailedSignal: "meta.postgres_actor.setup_failed.failing-db-postgres-actor",
      actor: {} as any,
      schema,
      description: "Failing DB actor",
      options: {
        databaseName: "failing_db",
        ownerServiceName: "FailingDB",
      },
      tasksGenerated: false,
      intentNames: new Set<string>(),
    };

    vi.spyOn(controller, "createPostgresActor").mockReturnValue(registration as any);
    vi.spyOn(controller, "requestPostgresActorSetup").mockImplementation(
      () => registration as any,
    );
    const createServiceSpy = vi
      .spyOn(Cadenza, "createCadenzaService")
      .mockImplementation(() => undefined);

    Cadenza.createDatabaseService("FailingDB", schema, "Failing DB service");
    Cadenza.emit(registration.setupFailedSignal, {
      actorName: registration.actorName,
      databaseName: registration.databaseName,
      __error: "boom",
      errored: true,
    });

    await new Promise((resolve) => setTimeout(resolve, 25));

    expect(createServiceSpy).not.toHaveBeenCalled();
  });

  it("routes default database tasks to the created database service", () => {
    const controller = DatabaseController.instance;
    const registration = {
      actorName: "MetricsDBPostgresActor",
      actorToken: "metrics-db-postgres-actor",
      actorKey: "metrics_db",
      databaseName: "metrics_db",
      ownerServiceName: "MetricsDB",
      setupSignal: "meta.postgres_actor.setup_requested.metrics-db-postgres-actor",
      setupDoneSignal: "meta.postgres_actor.setup_done.metrics-db-postgres-actor",
      setupFailedSignal: "meta.postgres_actor.setup_failed.metrics-db-postgres-actor",
      actor: {} as any,
      schema,
      description: "Metrics DB actor",
      options: {
        databaseName: "metrics_db",
        ownerServiceName: "MetricsDB",
      },
      tasksGenerated: false,
      intentNames: new Set<string>(),
    };

    vi.spyOn(controller, "createPostgresActor").mockReturnValue(registration as any);
    vi.spyOn(controller, "requestPostgresActorSetup").mockImplementation(
      () => registration as any,
    );
    vi.spyOn(Cadenza, "createCadenzaService").mockImplementation(() => undefined);

    Cadenza.createDatabaseService("MetricsDB", schema, "Metrics DB service");
    const defaultInsertTask = Cadenza.createDatabaseInsertTask("telemetry");

    expect((defaultInsertTask as any).serviceName).toBe("MetricsDB");
    expect(defaultInsertTask.name).toBe("Insert telemetry in MetricsDB");
  });

  it("preserves root insert payload fields when delegating remote database inserts", async () => {
    const delegatedContexts: Array<Record<string, unknown>> = [];

    const insertTask = Cadenza.createDatabaseInsertTask("telemetry", "MetricsDB", {
      onConflict: {
        target: ["uuid"],
        action: {
          do: "nothing",
        },
      },
    }) as any;

    const resultPromise = insertTask.execute(
      new GraphContext({
        data: {
          uuid: "telemetry-1",
        },
        onConflict: {
          target: ["name"],
          action: {
            do: "update",
            set: {
              uuid: "excluded",
            },
          },
        },
      }),
      (signal: string, ctx: Record<string, unknown>) => {
        if (signal !== "meta.deputy.delegation_requested") {
          return;
        }

        delegatedContexts.push({
          __serviceName: ctx.__serviceName,
          __remoteRoutineName: ctx.__remoteRoutineName,
          __localTaskName: ctx.__localTaskName,
          __delegationRequestContext: ctx.__delegationRequestContext,
          data: ctx.data,
          onConflict: ctx.onConflict,
          queryData: ctx.queryData,
        });

        queueMicrotask(() => {
          Cadenza.emit(`meta.fetch.delegated:${ctx.__metadata.__deputyExecId}`, {
            ...ctx,
            __success: true,
          });
        });
      },
      async () => ({}),
      () => undefined,
      {
        nodeId: "node-1",
        routineExecId: "routine-1",
      },
    );

    const result = await resultPromise;

    expect(delegatedContexts).toEqual([
      expect.objectContaining({
        data: expect.objectContaining({
          uuid: "telemetry-1",
        }),
        onConflict: expect.objectContaining({
          target: ["uuid"],
        }),
        queryData: expect.objectContaining({
          data: expect.objectContaining({
            uuid: "telemetry-1",
          }),
          onConflict: expect.objectContaining({
            target: ["uuid"],
          }),
        }),
      }),
    ]);
    expect(result).toMatchObject({
      data: expect.objectContaining({
        uuid: "telemetry-1",
      }),
    });
  });

  it("does not strip queryData from reused graph context across delegated insert executions", async () => {
    const delegatedContexts: Array<Record<string, unknown>> = [];

    const insertTask = Cadenza.createDatabaseInsertTask("telemetry", "MetricsDB", {
      onConflict: {
        target: ["uuid"],
        action: {
          do: "nothing",
        },
      },
    }) as any;

    const graphContext = new GraphContext({
      data: {
        uuid: "telemetry-1",
      },
      queryData: {
        data: {
          uuid: "telemetry-1",
        },
        onConflict: {
          target: ["uuid"],
          action: {
            do: "nothing",
          },
        },
      },
    });

    const runInsert = async () =>
      insertTask.execute(
        graphContext,
        (signal: string, ctx: Record<string, unknown>) => {
          if (signal !== "meta.deputy.delegation_requested") {
            return;
          }

          delegatedContexts.push({
            data: ctx.data,
            onConflict: ctx.onConflict,
            queryData: ctx.queryData,
          });

          queueMicrotask(() => {
            Cadenza.emit(`meta.fetch.delegated:${ctx.__metadata.__deputyExecId}`, {
              ...ctx,
              __success: true,
            });
          });
        },
        async () => ({}),
        () => undefined,
        {
          nodeId: "node-1",
          routineExecId: "routine-1",
        },
      );

    await runInsert();
    await runInsert();

    expect(delegatedContexts).toEqual([
      expect.objectContaining({
        data: expect.objectContaining({
          uuid: "telemetry-1",
        }),
        onConflict: expect.objectContaining({
          target: ["uuid"],
        }),
        queryData: expect.objectContaining({
          data: expect.objectContaining({
            uuid: "telemetry-1",
          }),
          onConflict: expect.objectContaining({
            target: ["uuid"],
          }),
        }),
      }),
      expect.objectContaining({
        data: expect.objectContaining({
          uuid: "telemetry-1",
        }),
        onConflict: expect.objectContaining({
          target: ["uuid"],
        }),
        queryData: expect.objectContaining({
          data: expect.objectContaining({
            uuid: "telemetry-1",
          }),
          onConflict: expect.objectContaining({
            target: ["uuid"],
          }),
        }),
      }),
    ]);
  });

  it("restores the original delegation payload when a retry context only contains a prior response", async () => {
    const delegatedContexts: Array<Record<string, unknown>> = [];

    const insertTask = Cadenza.createDatabaseInsertTask("telemetry", "MetricsDB", {
      onConflict: {
        target: ["uuid"],
        action: {
          do: "nothing",
        },
      },
    }) as any;

    const originalDelegationRequest = {
      data: {
        uuid: "telemetry-restore-1",
      },
      queryData: {
        data: {
          uuid: "telemetry-restore-1",
        },
        onConflict: {
          target: ["uuid"],
          action: {
            do: "nothing",
          },
        },
      },
      onConflict: {
        target: ["uuid"],
        action: {
          do: "nothing",
        },
      },
      __remoteRoutineName: "Insert telemetry",
      __serviceName: "MetricsDB",
      __localTaskName: "Insert telemetry in MetricsDB",
    };

    const retriedGraphContext = new GraphContext({
      actorSessionState: null,
      rowCount: 0,
      __success: true,
      __status: "success",
      __isDeputy: true,
      __retries: 1,
      __delegationRequestContext: originalDelegationRequest,
    });

    await insertTask.execute(
      retriedGraphContext,
      (signal: string, ctx: Record<string, unknown>) => {
        if (signal !== "meta.deputy.delegation_requested") {
          return;
        }

        delegatedContexts.push({
          __serviceName: ctx.__serviceName,
          __remoteRoutineName: ctx.__remoteRoutineName,
          __localTaskName: ctx.__localTaskName,
          __delegationRequestContext: ctx.__delegationRequestContext,
          data: ctx.data,
          onConflict: ctx.onConflict,
          queryData: ctx.queryData,
        });

        queueMicrotask(() => {
          Cadenza.emit(`meta.fetch.delegated:${ctx.__metadata.__deputyExecId}`, {
            ...ctx,
            __success: true,
          });
        });
      },
      async () => ({}),
      () => undefined,
      {
        nodeId: "node-restore-1",
        routineExecId: "routine-restore-1",
      },
    );

    expect(delegatedContexts).toEqual([
      expect.objectContaining({
        data: expect.objectContaining({
          uuid: "telemetry-restore-1",
        }),
        onConflict: expect.objectContaining({
          target: ["uuid"],
        }),
        queryData: expect.objectContaining({
          data: expect.objectContaining({
            uuid: "telemetry-restore-1",
          }),
          onConflict: expect.objectContaining({
            target: ["uuid"],
          }),
        }),
      }),
    ]);
  });

  it("prefers resolver query data over stale root query data for delegated inserts", async () => {
    const delegatedContexts: Array<Record<string, unknown>> = [];

    const insertTask = Cadenza.createDatabaseInsertTask("service_instance", "CadenzaDB", {
      onConflict: {
        target: ["uuid"],
        action: {
          do: "update",
        },
      },
    }) as any;

    const graphContext = new GraphContext({
      data: {
        uuid: "service-instance-1",
        process_pid: 321,
        service_name: "OrdersService",
      },
      onConflict: {
        target: ["name"],
        action: {
          do: "nothing",
        },
      },
      queryData: {
        data: {
          name: "OrdersService",
        },
        onConflict: {
          target: ["name"],
          action: {
            do: "nothing",
          },
        },
      },
      __resolverQueryData: {
        data: {
          uuid: "service-instance-1",
          process_pid: 321,
          service_name: "OrdersService",
        },
        onConflict: {
          target: ["uuid"],
          action: {
            do: "update",
          },
        },
      },
      __resolverRequestId: "resolver-service-instance-1",
      __remoteRoutineName: "Insert service_instance",
      __serviceName: "CadenzaDB",
      __localTaskName: "Insert service_instance in CadenzaDB",
    });

    const result = await insertTask.execute(
      graphContext,
      (signal: string, ctx: Record<string, unknown>) => {
        if (signal !== "meta.deputy.delegation_requested") {
          return;
        }

        delegatedContexts.push({
          __serviceName: ctx.__serviceName,
          __remoteRoutineName: ctx.__remoteRoutineName,
          __localTaskName: ctx.__localTaskName,
          __delegationRequestContext: ctx.__delegationRequestContext,
          data: ctx.data,
          onConflict: ctx.onConflict,
          queryData: ctx.queryData,
        });

        queueMicrotask(() => {
          Cadenza.emit(`meta.fetch.delegated:${ctx.__metadata.__deputyExecId}`, {
            ...ctx,
            __success: true,
          });
        });
      },
      async () => ({}),
      () => undefined,
      {
        nodeId: "node-resolver-priority-1",
        routineExecId: "routine-resolver-priority-1",
      },
    );

    expect(delegatedContexts).toEqual([
      expect.objectContaining({
        __serviceName: "CadenzaDB",
        __remoteRoutineName: "Insert service_instance",
        __localTaskName: "Insert service_instance in CadenzaDB",
        __delegationRequestContext: expect.objectContaining({
          __serviceName: "CadenzaDB",
          __remoteRoutineName: "Insert service_instance",
        }),
        data: expect.objectContaining({
          uuid: "service-instance-1",
          process_pid: 321,
          service_name: "OrdersService",
        }),
        onConflict: expect.objectContaining({
          target: ["uuid"],
        }),
        queryData: expect.objectContaining({
          data: expect.objectContaining({
            uuid: "service-instance-1",
            process_pid: 321,
            service_name: "OrdersService",
          }),
          onConflict: expect.objectContaining({
            target: ["uuid"],
          }),
        }),
      }),
    ]);
    expect(result).toMatchObject({
      __resolverRequestId: "resolver-service-instance-1",
      __resolverQueryData: expect.objectContaining({
        data: expect.objectContaining({
          uuid: "service-instance-1",
          process_pid: 321,
          service_name: "OrdersService",
        }),
      }),
    });
  });

  it("preserves resolver-owned insert payloads even when a stale delegation snapshot is present", async () => {
    const delegatedContexts: Array<Record<string, unknown>> = [];

    const insertTask = Cadenza.createDatabaseInsertTask("service_instance", "CadenzaDB", {
      onConflict: {
        target: ["uuid"],
        action: {
          do: "update",
        },
      },
    }) as any;

    const graphContext = new GraphContext({
      __status: "success",
      __success: true,
      data: {
        uuid: "service-instance-2",
        process_pid: 654,
        service_name: "OrdersService",
        is_active: true,
      },
      __resolverQueryData: {
        data: {
          uuid: "service-instance-2",
          process_pid: 654,
          service_name: "OrdersService",
          is_active: true,
        },
        onConflict: {
          target: ["uuid"],
          action: {
            do: "update",
          },
        },
      },
      __resolverRequestId: "resolver-service-instance-2",
      __delegationRequestContext: {
        data: {
          name: "OrdersService",
          description: "Orders",
          display_name: "Orders",
          is_meta: false,
        },
        queryData: {
          data: {
            name: "OrdersService",
            description: "Orders",
            display_name: "Orders",
            is_meta: false,
          },
          onConflict: {
            target: ["name"],
            action: {
              do: "nothing",
            },
          },
        },
        onConflict: {
          target: ["name"],
          action: {
            do: "nothing",
          },
        },
      },
      __remoteRoutineName: "Insert service_instance",
      __serviceName: "CadenzaDB",
      __localTaskName: "Insert service_instance in CadenzaDB",
    });

    await insertTask.execute(
      graphContext,
      (signal: string, ctx: Record<string, unknown>) => {
        if (signal !== "meta.deputy.delegation_requested") {
          return;
        }

        delegatedContexts.push({
          __serviceName: ctx.__serviceName,
          __remoteRoutineName: ctx.__remoteRoutineName,
          __localTaskName: ctx.__localTaskName,
          __delegationRequestContext: ctx.__delegationRequestContext,
          data: ctx.data,
          onConflict: ctx.onConflict,
          queryData: ctx.queryData,
        });

        queueMicrotask(() => {
          Cadenza.emit(`meta.fetch.delegated:${ctx.__metadata.__deputyExecId}`, {
            ...ctx,
            __success: true,
          });
        });
      },
      async () => ({}),
      () => undefined,
      {
        nodeId: "node-resolver-snapshot-2",
        routineExecId: "routine-resolver-snapshot-2",
      },
    );

    expect(delegatedContexts).toEqual([
      expect.objectContaining({
        __serviceName: "CadenzaDB",
        __remoteRoutineName: "Insert service_instance",
        __localTaskName: "Insert service_instance in CadenzaDB",
        __delegationRequestContext: expect.objectContaining({
          __serviceName: "CadenzaDB",
          __remoteRoutineName: "Insert service_instance",
        }),
        data: expect.objectContaining({
          uuid: "service-instance-2",
          process_pid: 654,
          service_name: "OrdersService",
          is_active: true,
        }),
        onConflict: expect.objectContaining({
          target: ["uuid"],
        }),
        queryData: expect.objectContaining({
          data: expect.objectContaining({
            uuid: "service-instance-2",
            process_pid: 654,
            service_name: "OrdersService",
            is_active: true,
          }),
          onConflict: expect.objectContaining({
            target: ["uuid"],
          }),
        }),
      }),
    ]);
    expect(delegatedContexts[0].data).not.toMatchObject({
      name: "OrdersService",
      description: "Orders",
      display_name: "Orders",
      is_meta: false,
    });
  });

  it("preserves child database payloads when a parent delegation snapshot is stale", async () => {
    const delegatedContexts: Array<Record<string, unknown>> = [];

    const insertTask = Cadenza.createDatabaseInsertTask(
      "signal_emission",
      "CadenzaDB",
    ) as any;

    const graphContext = new GraphContext({
      __status: "success",
      __success: true,
      __remoteRoutineName: "Insert execution_trace",
      __localTaskName: "Insert execution_trace in CadenzaDB",
      __serviceName: "CadenzaDB",
      __delegationRequestContext: {
        __remoteRoutineName: "Insert execution_trace",
        __localTaskName: "Insert execution_trace in CadenzaDB",
        data: {
          uuid: "trace-1",
          issuer_type: "service",
        },
        queryData: {
          data: {
            uuid: "trace-1",
            issuer_type: "service",
          },
        },
      },
      data: {
        uuid: "signal-1",
        signal_name: "runner.tick",
        execution_trace_id: "trace-1",
      },
      queryData: {
        data: {
          uuid: "signal-1",
          signal_name: "runner.tick",
          execution_trace_id: "trace-1",
        },
      },
    });

    await insertTask.execute(
      graphContext,
      (signal: string, ctx: Record<string, unknown>) => {
        if (signal !== "meta.deputy.delegation_requested") {
          return;
        }

        delegatedContexts.push({
          __serviceName: ctx.__serviceName,
          __remoteRoutineName: ctx.__remoteRoutineName,
          __localTaskName: ctx.__localTaskName,
          data: ctx.data,
          queryData: ctx.queryData,
          __delegationRequestContext: ctx.__delegationRequestContext,
        });

        queueMicrotask(() => {
          Cadenza.emit(`meta.fetch.delegated:${ctx.__metadata.__deputyExecId}`, {
            ...ctx,
            __success: true,
          });
        });
      },
      async () => ({}),
      () => undefined,
      {
        nodeId: "node-stale-parent-snapshot",
        routineExecId: "routine-stale-parent-snapshot",
      },
    );

    expect(delegatedContexts).toEqual([
      expect.objectContaining({
        __serviceName: "CadenzaDB",
        __remoteRoutineName: "Insert signal_emission",
        __localTaskName: "Insert signal_emission in CadenzaDB",
        data: expect.objectContaining({
          uuid: "signal-1",
          signal_name: "runner.tick",
          execution_trace_id: "trace-1",
        }),
        queryData: expect.objectContaining({
          data: expect.objectContaining({
            uuid: "signal-1",
            signal_name: "runner.tick",
            execution_trace_id: "trace-1",
          }),
        }),
        __delegationRequestContext: expect.objectContaining({
          __remoteRoutineName: "Insert signal_emission",
          __localTaskName: "Insert signal_emission in CadenzaDB",
        }),
      }),
    ]);
    expect(delegatedContexts[0].data).not.toMatchObject({
      issuer_type: "service",
    });
  });

  it("uses hardened deputy and database proxy defaults", () => {
    const deputyTask = Cadenza.createDeputyTask("Query MetricsDB", "MetricsDB");
    const databaseTask = Cadenza.createDatabaseInsertTask("telemetry", "MetricsDB");

    expect(deputyTask.concurrency).toBe(50);
    expect(deputyTask.timeout).toBe(120_000);
    expect(databaseTask.concurrency).toBe(50);
    expect(databaseTask.timeout).toBe(120_000);
  });

  it("propagates database deputy timeout overrides into the delegated context", async () => {
    const delegatedContexts: Array<Record<string, unknown>> = [];

    const insertTask = Cadenza.createCadenzaDBInsertTask(
      "routine_execution",
      {},
      {
        isSubMeta: true,
        timeout: 120_000,
      },
    ) as any;

    const graphContext = new GraphContext({
      data: {
        uuid: "routine-1",
        name: "Compute traffic tick plan",
        execution_trace_id: "trace-1",
      },
      queryData: {
        data: {
          uuid: "routine-1",
          name: "Compute traffic tick plan",
          execution_trace_id: "trace-1",
        },
      },
      __metadata: {
        __executionTraceId: "trace-1",
      },
    });

    await insertTask.execute(
      graphContext,
      (signal: string, ctx: Record<string, unknown>) => {
        if (signal !== "meta.deputy.delegation_requested") {
          return;
        }

        delegatedContexts.push(ctx);

        queueMicrotask(() => {
          Cadenza.emit(`meta.fetch.delegated:${ctx.__metadata.__deputyExecId}`, {
            ...ctx,
            __success: true,
          });
        });
      },
      async () => ({}),
      () => undefined,
      {
        nodeId: "node-timeout-propagation",
        routineExecId: "routine-timeout-propagation",
      },
    );

    expect(delegatedContexts).toHaveLength(1);
    expect(delegatedContexts[0].__timeout).toBe(120_000);
    expect(
      (delegatedContexts[0].__metadata as Record<string, unknown>)
        ?.__executionTraceId,
    ).toBe("trace-1");
  });

  it("caps generated insert and upsert tasks with shared write-task throttles", () => {
    const controller = DatabaseController.instance;
    const registration = controller.createPostgresActor(
      "MetricsDB",
      schema,
      "Metrics database actor",
      {
        databaseName: "metrics_db",
        ownerServiceName: "MetricsService",
      },
    );

    expect(() =>
      (controller as unknown as {
        generateDatabaseTasks: (value: unknown) => void;
      }).generateDatabaseTasks(registration),
    ).not.toThrow();

    const insertTask = Cadenza.get("Insert telemetry");
    const upsertTask = Cadenza.get("UPSERT telemetry");

    expect(insertTask).toBeDefined();
    expect(upsertTask).toBeDefined();
    expect(insertTask?.concurrency).toBe(200);
    expect(insertTask?.timeout).toBe(120_000);
    expect(insertTask?.getTag({} as any)).toBe(
      "insert:metrics-db-postgres-actor:telemetry",
    );
    expect(upsertTask?.concurrency).toBe(200);
    expect(upsertTask?.timeout).toBe(120_000);
    expect(upsertTask?.getTag({} as any)).toBe(
      "upsert:metrics-db-postgres-actor:telemetry",
    );
  });

  it("allows one inquiry intent to fan out across multiple generated table tasks", () => {
    const controller = DatabaseController.instance;
    const syncSchema: DatabaseSchemaDefinition = {
      version: 1,
      tables: {
        service_instance: {
          fields: {
            uuid: {
              type: "uuid",
              primary: true,
              required: true,
            },
          },
          customIntents: {
            query: [
              {
                intent: "meta-service-registry-full-sync",
                description: "Collect service registry sync data.",
                input: {
                  type: "object",
                  properties: {
                    syncScope: {
                      type: "string",
                    },
                  },
                },
              },
            ],
          },
        },
        service_instance_transport: {
          fields: {
            uuid: {
              type: "uuid",
              primary: true,
              required: true,
            },
          },
          customIntents: {
            query: [
              {
                intent: "meta-service-registry-full-sync",
                description: "Collect service registry sync data.",
                input: {
                  type: "object",
                  properties: {
                    syncScope: {
                      type: "string",
                    },
                  },
                },
              },
            ],
          },
        },
      },
    };

    const registration = controller.createPostgresActor(
      "RegistryDB",
      syncSchema,
      "Registry actor",
      {
        databaseName: "registry_db",
        ownerServiceName: "CadenzaDB",
      },
    );

    expect(() =>
      (controller as unknown as {
        generateDatabaseTasks: (value: unknown) => void;
      }).generateDatabaseTasks(registration),
    ).not.toThrow();

    const observer = Cadenza.inquiryBroker.inquiryObservers.get(
      "meta-service-registry-full-sync",
    );

    expect(observer).toBeDefined();
    expect(observer?.tasks.size).toBe(2);
  });
});
