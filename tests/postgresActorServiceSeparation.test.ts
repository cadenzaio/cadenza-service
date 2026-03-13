import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

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
