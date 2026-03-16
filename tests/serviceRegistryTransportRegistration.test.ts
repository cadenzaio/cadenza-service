import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import fs from "node:fs";

import Cadenza from "../src/Cadenza";
import DatabaseController from "@service-database-controller";
import GraphMetadataController from "../src/graph/controllers/GraphMetadataController";
import GraphSyncController from "../src/graph/controllers/GraphSyncController";
import RestController from "../src/network/RestController";
import ServiceRegistry from "../src/registry/ServiceRegistry";
import SignalController from "../src/signals/SignalController";
import SocketController from "../src/network/SocketController";

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
    // Ignore first-run resets before bootstrap.
  }

  (DatabaseController as any)._instance = undefined;
  (GraphMetadataController as any)._instance = undefined;
  (GraphSyncController as any)._instance = undefined;
  (RestController as any)._instance = undefined;
  (ServiceRegistry as any)._instance = undefined;
  (SignalController as any)._instance = undefined;
  (SocketController as any)._instance = undefined;
}

describe("service registry transport registration", () => {
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

  it("keeps declared transports available after service instance setup", async () => {
    const registry = ServiceRegistry.instance as any;
    const setupServiceTask = Cadenza.get("Setup service");

    expect(setupServiceTask).toBeDefined();

    Cadenza.run(setupServiceTask!, {
      serviceInstance: {
        uuid: "orders-1",
        serviceName: "OrdersService",
        isFrontend: false,
        isDatabase: false,
      },
      __transportData: [
        {
          uuid: "transport-1",
          service_instance_id: "orders-1",
          role: "public",
          origin: "http://orders.localhost",
          protocols: ["rest", "socket"],
        },
      ],
      __useSocket: true,
      __retryCount: 3,
      __isFrontend: false,
    });

    await waitForCondition(
      () => registry.instances.get("OrdersService")?.[0]?.transports?.length === 1,
      1_500,
    );

    expect(registry.instances.get("OrdersService")?.[0]).toMatchObject({
      uuid: "orders-1",
      serviceName: "OrdersService",
      transports: [
        {
          uuid: "transport-1",
          origin: "http://orders.localhost",
          role: "public",
        },
      ],
    });
  });

  it("emits transport registration payloads from metadata-backed transport data", async () => {
    const registrations: Array<Record<string, unknown>> = [];

    Cadenza.createMetaTask("Capture transport registration request", (ctx) => {
      registrations.push(ctx.data);
      return true;
    }).doOn("meta.service_registry.transport_registration_requested");

    const prepareTransportsTask = Cadenza.get("Prepare service transport inserts");
    expect(prepareTransportsTask).toBeDefined();

    Cadenza.run(prepareTransportsTask!, {
      __serviceInstanceId: "orders-1",
      __transportData: [
        {
          uuid: "transport-1",
          role: "internal",
          origin: "http://orders.internal",
          protocols: ["rest"],
        },
        {
          uuid: "transport-2",
          role: "public",
          origin: "http://orders.localhost",
          protocols: ["rest", "socket"],
        },
      ],
    });

    await waitForCondition(() => registrations.length === 2, 1_500);

    expect(registrations).toEqual([
      expect.objectContaining({
        uuid: "transport-1",
        role: "internal",
        service_instance_id: "orders-1",
      }),
      expect.objectContaining({
        uuid: "transport-2",
        role: "public",
        service_instance_id: "orders-1",
      }),
    ]);
  });

  it("preserves chained transport metadata through setup service", async () => {
    const registrations: Array<Record<string, unknown>> = [];

    Cadenza.createMetaTask("Capture chained transport registration request", (ctx) => {
      registrations.push(ctx.data);
      return true;
    }).doOn("meta.service_registry.transport_registration_requested");

    const setupServiceTask = Cadenza.get("Setup service") as any;
    const prepareTransportsTask = Cadenza.get("Prepare service transport inserts");

    expect(setupServiceTask).toBeDefined();
    expect(prepareTransportsTask).toBeDefined();

    const setupResult = setupServiceTask.taskFunction({
      serviceInstance: {
        uuid: "orders-2",
        serviceName: "OrdersService",
        isFrontend: false,
        isDatabase: false,
      },
      data: {
        uuid: "orders-2",
        service_name: "OrdersService",
      },
      __transportData: [
        {
          uuid: "transport-3",
          role: "public",
          origin: "http://orders-2.localhost",
          protocols: ["rest", "socket"],
        },
      ],
      __useSocket: true,
      __retryCount: 3,
      __isFrontend: false,
    });

    Cadenza.run(prepareTransportsTask!, setupResult);

    await waitForCondition(() => registrations.length === 1, 1_500);

    expect(registrations[0]).toEqual(
      expect.objectContaining({
        uuid: "transport-3",
        role: "public",
        origin: "http://orders-2.localhost",
        service_instance_id: "orders-2",
      }),
    );
  });

  it("resolves the local service instance insert task at execution time", async () => {
    const registry = ServiceRegistry.instance as any;
    const capturedQueryData: Array<Record<string, unknown>> = [];

    Cadenza.createMetaTask("Insert service_instance", (ctx) => {
      capturedQueryData.push(
        (ctx.queryData as Record<string, unknown>) ?? {},
      );

      return {
        ...ctx,
        uuid:
          (ctx.queryData as any)?.data?.uuid ??
          (ctx.data as any)?.uuid ??
          "missing-uuid",
      };
    });

    const result = await registry.insertServiceInstanceTask.taskFunction(
      {
        data: {
          uuid: "orders-3",
          process_pid: 1,
          service_name: "OrdersService",
          is_active: true,
        },
      },
      Cadenza.emit.bind(Cadenza),
    );

    expect(capturedQueryData).toEqual([
      expect.objectContaining({
        data: expect.objectContaining({
          uuid: "orders-3",
          process_pid: 1,
          service_name: "OrdersService",
        }),
      }),
    ]);
    expect(result).toMatchObject({
      uuid: "orders-3",
    });
  });

  it("routes remote service inserts through signal-driven runner flow", async () => {
    resetRuntimeState();

    const capturedQueryData: Array<Record<string, unknown>> = [];

    vi.spyOn(Cadenza, "createCadenzaDBInsertTask").mockImplementation(
      () =>
        Cadenza.createMetaTask(
          "Mock remote service insert",
          (ctx) => {
            capturedQueryData.push(
              (ctx.queryData as Record<string, unknown>) ?? {},
            );

            return {
              ...ctx,
              __serviceName: "OrdersService",
            };
          },
          "Captures remote service insert payloads for resolver tests.",
          {
            register: false,
            isHidden: true,
          },
        ),
    );

    Cadenza.bootstrap();
    Cadenza.setMode("production");

    const registry = ServiceRegistry.instance as any;
    const result = await registry.insertServiceTask.taskFunction(
      {
        data: {
          name: "OrdersService",
          description: "Orders",
        },
        __serviceName: "OrdersService",
      },
      Cadenza.emit.bind(Cadenza),
    );

    expect(capturedQueryData).toEqual([
      expect.objectContaining({
        data: expect.objectContaining({
          name: "OrdersService",
          description: "Orders",
        }),
      }),
    ]);
    expect(result).toMatchObject({
      __serviceName: "OrdersService",
    });
  });

  it("preserves local instance identity when remote service inserts return authority metadata", async () => {
    resetRuntimeState();

    vi.spyOn(Cadenza, "createCadenzaDBInsertTask").mockImplementation(
      () =>
        Cadenza.createMetaTask(
          "Mock remote authority insert",
          (ctx) => ({
            ...ctx,
            __serviceName: "CadenzaDB",
            __serviceInstanceId: "authority-1",
          }),
          "Returns authority metadata for identity preservation tests.",
          {
            register: false,
            isHidden: true,
          },
        ),
    );

    Cadenza.bootstrap();
    Cadenza.setMode("production");

    const registry = ServiceRegistry.instance as any;
    const result = await registry.insertServiceTask.taskFunction(
      {
        data: {
          name: "OrdersService",
          description: "Orders",
        },
        __serviceName: "OrdersService",
        __serviceInstanceId: "orders-4",
      },
      Cadenza.emit.bind(Cadenza),
    );

    expect(result).toMatchObject({
      __serviceName: "OrdersService",
      __serviceInstanceId: "orders-4",
      queryData: expect.objectContaining({
        data: expect.objectContaining({
          name: "OrdersService",
        }),
      }),
    });
  });

  it("routes frontend targets through browser socket fetch ids without transports", async () => {
    const registry = ServiceRegistry.instance as any;
    const selectedContexts: Array<Record<string, unknown>> = [];

    Cadenza.createMetaTask("Capture frontend socket selection", (ctx) => {
      selectedContexts.push(ctx);
      return true;
    }).doOn("meta.service_registry.selected_instance_for_socket:browser:frontend-1");

    registry.instances.set("DashboardFrontend", [
      {
        uuid: "frontend-1",
        serviceName: "DashboardFrontend",
        numberOfRunningGraphs: 0,
        isPrimary: false,
        isActive: true,
        isNonResponsive: false,
        isBlocked: false,
        runtimeState: "healthy",
        acceptingWork: true,
        reportedAt: new Date().toISOString(),
        health: {},
        isFrontend: true,
        isDatabase: false,
        transports: [],
        clientCreatedTransportIds: [],
      },
    ]);

    Cadenza.run(registry.getBalancedInstance, {
      __serviceName: "DashboardFrontend",
      __remoteRoutineName: "HandleFrontendDelegation",
      __metadata: {
        __deputyExecId: "frontend-routing-1",
      },
    });

    await waitForCondition(() => selectedContexts.length === 1, 1_500);

    expect(selectedContexts[0]).toMatchObject({
      __instance: "frontend-1",
      __fetchId: "browser:frontend-1",
      __transportProtocols: ["socket"],
    });
    expect(selectedContexts[0].__transportId).toBeUndefined();
  });

  it("does not create server transport clients for frontend instances", async () => {
    const registry = ServiceRegistry.instance as any;
    const dependeeRegistrations: Array<Record<string, unknown>> = [];

    Cadenza.createMetaTask("Capture dependee registration", (ctx) => {
      dependeeRegistrations.push(ctx.data);
      return true;
    }).doOn("meta.service_registry.dependee_registered");

    registry.remoteSignals.set("DashboardFrontend", new Set(["signal.test"]));
    registry.instances.set("DashboardFrontend", [
      {
        uuid: "frontend-2",
        serviceName: "DashboardFrontend",
        numberOfRunningGraphs: 0,
        isPrimary: false,
        isActive: true,
        isNonResponsive: false,
        isBlocked: false,
        runtimeState: "healthy",
        acceptingWork: true,
        reportedAt: new Date().toISOString(),
        health: {},
        isFrontend: true,
        isDatabase: false,
        transports: [],
        clientCreatedTransportIds: [],
      },
    ]);

    Cadenza.run(registry.handleTransportUpdateTask, {
      serviceTransport: {
        uuid: "frontend-transport-1",
        serviceInstanceId: "frontend-2",
        role: "internal",
        origin: "http://frontend.internal",
        protocols: ["socket"],
      },
    });

    await new Promise((resolve) => setTimeout(resolve, 25));

    expect(dependeeRegistrations).toEqual([]);
    expect(
      registry.instances.get("DashboardFrontend")?.[0]?.clientCreatedTransportIds,
    ).toEqual([]);
  });

  it("does not call task.execute directly from resolver wrappers", () => {
    const serviceRegistrySource = fs.readFileSync(
      "/Users/emilforsvall/WebstormProjects/cadenza-workspace/cadenza-service/src/registry/ServiceRegistry.ts",
      "utf8",
    );
    const graphSyncSource = fs.readFileSync(
      "/Users/emilforsvall/WebstormProjects/cadenza-workspace/cadenza-service/src/graph/controllers/GraphSyncController.ts",
      "utf8",
    );

    expect(serviceRegistrySource).not.toMatch(/targetTask\.execute\s*\(/);
    expect(graphSyncSource).not.toMatch(/targetTask\.execute\s*\(/);
  });

});
