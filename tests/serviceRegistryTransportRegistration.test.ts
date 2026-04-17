import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import fs from "node:fs";
import http from "node:http";

import Cadenza from "../src/Cadenza";
import DatabaseController from "@service-database-controller";
import GraphMetadataController from "../src/graph/controllers/GraphMetadataController";
import GraphSyncController from "../src/graph/controllers/GraphSyncController";
import RestController from "../src/network/RestController";
import ServiceRegistry from "../src/registry/ServiceRegistry";
import SignalController from "../src/signals/SignalController";
import SocketController from "../src/network/SocketController";
import { AUTHORITY_RUNTIME_STATUS_REPORT_INTENT } from "../src/registry/runtimeStatusContract";
import {
  AUTHORITY_SERVICE_INSTANCE_REGISTER_INTENT,
  AUTHORITY_SERVICE_INSTANCE_TRANSPORT_REGISTER_INTENT,
} from "../src/registry/authorityBootstrapControlPlane";
import { buildTransportHandleKey as buildHandleKey } from "../src/utils/transport";
import {
  buildServiceCommunicationEstablishedContext,
  META_SERVICE_COMMUNICATION_PERSIST_RETRY_SIGNAL,
} from "../src/utils/serviceCommunication";
import {
  AUTHORITY_SERVICE_MANIFEST_REPORT_INTENT,
  AUTHORITY_SERVICE_MANIFEST_UPDATED_SIGNAL,
} from "../src/registry/serviceManifestContract";
import { buildServiceManifestSnapshot } from "../src/registry/serviceManifest";
import { EXECUTION_PERSISTENCE_BUNDLE_SIGNAL } from "../src/execution/ExecutionPersistenceCoordinator";

function countImmediateStartupManifestRequests(spy: ReturnType<typeof vi.spyOn>): number {
  return spy.mock.calls.filter(
    (call) =>
      call[0] === "service_setup_completed" &&
      call[1] === true &&
      call[2] === "local_meta_structural",
  ).length;
}

function buildRouteKey(
  serviceName: string,
  role: "internal" | "public",
  origin: string,
) {
  return `${serviceName}|${role}|${origin}`;
}

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
  delete (globalThis as any).__CADENZA_RUNTIME__;
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
    vi.useRealTimers();
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

  it("replays declared startup transports through the bootstrap ensure path", async () => {
    const registrations: Array<Record<string, unknown>> = [];

    Cadenza.createMetaTask("Capture ensured transport registration request", (ctx) => {
      registrations.push(ctx.data);
      return true;
    }).doOn("meta.service_registry.transport_registration_requested");

    Cadenza.emit("meta.service_registry.transport_registration_ensure_requested", {
      __serviceName: "PredictorService",
      __serviceInstanceId: "predictor-b-1",
      __transportData: [
        {
          uuid: "transport-1",
          role: "internal",
          origin: "http://predictor-b:3005",
          protocols: ["rest"],
        },
        {
          uuid: "transport-2",
          role: "public",
          origin: "http://predictor-b.localhost",
          protocols: ["rest", "socket"],
        },
      ],
    });

    await waitForCondition(() => registrations.length === 2, 1_500);

    expect(registrations).toEqual([
      expect.objectContaining({
        uuid: "transport-1",
        role: "internal",
        service_instance_id: "predictor-b-1",
      }),
      expect.objectContaining({
        uuid: "transport-2",
        role: "public",
        service_instance_id: "predictor-b-1",
      }),
    ]);
  });

  it("only handles service setup completion for the local inserted instance", async () => {
    Cadenza.createCadenzaService("OrdersService", "Orders service", {
      port: 3010,
      cadenzaDB: {
        connect: true,
        address: "cadenza-db-service",
        port: 8080,
      },
    });

    const registry = ServiceRegistry.instance;
    const localInstanceId = registry.serviceInstanceId;

    const bootstrapFullSyncSpy = vi
      .spyOn(registry, "bootstrapFullSync")
      .mockImplementation(() => undefined);
    const requestServiceManifestPublicationSpy = vi
      .spyOn(Cadenza as any, "requestServiceManifestPublication")
      .mockImplementation(() => undefined);

    bootstrapFullSyncSpy.mockClear();
    requestServiceManifestPublicationSpy.mockClear();

    Cadenza.emit("meta.service_registry.instance_inserted", {
      serviceInstance: {
        uuid: "remote-telemetry-1",
        serviceName: "TelemetryCollectorService",
      },
      serviceInstanceId: "remote-telemetry-1",
      serviceName: "TelemetryCollectorService",
    });

    Cadenza.emit("meta.service_registry.instance_inserted", {
      serviceName: "PredictorService",
    });

    Cadenza.emit("meta.service_registry.instance_inserted", {});

    await new Promise((resolve) => setTimeout(resolve, 25));

    expect(bootstrapFullSyncSpy).not.toHaveBeenCalled();
    expect(countImmediateStartupManifestRequests(requestServiceManifestPublicationSpy)).toBe(0);

    Cadenza.emit("meta.service_registry.instance_inserted", {
      serviceInstance: {
        uuid: localInstanceId,
        serviceName: "OrdersService",
      },
      serviceInstanceId: localInstanceId,
      serviceName: "OrdersService",
    });

    await waitForCondition(() => bootstrapFullSyncSpy.mock.calls.length === 1, 500);

    expect(bootstrapFullSyncSpy).toHaveBeenCalledTimes(1);
    expect(countImmediateStartupManifestRequests(requestServiceManifestPublicationSpy)).toBe(0);

    Cadenza.emit("meta.service_registry.service_inserted", {
      data: {
        name: "OrdersService",
      },
      __serviceName: "OrdersService",
      __serviceInstanceId: localInstanceId,
    });

    await waitForCondition(
      () => countImmediateStartupManifestRequests(requestServiceManifestPublicationSpy) === 1,
      500,
    );

    expect(countImmediateStartupManifestRequests(requestServiceManifestPublicationSpy)).toBe(1);

    Cadenza.emit("meta.service_registry.instance_inserted", {
      serviceInstance: {
        uuid: localInstanceId,
        serviceName: "OrdersService",
      },
      serviceInstanceId: localInstanceId,
      serviceName: "OrdersService",
    });

    await new Promise((resolve) => setTimeout(resolve, 25));

    expect(bootstrapFullSyncSpy).toHaveBeenCalledTimes(1);
    expect(countImmediateStartupManifestRequests(requestServiceManifestPublicationSpy)).toBe(1);
  });

  it("waits for the local instance insert when the service row is acknowledged first", async () => {
    Cadenza.createCadenzaService("OrdersService", "Orders service", {
      port: 3010,
      cadenzaDB: {
        connect: true,
        address: "cadenza-db-service",
        port: 8080,
      },
    });

    const registry = ServiceRegistry.instance;
    const localInstanceId = registry.serviceInstanceId;

    const bootstrapFullSyncSpy = vi
      .spyOn(registry, "bootstrapFullSync")
      .mockImplementation(() => undefined);
    const requestServiceManifestPublicationSpy = vi
      .spyOn(Cadenza as any, "requestServiceManifestPublication")
      .mockImplementation(() => undefined);

    bootstrapFullSyncSpy.mockClear();
    requestServiceManifestPublicationSpy.mockClear();

    Cadenza.emit("meta.service_registry.service_inserted", {
      data: {
        name: "OrdersService",
      },
      __serviceName: "OrdersService",
      __serviceInstanceId: localInstanceId,
    });

    await new Promise((resolve) => setTimeout(resolve, 25));

    expect(bootstrapFullSyncSpy).not.toHaveBeenCalled();
    expect(countImmediateStartupManifestRequests(requestServiceManifestPublicationSpy)).toBe(0);

    Cadenza.emit("meta.service_registry.instance_inserted", {
      serviceInstance: {
        uuid: localInstanceId,
        serviceName: "OrdersService",
      },
      serviceInstanceId: localInstanceId,
      serviceName: "OrdersService",
    });

    await waitForCondition(() => bootstrapFullSyncSpy.mock.calls.length === 1, 500);
    await waitForCondition(
      () => countImmediateStartupManifestRequests(requestServiceManifestPublicationSpy) === 1,
      500,
    );

    expect(bootstrapFullSyncSpy).toHaveBeenCalledTimes(1);
    expect(countImmediateStartupManifestRequests(requestServiceManifestPublicationSpy)).toBe(1);
  });

  it("falls back to requesting the initial manifest publication when the local service insert signal never arrives", async () => {
    Cadenza.createCadenzaService("OrdersService", "Orders service", {
      port: 3010,
      cadenzaDB: {
        connect: true,
        address: "cadenza-db-service",
        port: 8080,
      },
    });

    const registry = ServiceRegistry.instance;
    const localInstanceId = registry.serviceInstanceId;

    const bootstrapFullSyncSpy = vi
      .spyOn(registry, "bootstrapFullSync")
      .mockImplementation(() => undefined);
    const requestServiceManifestPublicationSpy = vi
      .spyOn(Cadenza as any, "requestServiceManifestPublication")
      .mockImplementation(() => undefined);

    bootstrapFullSyncSpy.mockClear();
    requestServiceManifestPublicationSpy.mockClear();

    Cadenza.emit("meta.service_registry.instance_inserted", {
      serviceInstance: {
        uuid: localInstanceId,
        serviceName: "OrdersService",
      },
      serviceInstanceId: localInstanceId,
      serviceName: "OrdersService",
    });

    await waitForCondition(() => bootstrapFullSyncSpy.mock.calls.length === 1, 500);
    await waitForCondition(
      () => countImmediateStartupManifestRequests(requestServiceManifestPublicationSpy) === 1,
      2_000,
    );

    expect(bootstrapFullSyncSpy).toHaveBeenCalledTimes(1);
    expect(countImmediateStartupManifestRequests(requestServiceManifestPublicationSpy)).toBe(1);
  });

  it("does not treat transport handshake lifecycle signals as manifest publication triggers", () => {
    expect(
      (Cadenza as any).shouldRequestServiceManifestPublicationForSignal({
        signal: "meta.fetch.handshake_complete",
      }),
    ).toBe(false);
    expect(
      (Cadenza as any).shouldRequestServiceManifestPublicationForSignal({
        signal: "meta.fetch.handshake_failed",
      }),
    ).toBe(false);
  });

  it("does not request manifest publication for meta-only structural changes", () => {
    const businessTask = Cadenza.createTask(
      "Business structural test task",
      () => true,
    ).respondsTo("business-structural-test-intent");
    const metaTask = Cadenza.createMetaTask(
      "Meta-only structural test task",
      () => true,
      "",
      {
        register: true,
      },
    ).respondsTo("meta-only-structural-test-intent");

    Cadenza.createMetaHelper(
      "metaOnlyStructuralTestHelper",
      () => true,
      "Meta-only helper",
    );
    Cadenza.createMetaGlobal(
      "metaOnlyStructuralTestGlobal",
      { enabled: true },
      "Meta-only global",
    );

    expect(
      (Cadenza as any).shouldRequestServiceManifestPublicationForSignal({
        signal: "meta.task.intent_associated",
        taskInstance: businessTask,
        data: {
          intentName: "business-structural-test-intent",
          taskName: businessTask.name,
          taskVersion: businessTask.version,
        },
      }),
    ).toBe(true);

    expect(
      (Cadenza as any).shouldRequestServiceManifestPublicationForSignal({
        signal: "meta.task.intent_associated",
        taskInstance: metaTask,
        data: {
          intentName: "meta-only-structural-test-intent",
          taskName: metaTask.name,
          taskVersion: metaTask.version,
        },
      }),
    ).toBe(false);

    const routingCriticalMetaTask = Cadenza.createMetaTask(
      "Routing-critical meta structural test task",
      () => true,
      "",
      {
        register: true,
      },
    ).doOn(EXECUTION_PERSISTENCE_BUNDLE_SIGNAL);

    expect(
      (Cadenza as any).shouldRequestServiceManifestPublicationForSignal({
        signal: "meta.task.observed_signal",
        taskInstance: routingCriticalMetaTask,
        data: {
          signalName: EXECUTION_PERSISTENCE_BUNDLE_SIGNAL,
          taskName: routingCriticalMetaTask.name,
          taskVersion: routingCriticalMetaTask.version,
        },
      }),
    ).toBe(true);

    expect(
      (Cadenza as any).shouldRequestServiceManifestPublicationForSignal({
        signal: "meta.helper.created",
        data: {
          name: "metaOnlyStructuralTestHelper",
          isMeta: true,
        },
      }),
    ).toBe(false);

    expect(
      (Cadenza as any).shouldRequestServiceManifestPublicationForSignal({
        signal: "meta.global.created",
        data: {
          name: "metaOnlyStructuralTestGlobal",
          isMeta: true,
        },
      }),
    ).toBe(false);

    expect(
      (Cadenza as any).shouldRequestServiceManifestPublicationForSignal({
        signal: "meta.actor.created",
        data: {
          name: "MetaActor",
          is_meta: true,
        },
      }),
    ).toBe(false);

    expect(
      (Cadenza as any).shouldRequestServiceManifestPublicationForSignal({
        signal: "global.meta.graph_metadata.routine_created",
        data: {
          name: "MetaRoutine",
          isMeta: true,
        },
      }),
    ).toBe(false);

    expect(
      (Cadenza as any).shouldRequestServiceManifestPublicationForSignal({
        signal: "global.meta.service_instance.updated",
        serviceInstance: {
          uuid: "instance-1",
          serviceName: "OrdersService",
        },
      }),
    ).toBe(false);

    expect(
      (Cadenza as any).shouldRequestServiceManifestPublicationForSignal({
        signal: "global.meta.service_registry.transport_updated",
        data: {
          service_instance_id: "instance-1",
          origin: "http://orders.internal",
        },
      }),
    ).toBe(false);
  });

  it("keeps the local service instance identity when bootstrap authority insert metadata leaks responder ids", async () => {
    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "TelemetryCollectorService";
    registry.serviceInstanceId = "telemetry-local-1";
    registry.connectsToCadenzaDB = true;
    registry.seedAuthorityBootstrapRoute(
      "http://cadenza-db-service:8080",
      "internal",
    );
    registry.noteAuthorityBootstrapHandshake({
      serviceName: "CadenzaDB",
      serviceInstanceId: "cadenza-db-live-1",
      serviceTransportId: "cadenza-db-transport-1",
      serviceOrigin: "http://cadenza-db-service:8080",
    });

    vi.spyOn(Cadenza, "inquire").mockResolvedValue({
      __status: "success",
      serviceName: "CadenzaDB",
      serviceInstanceId: "cadenza-db-live-1",
      data: {
        uuid: "cadenza-db-live-1",
        service_name: "CadenzaDB",
      },
    } as any);

    Cadenza.emit("meta.service_registry.instance_registration_requested", {
      data: {
        uuid: "telemetry-local-1",
        process_pid: 1,
        service_name: "TelemetryCollectorService",
        is_active: true,
      },
      __registrationData: {
        uuid: "telemetry-local-1",
        process_pid: 1,
        service_name: "TelemetryCollectorService",
        is_active: true,
      },
      __serviceName: "TelemetryCollectorService",
      __serviceInstanceId: "telemetry-local-1",
    });

    await waitForCondition(() => registry.serviceInstanceId === "telemetry-local-1", 500);

    expect(registry.serviceInstanceId).toBe("telemetry-local-1");
  });

  it("ignores resolved remote service_instance inserts when seeding local service state", async () => {
    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "PredictorService";
    registry.serviceInstanceId = "predictor-local-1";
    registry.seedLocalInstance(
      {
        uuid: "predictor-local-1",
        serviceName: "PredictorService",
        isFrontend: false,
        isDatabase: false,
        isActive: true,
        isNonResponsive: false,
        isBlocked: false,
        transports: [
          {
            uuid: "predictor-internal-1",
            service_instance_id: "predictor-local-1",
            role: "internal",
            origin: "http://predictor:3005",
            protocols: ["rest"],
          },
        ],
      },
      { markTransportsReady: true },
    );

    const setupServiceTask = Cadenza.get("Setup service");
    expect(setupServiceTask).toBeDefined();

    Cadenza.run(setupServiceTask!, {
      serviceInstance: {
        uuid: "telemetry-remote-1",
        serviceName: "TelemetryCollectorService",
        isFrontend: false,
        isDatabase: false,
      },
      __transportData: [
        {
          uuid: "telemetry-internal-1",
          service_instance_id: "telemetry-remote-1",
          role: "internal",
          origin: "http://telemetry-collector:3003",
          protocols: ["rest"],
        },
      ],
      __useSocket: true,
      __retryCount: 3,
      __isFrontend: false,
    });

    await new Promise((resolve) => setTimeout(resolve, 25));

    expect(registry.serviceInstanceId).toBe("predictor-local-1");
    expect((registry as any).buildLocalRuntimeStatusReport("minimal")).toMatchObject({
      serviceName: "PredictorService",
      serviceInstanceId: "predictor-local-1",
      transportOrigin: "http://predictor:3005",
      isActive: true,
    });
    expect(registry.instances.get("TelemetryCollectorService")).toBeUndefined();
  });

  it("keeps local transport readiness state when synced instance updates arrive", async () => {
    const registry = ServiceRegistry.instance as any;

    registry.instances.set("CadenzaDB", [
      {
        uuid: "cadenza-db-1",
        serviceName: "CadenzaDB",
        isFrontend: false,
        isDatabase: true,
        isActive: true,
        isNonResponsive: false,
        isBlocked: false,
        numberOfRunningGraphs: 0,
        isPrimary: false,
        health: {},
        transports: [
          {
            uuid: "db-transport-1",
            serviceInstanceId: "cadenza-db-1",
            role: "internal",
            origin: "http://cadenza-db-service:8080",
            protocols: ["rest", "socket"],
          },
        ],
        clientPendingTransportIds: ["db-transport-1"],
        clientReadyTransportIds: ["db-transport-1"],
      },
    ]);

    Cadenza.emit("global.meta.service_instance.updated", {
      serviceInstance: {
        uuid: "cadenza-db-1",
        service_name: "CadenzaDB",
        is_active: true,
        is_non_responsive: false,
        is_blocked: false,
        is_database: true,
        transports: [
          {
            uuid: "db-transport-1",
            service_instance_id: "cadenza-db-1",
            role: "internal",
            origin: "http://cadenza-db-service:8080",
            protocols: ["rest", "socket"],
          },
        ],
        clientPendingTransportIds: ["remote-pending"],
        clientReadyTransportIds: ["remote-ready"],
      },
    });

    await waitForCondition(
      () =>
        registry.instances.get("CadenzaDB")?.[0]?.clientReadyTransportIds?.includes(
          "db-transport-1",
        ) === true,
      1_500,
    );

    expect(registry.instances.get("CadenzaDB")?.[0]).toMatchObject({
      clientPendingTransportIds: ["db-transport-1"],
      clientReadyTransportIds: ["db-transport-1"],
    });
  });

  it("ignores foreign same-service instance updates from authority replay", async () => {
    Cadenza.createCadenzaService(
      "TelemetryCollectorService",
      "Telemetry collector",
      {
        port: 3003,
        cadenzaDB: {
          connect: true,
          address: "cadenza-db-service",
          port: 8080,
        },
      },
    );

    const registry = ServiceRegistry.instance as any;
    const handleInstanceUpdateTask = Cadenza.get("Handle Instance Update");

    expect(handleInstanceUpdateTask).toBeDefined();

    Cadenza.run(handleInstanceUpdateTask!, {
      serviceInstance: {
        uuid: "telemetry-stale-1",
        service_name: "TelemetryCollectorService",
        is_active: true,
        is_non_responsive: false,
        is_blocked: false,
        transports: [
          {
            uuid: "telemetry-stale-transport-1",
            service_instance_id: "telemetry-stale-1",
            role: "internal",
            origin: "http://telemetry-collector:3003",
            protocols: ["rest"],
          },
        ],
      },
    });

    await new Promise((resolve) => setTimeout(resolve, 25));

    expect(
      registry.instances
        .get("TelemetryCollectorService")
        ?.some((instance: any) => instance.uuid === "telemetry-stale-1"),
    ).toBe(false);
  });

  it("ignores foreign same-service transport updates from authority replay", async () => {
    Cadenza.createCadenzaService(
      "TelemetryCollectorService",
      "Telemetry collector",
      {
        port: 3003,
        cadenzaDB: {
          connect: true,
          address: "cadenza-db-service",
          port: 8080,
        },
      },
    );

    const registry = ServiceRegistry.instance as any;
    const handleTransportUpdateTask = Cadenza.get("Handle Transport Update");

    expect(handleTransportUpdateTask).toBeDefined();

    vi.spyOn(registry, "hydrateAuthorityInstanceForTransport").mockResolvedValue({
      uuid: "telemetry-stale-1",
      serviceName: "TelemetryCollectorService",
      isFrontend: false,
      isDatabase: false,
      isActive: true,
      isNonResponsive: false,
      isBlocked: false,
      numberOfRunningGraphs: 0,
      isPrimary: true,
      health: {},
      transports: [],
    });

    Cadenza.run(handleTransportUpdateTask!, {
      data: {
        uuid: "telemetry-stale-transport-1",
        service_instance_id: "telemetry-stale-1",
        role: "internal",
        origin: "http://telemetry-collector:3003",
        protocols: ["rest"],
      },
    });

    await new Promise((resolve) => setTimeout(resolve, 25));

    expect(
      registry.instances
        .get("TelemetryCollectorService")
        ?.some((instance: any) => instance.uuid === "telemetry-stale-1"),
    ).toBe(false);
  });

  it(
    "marks local authority transports ready when the authoritative transport row arrives",
    async () => {
    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "CadenzaDB";
    registry.serviceInstanceId = "cadenza-db-1";

    registry.instances.set("CadenzaDB", [
      {
        uuid: "cadenza-db-1",
        serviceName: "CadenzaDB",
        numberOfRunningGraphs: 0,
        isPrimary: true,
        isActive: true,
        isNonResponsive: false,
        isBlocked: false,
        runtimeState: "healthy",
        acceptingWork: true,
        reportedAt: new Date().toISOString(),
        health: {},
        isFrontend: false,
        isDatabase: true,
        transports: [
          {
            uuid: "cadenza-db-internal-bootstrap",
            serviceInstanceId: "cadenza-db-1",
            role: "internal",
            origin: "http://cadenza-db-service:8080",
            protocols: ["rest", "socket"],
            securityProfile: null,
            authStrategy: null,
            deleted: false,
          },
        ],
        clientCreatedTransportIds: ["cadenza-db-internal-bootstrap"],
        clientReadyTransportIds: ["cadenza-db-internal-bootstrap"],
      },
    ]);

    const handleTransportUpdateTask = Cadenza.get("Handle Transport Update");
    expect(handleTransportUpdateTask).toBeDefined();

    Cadenza.run(handleTransportUpdateTask!, {
      data: {
        uuid: "11111111-1111-4111-8111-111111111111",
        service_instance_id: "cadenza-db-1",
        role: "internal",
        origin: "http://cadenza-db-service:8080",
        protocols: ["rest", "socket"],
      },
    });

    await waitForCondition(() => {
      const instance = registry.instances.get("CadenzaDB")?.[0];
      return Boolean(
        instance?.clientReadyTransportIds?.includes(
          buildRouteKey("CadenzaDB", "internal", "http://cadenza-db-service:8080"),
        ),
      );
    }, 3_000);
    },
    15_000,
  );

  it("does not create global signal transmitters for self-service maps", async () => {
    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "OrdersService";
    const createSignalTransmissionTaskSpy = vi.spyOn(
      Cadenza,
      "createSignalTransmissionTask",
    );

    Cadenza.createMetaTask("Emit global orders signal", () => true).emits(
      "global.orders.updated",
    );

    const handleGlobalSignalRegistrationTask = Cadenza.get(
      "Handle global Signal Registration",
    );
    expect(handleGlobalSignalRegistrationTask).toBeDefined();

    Cadenza.run(handleGlobalSignalRegistrationTask!, {
      data: {
        signal_name: "global.orders.updated",
        service_name: "OrdersService",
        is_global: true,
      },
    });

    await new Promise((resolve) => setTimeout(resolve, 25));

    expect(createSignalTransmissionTaskSpy).not.toHaveBeenCalledWith(
      "global.orders.updated",
      "OrdersService",
    );
  });

  it("marks observed global signals as registered when authority sync returns remote listeners", async () => {
    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "CadenzaDB";
    registry.ensureAuthorityServiceCommunicationPersistenceTask();

    const observer = (Cadenza.signalBroker as any).signalObservers.get(
      "global.meta.fetch.service_communication_established",
    );
    expect(observer).toBeTruthy();
    observer.registered = false;
    observer.registrationRequested = true;

    const handleGlobalSignalRegistrationTask = Cadenza.get(
      "Handle global Signal Registration",
    );
    expect(handleGlobalSignalRegistrationTask).toBeDefined();

    Cadenza.run(handleGlobalSignalRegistrationTask!, {
      data: {
        signal_name: "global.meta.fetch.service_communication_established",
        service_name: "TelemetryCollectorService",
        is_global: true,
      },
    });

    await new Promise((resolve) => setTimeout(resolve, 25));

    expect(observer.registered).toBe(true);
    expect(observer.registrationRequested).toBe(false);
  });

  it("tracks emitted routine lifecycle signals when tracking runtime load", async () => {
    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "OrdersService";
    registry.serviceInstanceId = "orders-1";
    registry.instances.set("OrdersService", [
      {
        uuid: "orders-1",
        serviceName: "OrdersService",
        isActive: true,
        isNonResponsive: false,
        isBlocked: false,
        health: {},
        transports: [],
      },
    ]);

    Cadenza.createTask("Business load tracker probe", () => true);

    Cadenza.emit("meta.node.started_routine_execution", {
      filter: { uuid: "business-routine-1" },
      __signalEmission: {
        taskName: "Business load tracker probe",
      },
    });

    await new Promise((resolve) => setTimeout(resolve, 25));

    expect(registry.activeRoutineExecutionIds.size).toBe(1);
    expect(registry.numberOfRunningGraphs).toBe(1);

    Cadenza.emit("meta.node.ended_routine_execution", {
      filter: { uuid: "business-routine-1" },
      __signalEmission: {
        taskName: "Business load tracker probe",
      },
    });

    await new Promise((resolve) => setTimeout(resolve, 25));

    expect(registry.activeRoutineExecutionIds.size).toBe(0);
    expect(registry.numberOfRunningGraphs).toBe(0);
  });

  it(
    "retries bootstrap full sync one timer at a time until a healthy result arrives",
    async () => {
      vi.useFakeTimers();

    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "OrdersService";
    registry.connectsToCadenzaDB = true;
    registry.bootstrapFullSyncRetryJitterRatio = 0;
    registry.seedAuthorityBootstrapRoute(
      "http://cadenza-db-service:8080",
      "internal",
    );
    registry.noteAuthorityBootstrapHandshake({
      serviceName: "CadenzaDB",
      serviceInstanceId: "cadenza-db-1",
      serviceTransportId: "cadenza-db-transport-1",
      serviceOrigin: "http://cadenza-db-service:8080",
    });

    const syncRequestSpy = vi.spyOn(Cadenza, "emit");
    const inquireSpy = vi
      .spyOn(Cadenza, "inquire")
      .mockResolvedValueOnce({
        serviceInstances: [],
        intentToTaskMaps: [],
        signalToTaskMaps: [],
      } as any)
      .mockResolvedValue({
        serviceInstances: [
          {
            uuid: "cadenza-db-1",
            service_name: "CadenzaDB",
            is_active: true,
            is_non_responsive: false,
            is_blocked: false,
            is_frontend: false,
            health: {},
          },
        ],
        intentToTaskMaps: [
          {
            intent_name: "orders.lookup",
            service_name: "OrdersService",
            task_name: "LookupOrders",
            task_version: 1,
          },
        ],
        signalToTaskMaps: [],
      } as any);
    const bootstrapFullSyncInquiryCount = () =>
      inquireSpy.mock.calls.filter(
        ([inquiry]) => inquiry === "meta-service-registry-full-sync",
      ).length;

    registry.bootstrapFullSync(() => {}, {}, "service_setup_completed");

    expect(registry.bootstrapFullSyncRetryTimer).not.toBeNull();

    await vi.advanceTimersByTimeAsync(120);

    expect(bootstrapFullSyncInquiryCount()).toBe(1);
    expect(
      syncRequestSpy.mock.calls.filter(
        ([signal]) => signal === "meta.sync_requested",
      ),
    ).toHaveLength(1);
    expect(registry.bootstrapFullSyncRetryTimer).not.toBeNull();

    await vi.advanceTimersByTimeAsync(1_800);

    expect(bootstrapFullSyncInquiryCount()).toBe(2);
    expect(
      syncRequestSpy.mock.calls.filter(
        ([signal]) => signal === "meta.sync_requested",
      ),
    ).toHaveLength(2);
    expect(registry.bootstrapFullSyncSatisfied).toBe(false);
    expect(registry.bootstrapFullSyncRetryTimer).not.toBeNull();

    await vi.advanceTimersByTimeAsync(6_000);

    expect(bootstrapFullSyncInquiryCount()).toBe(3);
    expect(
      syncRequestSpy.mock.calls.filter(
        ([signal]) => signal === "meta.sync_requested",
      ),
    ).toHaveLength(3);
    expect(registry.bootstrapFullSyncSatisfied).toBe(false);
    expect(registry.bootstrapFullSyncRetryTimer).not.toBeNull();

    await vi.advanceTimersByTimeAsync(14_400);

    expect(bootstrapFullSyncInquiryCount()).toBeGreaterThanOrEqual(4);
    expect(
      syncRequestSpy.mock.calls.filter(
        ([signal]) => signal === "meta.sync_requested",
      ).length,
    ).toBeGreaterThanOrEqual(4);

    if (!registry.bootstrapFullSyncSatisfied) {
      await vi.advanceTimersByTimeAsync(25_000);
    }

    expect(registry.bootstrapFullSyncSatisfied).toBe(true);
    expect(registry.bootstrapFullSyncRetryTimer).toBeNull();

    await vi.advanceTimersByTimeAsync(70_000);

    expect(registry.bootstrapFullSyncSatisfied).toBe(true);
    expect(registry.bootstrapFullSyncRetryTimer).toBeNull();
    },
    25_000,
  );

  it(
    "restarts bootstrap full sync recovery from the first backoff step after a fresh authority recovery cycle",
    async () => {
      vi.useFakeTimers();

    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "OrdersService";
    registry.connectsToCadenzaDB = true;
    registry.bootstrapFullSyncRetryJitterRatio = 0;
    registry.seedAuthorityBootstrapRoute(
      "http://cadenza-db-service:8080",
      "internal",
    );
    registry.noteAuthorityBootstrapHandshake({
      serviceName: "CadenzaDB",
      serviceInstanceId: "cadenza-db-1",
      serviceTransportId: "cadenza-db-transport-1",
      serviceOrigin: "http://cadenza-db-service:8080",
    });

    const inquireSpy = vi.spyOn(Cadenza, "inquire").mockResolvedValue({
      serviceInstances: [
        {
          uuid: "cadenza-db-1",
          service_name: "CadenzaDB",
          is_active: true,
          is_non_responsive: false,
          is_blocked: false,
          is_frontend: false,
          health: {},
        },
      ],
      intentToTaskMaps: [
        {
          intent_name: "orders.lookup",
          service_name: "OrdersService",
          task_name: "LookupOrders",
          task_version: 1,
        },
      ],
      signalToTaskMaps: [],
    } as any);

    registry.bootstrapFullSync(() => {}, {}, "service_setup_completed");
    await vi.advanceTimersByTimeAsync(120);
    await vi.advanceTimersByTimeAsync(1_680);
    await vi.advanceTimersByTimeAsync(6_000);
    await vi.advanceTimersByTimeAsync(14_400);

      expect(inquireSpy.mock.calls.length).toBeGreaterThanOrEqual(4);
      expect(registry.bootstrapFullSyncSatisfied).toBe(true);
      expect(registry.bootstrapFullSyncRetryTimer).toBeNull();

      registry.restartBootstrapFullSyncRetryChain("cadenza_db_fetch_handshake");

      expect(registry.bootstrapFullSyncSatisfied).toBe(false);
      expect(registry.bootstrapFullSyncRetryTimer).not.toBeNull();

      await vi.advanceTimersByTimeAsync(120);

      expect(inquireSpy.mock.calls.length).toBeGreaterThanOrEqual(5);
      expect(registry.bootstrapFullSyncSatisfied).toBe(false);
    },
    15_000,
  );

  it("registers a single bootstrap full-sync deputy", () => {
    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "OrdersService";
    registry.connectsToCadenzaDB = true;

    registry.bootstrapFullSync(() => {}, {}, "service_setup_completed");

    const fullSyncDeputies = Array.from(
      registry.remoteIntentDeputiesByKey.values(),
    ).filter(
      (descriptor: any) =>
        descriptor.intentName === "meta-service-registry-full-sync",
    );

    expect(fullSyncDeputies).toHaveLength(1);
    expect(fullSyncDeputies[0]).toMatchObject({
      serviceName: "CadenzaDB",
      remoteTaskName: "Respond service registry full sync",
    });
    expect(fullSyncDeputies[0].localTaskName).toContain(
      "Respond service registry full sync",
    );
  });

  it("marks full-sync inquiries as syncing and applies the long timeout", async () => {
    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "OrdersService";
    registry.connectsToCadenzaDB = true;
    registry.bootstrapFullSyncRetryIndex = 4;
    registry.seedAuthorityBootstrapRoute(
      "http://cadenza-db-service:8080",
      "internal",
    );
    registry.noteAuthorityBootstrapHandshake({
      serviceName: "CadenzaDB",
      serviceInstanceId: "cadenza-db-1",
      serviceTransportId: "cadenza-db-transport-1",
      serviceOrigin: "http://cadenza-db-service:8080",
    });
    registry.ensureBootstrapAuthorityControlPlaneForInquiry(
      "meta-service-registry-full-sync",
      { __reason: "service_setup_completed" },
    );

    const inquireSpy = vi.spyOn(Cadenza, "inquire").mockResolvedValue({
      serviceInstances: [
        {
          uuid: "cadenza-db-1",
          service_name: "CadenzaDB",
          is_active: true,
          is_non_responsive: false,
          is_blocked: false,
          is_frontend: false,
          health: {},
        },
      ],
      intentToTaskMaps: [
        {
          intent_name: "orders.lookup",
          service_name: "OrdersService",
          task_name: "LookupOrders",
          task_version: 1,
        },
      ],
      signalToTaskMaps: [],
    } as any);

    Cadenza.run(registry.fullSyncTask, {
      __reason: "service_setup_completed",
    });

    await waitForCondition(() => inquireSpy.mock.calls.length >= 1, 1_500);

    expect(inquireSpy).toHaveBeenCalledWith(
      "meta-service-registry-full-sync",
      expect.objectContaining({
        syncScope: "service-registry-full-sync",
        __syncing: true,
      }),
      expect.objectContaining({
        timeout: 120_000,
      }),
    );
  });

  it("builds the authority full-sync response shape from collected authority rows", () => {
    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "CadenzaDB";

    const result = registry.collectBootstrapFullSyncPayload({
      serviceInstances: [
        {
          uuid: "orders-1",
          service_name: "OrdersService",
          is_active: true,
          is_non_responsive: false,
          is_blocked: false,
        },
      ],
      serviceInstanceTransports: [
        {
          uuid: "orders-internal-1",
          service_instance_id: "orders-1",
          role: "internal",
          origin: "http://orders.example:7000",
          protocols: ["rest", "socket"],
        },
      ],
      serviceManifests: [
        {
          service_instance_id: "orders-1",
          manifest: {
            serviceName: "OrdersService",
            serviceInstanceId: "orders-1",
            revision: 1,
            manifestHash: "orders-manifest-v1",
            publishedAt: "2026-03-29T18:00:00.000Z",
            tasks: [],
            signals: [],
            intents: [],
            actors: [],
            routines: [],
            directionalTaskMaps: [],
            actorTaskMaps: [],
            taskToRoutineMaps: [],
            signalToTaskMaps: [
              {
                signal_name: "global.orders.updated",
                service_name: "OrdersService",
                is_global: true,
              },
            ],
            intentToTaskMaps: [
              {
                intent_name: "orders.lookup",
                service_name: "OrdersService",
                task_name: "LookupOrders",
                task_version: 1,
              },
            ],
          },
        },
      ],
    }) as Record<string, any>;

    expect(result).toMatchObject({
      serviceInstances: [
        expect.objectContaining({
          uuid: "orders-1",
          service_name: "OrdersService",
        }),
      ],
      serviceInstanceTransports: [
        expect.objectContaining({
          uuid: "orders-internal-1",
          service_instance_id: "orders-1",
        }),
      ],
      signalToTaskMaps: [
        expect.objectContaining({
          signal_name: "global.orders.updated",
          service_name: "OrdersService",
        }),
      ],
      intentToTaskMaps: [
        expect.objectContaining({
          intent_name: "orders.lookup",
          service_name: "OrdersService",
          task_name: "LookupOrders",
        }),
      ],
    });
  });

  it("prefers the latest manifest revision per service instance when composing authority full sync", () => {
    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "CadenzaDB";

    const result = registry.collectBootstrapFullSyncPayload({
      serviceInstances: [
        {
          uuid: "orders-1",
          service_name: "OrdersService",
          is_active: true,
          is_non_responsive: false,
          is_blocked: false,
        },
      ],
      serviceInstanceTransports: [
        {
          uuid: "orders-internal-1",
          service_instance_id: "orders-1",
          role: "internal",
          origin: "http://orders.example:7000",
          protocols: ["rest", "socket"],
        },
      ],
      serviceManifests: [
        {
          service_instance_id: "orders-1",
          manifest: {
            serviceName: "OrdersService",
            serviceInstanceId: "orders-1",
            revision: 1,
            manifestHash: "orders-manifest-v1",
            publishedAt: "2026-03-29T18:00:00.000Z",
            tasks: [],
            signals: [],
            intents: [],
            actors: [],
            routines: [],
            directionalTaskMaps: [],
            actorTaskMaps: [],
            taskToRoutineMaps: [],
            signalToTaskMaps: [
              {
                signal_name: "global.orders.stale",
                service_name: "OrdersService",
                task_name: "Handle stale orders update",
                task_version: 1,
                is_global: true,
              },
            ],
            intentToTaskMaps: [],
          },
        },
        {
          service_instance_id: "orders-1",
          manifest: {
            serviceName: "OrdersService",
            serviceInstanceId: "orders-1",
            revision: 2,
            manifestHash: "orders-manifest-v2",
            publishedAt: "2026-03-29T18:05:00.000Z",
            tasks: [],
            signals: [],
            intents: [],
            actors: [],
            routines: [],
            directionalTaskMaps: [],
            actorTaskMaps: [],
            taskToRoutineMaps: [],
            signalToTaskMaps: [
              {
                signal_name: "global.orders.updated",
                service_name: "OrdersService",
                task_name: "Handle orders update",
                task_version: 1,
                is_global: true,
              },
            ],
            intentToTaskMaps: [],
          },
        },
      ],
    });

    expect(result.serviceManifests).toEqual([
      expect.objectContaining({
        service_instance_id: "orders-1",
        revision: 2,
      }),
    ]);
    expect(result.signalToTaskMaps).toEqual([
      expect.objectContaining({
        signal_name: "global.orders.updated",
        service_name: "OrdersService",
        task_name: "Handle orders update",
      }),
    ]);
  });

  it("filters inactive instance transports and manifests out of bootstrap full sync", () => {
    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "CadenzaDB";

    const result = registry.collectBootstrapFullSyncPayload({
      serviceInstances: [
        {
          uuid: "orders-active",
          service_name: "OrdersService",
          is_active: true,
          is_non_responsive: false,
          is_blocked: false,
        },
        {
          uuid: "orders-stale",
          service_name: "OrdersService",
          is_active: false,
          is_non_responsive: false,
          is_blocked: true,
        },
      ],
      serviceInstanceTransports: [
        {
          uuid: "orders-active-transport",
          service_instance_id: "orders-active",
          role: "internal",
          origin: "http://orders-active.example:7000",
          protocols: ["rest"],
        },
        {
          uuid: "orders-stale-transport",
          service_instance_id: "orders-stale",
          role: "internal",
          origin: "http://orders-stale.example:7000",
          protocols: ["rest"],
        },
      ],
      serviceManifests: [
        {
          service_instance_id: "orders-active",
          manifest: {
            serviceName: "OrdersService",
            serviceInstanceId: "orders-active",
            revision: 3,
            manifestHash: "orders-active-v3",
            publishedAt: "2026-04-10T12:00:00.000Z",
            tasks: [],
            signals: [],
            intents: [],
            actors: [],
            routines: [],
            directionalTaskMaps: [],
            actorTaskMaps: [],
            taskToRoutineMaps: [],
            signalToTaskMaps: [],
            intentToTaskMaps: [],
          },
        },
        {
          service_instance_id: "orders-stale",
          manifest: {
            serviceName: "OrdersService",
            serviceInstanceId: "orders-stale",
            revision: 9,
            manifestHash: "orders-stale-v9",
            publishedAt: "2026-04-10T12:05:00.000Z",
            tasks: [],
            signals: [],
            intents: [],
            actors: [],
            routines: [],
            directionalTaskMaps: [],
            actorTaskMaps: [],
            taskToRoutineMaps: [],
            signalToTaskMaps: [],
            intentToTaskMaps: [],
          },
        },
      ],
      signalToTaskMaps: [
        {
          signal_name: "global.orders.updated",
          service_name: "OrdersService",
          task_name: "Handle active orders update",
          task_version: 1,
        },
        {
          signal_name: "global.orders.stale",
          service_name: "RetiredOrdersService",
          task_name: "Handle stale orders update",
          task_version: 1,
        },
      ],
      intentToTaskMaps: [
        {
          intent_name: "orders.lookup",
          service_name: "OrdersService",
          task_name: "LookupOrders",
          task_version: 1,
        },
        {
          intent_name: "orders.lookup.stale",
          service_name: "RetiredOrdersService",
          task_name: "LookupRetiredOrders",
          task_version: 1,
        },
      ],
    });

    expect(result.serviceInstances).toEqual([
      expect.objectContaining({
        uuid: "orders-active",
      }),
    ]);
    expect(result.serviceInstanceTransports).toEqual([
      expect.objectContaining({
        uuid: "orders-active-transport",
        service_instance_id: "orders-active",
      }),
    ]);
    expect(result.serviceManifests).toEqual([
      expect.objectContaining({
        service_instance_id: "orders-active",
        manifest_hash: "orders-active-v3",
      }),
    ]);
    expect(result.signalToTaskMaps).toEqual([
      expect.objectContaining({
        service_name: "OrdersService",
        signal_name: "global.orders.updated",
      }),
    ]);
    expect(result.intentToTaskMaps).toEqual([
      expect.objectContaining({
        service_name: "OrdersService",
        intent_name: "orders.lookup",
      }),
    ]);
  });

  it("keeps distinct task associations for the same signal in authority full sync", () => {
    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "CadenzaDB";

    const result = registry.collectBootstrapFullSyncPayload({
      serviceManifests: [
        {
          service_instance_id: "runner-1",
          manifest: {
            serviceName: "ScheduledRunnerService",
            serviceInstanceId: "runner-1",
            revision: 3,
            manifestHash: "runner-manifest-v3",
            publishedAt: "2026-04-10T12:00:00.000Z",
            tasks: [],
            signals: [],
            intents: [],
            actors: [],
            routines: [],
            directionalTaskMaps: [],
            actorTaskMaps: [],
            taskToRoutineMaps: [],
            signalToTaskMaps: [
              {
                signal_name: "global.meta.service_instance.updated",
                service_name: "ScheduledRunnerService",
                task_name: "Handle Instance Update",
                task_version: 1,
                is_global: true,
              },
              {
                signal_name: "global.meta.service_instance.updated",
                service_name: "ScheduledRunnerService",
                task_name: "Ensure authority bootstrap signal transmissions",
                task_version: 1,
                is_global: true,
              },
            ],
            intentToTaskMaps: [],
          },
        },
      ],
      signalToTaskMaps: [
        {
          signal_name: "global.meta.service_instance.updated",
          service_name: "ScheduledRunnerService",
          task_name: "Handle Instance Update",
          task_version: 1,
          is_global: true,
        },
        {
          signal_name: "global.meta.service_instance.updated",
          service_name: "ScheduledRunnerService",
          task_name: "Ensure authority bootstrap signal transmissions",
          task_version: 1,
          is_global: true,
        },
      ],
      intentToTaskMaps: [],
    });

    expect(result.signalToTaskMaps).toEqual([
      expect.objectContaining({
        signal_name: "global.meta.service_instance.updated",
        service_name: "ScheduledRunnerService",
        task_name: "Handle Instance Update",
      }),
      expect.objectContaining({
        signal_name: "global.meta.service_instance.updated",
        service_name: "ScheduledRunnerService",
        task_name: "Ensure authority bootstrap signal transmissions",
      }),
    ]);
  });

  it("registers the authority full-sync responder as the one persisted responder", () => {
    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "CadenzaDB";

    const responderTask = registry.ensureAuthorityFullSyncResponderTask();

    expect(responderTask).toBeDefined();
    expect(responderTask).toMatchObject({
      name: "Respond service registry full sync",
      register: true,
      isHidden: false,
    });
    expect(responderTask?.handlesIntents.has("meta-service-registry-full-sync")).toBe(
      true,
    );
  });

  it("exports signal delivery metadata in the manifest snapshot", () => {
    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "OrdersService";
    registry.serviceInstanceId = "orders-instance-1";

    Cadenza.createTask("Consume order updates", () => true).doOn({
      name: "global.orders.updated",
      deliveryMode: "broadcast",
      broadcastFilter: {
        serviceNames: ["BillingService", "WarehouseService"],
        origins: ["http://billing.internal"],
      },
    });

    const manifest = buildServiceManifestSnapshot({
      serviceName: "OrdersService",
      serviceInstanceId: "orders-instance-1",
      revision: 1,
      publishedAt: "2026-04-04T12:00:00.000Z",
    });

    const exportedSignal = manifest.signals.find(
      (signal) => signal.name === "global.orders.updated",
    );

    expect(exportedSignal).toMatchObject({
      name: "global.orders.updated",
      is_global: true,
      is_meta: false,
      delivery_mode: "broadcast",
      broadcast_filter: {
        serviceNames: ["BillingService", "WarehouseService"],
        origins: ["http://billing.internal"],
      },
    });
  });

  it("separates routing, business, and local meta manifest layers", async () => {
    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "OrdersService";
    registry.serviceInstanceId = "orders-instance-1";
    GraphMetadataController.instance;

    Cadenza.createTask("Handle order inquiry", () => true)
      .respondsTo("orders-compute")
      .doOn("orders.created");
    Cadenza.createTask("Project order read model", () => true);
    Cadenza.createMetaTask("Handle authority manifest report", () => true).respondsTo(
      AUTHORITY_SERVICE_MANIFEST_REPORT_INTENT,
    );
    Cadenza.createMetaTask("Handle authority manifest updates", () => true).doOn(
      AUTHORITY_SERVICE_MANIFEST_UPDATED_SIGNAL,
    );
    Cadenza.createMetaTask(
      "Process execution persistence bundle",
      () => true,
    ).doOn(EXECUTION_PERSISTENCE_BUNDLE_SIGNAL);
    Cadenza.createMetaTask("Handle order meta sync", () => true)
      .respondsTo("meta-orders-sync")
      .doOn("meta.orders.internal");

    const businessActor = Cadenza.createActor<{ count: number }>({
      name: "OrderSessionActor",
      defaultKey: "order:unknown",
      initState: { count: 0 },
    });
    Cadenza.createTask(
      "Record order session",
      businessActor.task(({ state, setState }) => {
        setState({ count: state.count + 1 });
        return true;
      }, { mode: "write" }),
    ).doOn("orders.session.recorded");

    const metaActor = Cadenza.createActor<{ count: number }>(
      {
        name: "OrderMetaSessionActor",
        defaultKey: "order:meta",
        initState: { count: 0 },
      },
      { isMeta: true },
    );
    Cadenza.createMetaTask(
      "Record order meta session",
      metaActor.task(({ state, setState }) => {
        setState({ count: state.count + 1 });
        return true;
      }, { mode: "write" }),
    ).doOn("meta.orders.session.recorded");

    const routingManifest = buildServiceManifestSnapshot({
      serviceName: "OrdersService",
      serviceInstanceId: "orders-instance-1",
      revision: 1,
      publishedAt: "2026-04-10T12:00:00.000Z",
      publicationLayer: "routing_capability",
    });
    const businessManifest = buildServiceManifestSnapshot({
      serviceName: "OrdersService",
      serviceInstanceId: "orders-instance-1",
      revision: 2,
      publishedAt: "2026-04-10T12:00:01.000Z",
      publicationLayer: "business_structural",
    });
    const localMetaManifest = buildServiceManifestSnapshot({
      serviceName: "OrdersService",
      serviceInstanceId: "orders-instance-1",
      revision: 3,
      publishedAt: "2026-04-10T12:00:02.000Z",
      publicationLayer: "local_meta_structural",
    });

    expect(routingManifest.publicationLayer).toBe("routing_capability");
    expect(routingManifest.tasks.map((task) => task.name)).toEqual(
      expect.arrayContaining([
        "Handle order inquiry",
        "Handle authority manifest report",
        "Handle authority manifest updates",
      ]),
    );
    expect(routingManifest.signals.map((signal) => signal.name)).toEqual(
      expect.arrayContaining([
        "orders.created",
        AUTHORITY_SERVICE_MANIFEST_UPDATED_SIGNAL,
      ]),
    );
    expect(routingManifest.intents.map((intent) => intent.name)).toEqual(
      expect.arrayContaining([
        "orders-compute",
        AUTHORITY_SERVICE_MANIFEST_REPORT_INTENT,
      ]),
    );
    expect(routingManifest.signalToTaskMaps).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          signal_name: "orders.created",
          task_name: "Handle order inquiry",
        }),
        expect.objectContaining({
          signal_name: AUTHORITY_SERVICE_MANIFEST_UPDATED_SIGNAL,
          task_name: "Handle authority manifest updates",
        }),
        expect.objectContaining({
          signal_name: EXECUTION_PERSISTENCE_BUNDLE_SIGNAL,
          task_name: "Process execution persistence bundle",
        }),
      ]),
    );
    expect(routingManifest.intentToTaskMaps).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          intent_name: "orders-compute",
          task_name: "Handle order inquiry",
        }),
        expect.objectContaining({
          intent_name: AUTHORITY_SERVICE_MANIFEST_REPORT_INTENT,
          task_name: "Handle authority manifest report",
        }),
      ]),
    );
    expect(routingManifest.actors).toEqual([]);
    expect(routingManifest.routines).toEqual([]);
    expect(routingManifest.directionalTaskMaps).toEqual([]);

    expect(businessManifest.publicationLayer).toBe("business_structural");
    expect(businessManifest.tasks.map((task) => task.name)).toEqual(
      expect.arrayContaining([
        "Handle order inquiry",
        "Project order read model",
        "Handle authority manifest report",
        "Handle authority manifest updates",
      ]),
    );
    expect(businessManifest.tasks.map((task) => task.name)).not.toContain(
      "Handle order meta sync",
    );
    expect(businessManifest.signals.map((signal) => signal.name)).toEqual(
      expect.arrayContaining([
        "orders.created",
        AUTHORITY_SERVICE_MANIFEST_UPDATED_SIGNAL,
      ]),
    );
    expect(businessManifest.signals.map((signal) => signal.name)).not.toContain(
      "meta.orders.internal",
    );
    expect(businessManifest.intents.map((intent) => intent.name)).toEqual(
      expect.arrayContaining([
        "orders-compute",
        AUTHORITY_SERVICE_MANIFEST_REPORT_INTENT,
      ]),
    );
    expect(businessManifest.intents.map((intent) => intent.name)).not.toContain(
      "meta-orders-sync",
    );
    expect(businessManifest.signalToTaskMaps).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          signal_name: "orders.created",
          task_name: "Handle order inquiry",
        }),
        expect.objectContaining({
          signal_name: AUTHORITY_SERVICE_MANIFEST_UPDATED_SIGNAL,
          task_name: "Handle authority manifest updates",
        }),
      ]),
    );
    expect(
      businessManifest.signalToTaskMaps.some(
        (map) =>
          map.signal_name === "meta.orders.internal" &&
          map.task_name === "Handle order meta sync",
      ),
    ).toBe(false);
    expect(businessManifest.intentToTaskMaps).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          intent_name: "orders-compute",
          task_name: "Handle order inquiry",
        }),
        expect.objectContaining({
          intent_name: AUTHORITY_SERVICE_MANIFEST_REPORT_INTENT,
          task_name: "Handle authority manifest report",
        }),
      ]),
    );
    expect(
      businessManifest.intentToTaskMaps.some(
        (map) =>
          map.intent_name === "meta-orders-sync" &&
          map.task_name === "Handle order meta sync",
      ),
    ).toBe(false);
    expect(
      businessManifest.actorTaskMaps.some(
        (map) =>
          map.actor_name === "OrderMetaSessionActor" &&
          map.task_name === "Record order meta session",
      ),
    ).toBe(false);
    expect(
      businessManifest.actorTaskMaps.some(
        (map) =>
          map.actor_name === "OrderSessionActor" &&
          map.task_name === "Record order session",
      ),
    ).toBe(true);

    expect(localMetaManifest.publicationLayer).toBe("local_meta_structural");
    expect(localMetaManifest.tasks.map((task) => task.name)).toContain(
      "Handle order meta sync",
    );
    expect(localMetaManifest.signals.map((signal) => signal.name)).toContain(
      "meta.orders.internal",
    );
    expect(localMetaManifest.intents.map((intent) => intent.name)).toContain(
      "meta-orders-sync",
    );
    expect(
      localMetaManifest.actorTaskMaps.some(
        (map) =>
          map.actor_name === "OrderMetaSessionActor" &&
          map.task_name === "Record order meta session",
      ),
    ).toBe(true);
  });

  it("does not publish hidden unregistered proxy tasks from the runtime cache", () => {
    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "OrdersService";
    registry.serviceInstanceId = "orders-instance-1";
    GraphMetadataController.instance;

    Cadenza.createTask("Handle order inquiry", () => true).respondsTo("orders-compute");
    Cadenza.createMetaTask(
      "Inquire meta-service-registry-authority-service-manifest-report via CadenzaDB (Report service manifest to authority v1)",
      () => true,
      "Runtime-only authority proxy task that must not be published durably.",
      {
        register: false,
        isHidden: true,
      },
    ).respondsTo("meta-service-registry-authority-service-manifest-report");

    const routingManifest = buildServiceManifestSnapshot({
      serviceName: "OrdersService",
      serviceInstanceId: "orders-instance-1",
      revision: 1,
      publishedAt: "2026-04-10T12:00:00.000Z",
      publicationLayer: "routing_capability",
    });
    const businessManifest = buildServiceManifestSnapshot({
      serviceName: "OrdersService",
      serviceInstanceId: "orders-instance-1",
      revision: 2,
      publishedAt: "2026-04-10T12:00:01.000Z",
      publicationLayer: "business_structural",
    });
    const localMetaManifest = buildServiceManifestSnapshot({
      serviceName: "OrdersService",
      serviceInstanceId: "orders-instance-1",
      revision: 3,
      publishedAt: "2026-04-10T12:00:02.000Z",
      publicationLayer: "local_meta_structural",
    });

    const forbiddenTaskName =
      "Inquire meta-service-registry-authority-service-manifest-report via CadenzaDB (Report service manifest to authority v1)";
    const containsForbiddenIntentMap = (manifest: {
      intentToTaskMaps: Array<{ intent_name: string; task_name: string }>;
    }) =>
      manifest.intentToTaskMaps.some(
        (map) =>
          map.intent_name ===
            "meta-service-registry-authority-service-manifest-report" &&
          map.task_name === forbiddenTaskName,
      );

    expect(routingManifest.tasks.map((task) => task.name)).not.toContain(forbiddenTaskName);
    expect(businessManifest.tasks.map((task) => task.name)).not.toContain(forbiddenTaskName);
    expect(localMetaManifest.tasks.map((task) => task.name)).not.toContain(forbiddenTaskName);
    expect(containsForbiddenIntentMap(routingManifest)).toBe(false);
    expect(containsForbiddenIntentMap(businessManifest)).toBe(false);
    expect(containsForbiddenIntentMap(localMetaManifest)).toBe(false);
  });

  it("publishes routing capability before business structure and deferred local meta", async () => {
    vi.useFakeTimers();
    try {
    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "ScheduledRunnerService";
    registry.serviceInstanceId = "scheduled-runner-1";
    registry.connectsToCadenzaDB = true;
    GraphMetadataController.instance;
    registry.seedAuthorityBootstrapRoute(
      "http://cadenza-db-service:8080",
      "internal",
    );
    registry.noteAuthorityBootstrapHandshake({
      serviceName: "CadenzaDB",
      serviceInstanceId: "cadenza-db-live-1",
      serviceTransportId: "cadenza-db-transport-1",
      serviceOrigin: "http://cadenza-db-service:8080",
    });

    Cadenza.createTask("Handle runner dispatch", () => true)
      .respondsTo("runner-dispatch")
      .doOn("runner.dispatched");
    Cadenza.createTask("Project runner state", () => true);
    Cadenza.createMetaTask("Handle runner meta sync", () => true)
      .respondsTo("meta-runner-sync")
      .doOn("meta.runner.internal");

    (Cadenza as any).serviceManifestRevision = 0;
    (Cadenza as any).lastPublishedServiceManifestHashes = {};
    (Cadenza as any).serviceManifestPublishedAt = {};
    (Cadenza as any).bootstrapSyncCompleted = false;
    (Cadenza as any).bootstrapSyncCompletedAt = 0;
    (Cadenza as any).localServiceManifestDefinitionInserted = true;
    (Cadenza as any).localServiceManifestInstanceInserted = true;

    const inquireSpy = vi.spyOn(Cadenza, "inquire").mockResolvedValue({});

    const firstResult = await (Cadenza as any).publishServiceManifestIfNeeded(
      "service_setup_completed",
      "local_meta_structural",
    );

    expect(firstResult).toMatchObject({
      published: true,
      publicationLayer: "routing_capability",
      serviceManifest: expect.objectContaining({
        publicationLayer: "routing_capability",
        tasks: [
          expect.objectContaining({
            name: "Handle runner dispatch",
          }),
        ],
      }),
    });
    expect(inquireSpy).toHaveBeenNthCalledWith(
      1,
      AUTHORITY_SERVICE_MANIFEST_REPORT_INTENT,
      expect.objectContaining({
        publicationLayer: "routing_capability",
      }),
      expect.objectContaining({
        timeout: 15_000,
        requireComplete: true,
      }),
    );
    expect(inquireSpy).toHaveBeenCalledTimes(1);

    (Cadenza as any).markBootstrapSyncCompleted();

    await vi.advanceTimersByTimeAsync(1_500);

    expect(inquireSpy).toHaveBeenNthCalledWith(
      2,
      AUTHORITY_SERVICE_MANIFEST_REPORT_INTENT,
      expect.objectContaining({
        publicationLayer: "business_structural",
      }),
      expect.objectContaining({
        timeout: 15_000,
        requireComplete: true,
      }),
    );

    await vi.advanceTimersByTimeAsync(15_000);

    expect(inquireSpy).toHaveBeenNthCalledWith(
      3,
      AUTHORITY_SERVICE_MANIFEST_REPORT_INTENT,
      expect.objectContaining({
        publicationLayer: "local_meta_structural",
      }),
      expect.objectContaining({
        timeout: 15_000,
        requireComplete: true,
      }),
    );
    expect((Cadenza as any).lastPublishedServiceManifestHashes).toMatchObject({
      routing_capability: expect.any(String),
      business_structural: expect.any(String),
      local_meta_structural: expect.any(String),
    });
    expect((Cadenza as any).serviceManifestRevision).toBe(3);
    } finally {
      vi.useRealTimers();
    }
  });

  it("caps local authority self-publication at routing capability", async () => {
    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "CadenzaDB";
    registry.serviceInstanceId = "cadenza-db-live-1";
    registry.connectsToCadenzaDB = false;
    GraphMetadataController.instance;

    Cadenza.createMetaTask("Process execution persistence bundle", () => true).doOn(
      EXECUTION_PERSISTENCE_BUNDLE_SIGNAL,
    );
    Cadenza.createMetaTask("Local authority-only meta task", () => true);

    (Cadenza as any).serviceManifestRevision = 0;
    (Cadenza as any).lastPublishedServiceManifestHashes = {};
    (Cadenza as any).serviceManifestPublishedAt = {};
    (Cadenza as any).bootstrapSyncCompleted = true;
    (Cadenza as any).bootstrapSyncCompletedAt = Date.now();
    (Cadenza as any).localServiceManifestDefinitionInserted = true;
    (Cadenza as any).localServiceManifestInstanceInserted = true;

    const inquireSpy = vi.spyOn(Cadenza, "inquire").mockResolvedValue({});

    const result = await (Cadenza as any).publishServiceManifestIfNeeded(
      "service_setup_completed",
      "local_meta_structural",
    );

    expect(result).toMatchObject({
      published: true,
      publicationLayer: "routing_capability",
      serviceManifest: expect.objectContaining({
        publicationLayer: "routing_capability",
      }),
    });
    expect(inquireSpy).toHaveBeenCalledTimes(1);
    expect(inquireSpy).toHaveBeenCalledWith(
      AUTHORITY_SERVICE_MANIFEST_REPORT_INTENT,
      expect.objectContaining({
        publicationLayer: "routing_capability",
      }),
      expect.objectContaining({
        timeout: 60_000,
        requireComplete: true,
      }),
    );

    const snapshot = inquireSpy.mock.calls[0]?.[1] as ReturnType<
      typeof buildServiceManifestSnapshot
    >;
    expect(
      snapshot.signalToTaskMaps.some(
        (map) =>
          map.signal_name === EXECUTION_PERSISTENCE_BUNDLE_SIGNAL &&
          map.task_name === "Process execution persistence bundle",
      ),
    ).toBe(true);
    expect(snapshot.tasks.map((task) => task.name)).not.toContain(
      "Local authority-only meta task",
    );
    expect((Cadenza as any).serviceManifestRevision).toBe(1);
    expect((Cadenza as any).lastPublishedServiceManifestHashes).toEqual({
      routing_capability: expect.any(String),
    });
  });

  it("resolves the authority full-sync inquiry through the single local responder", async () => {
    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "CadenzaDB";
    registry.ensureAuthorityFullSyncResponderTask();

    const queryAuthorityTableRowsSpy = vi
      .spyOn(DatabaseController.instance, "queryAuthorityTableRows")
      .mockImplementation(async (tableName: string) => {
        switch (tableName) {
          case "service_instance":
            return [
              {
                uuid: "orders-1",
                service_name: "OrdersService",
                is_active: true,
              },
            ];
          case "service_instance_transport":
            return [
              {
                uuid: "orders-transport-1",
                service_instance_id: "orders-1",
                role: "internal",
                origin: "http://orders.example:7000",
                protocols: ["rest", "socket"],
              },
            ];
          case "service_manifest":
            return [
              {
                serviceName: "OrdersService",
                serviceInstanceId: "orders-1",
                revision: 1,
                manifestHash: "orders-manifest-v1",
                publishedAt: "2026-03-29T18:00:00.000Z",
                signalToTaskMaps: [
                  {
                    signal_name: "global.orders.updated",
                    service_name: "OrdersService",
                    task_name: "Handle orders update",
                    task_version: 1,
                    is_global: true,
                  },
                ],
                intentToTaskMaps: [
                  {
                    intent_name: "orders.lookup",
                    service_name: "OrdersService",
                    task_name: "LookupOrders",
                    task_version: 1,
                  },
                ],
              },
            ];
          case "service_instance_lease":
            return [
              {
                service_instance_id: "orders-1",
                status: "active",
                is_ready: true,
                readiness_reason: "accepting_work",
                lease_expires_at: "2026-03-29T18:00:45.000Z",
                last_lease_renewed_at: "2026-03-29T18:00:00.000Z",
                last_ready_at: "2026-03-29T18:00:00.000Z",
              },
            ];
          default:
            return [];
        }
      });

    const result = await Cadenza.inquire("meta-service-registry-full-sync", {
      syncScope: "service-registry-full-sync",
      __syncing: true,
    });

    expect(queryAuthorityTableRowsSpy).toHaveBeenCalledTimes(6);
    expect(result).toMatchObject({
      serviceInstances: [
        expect.objectContaining({
          uuid: "orders-1",
          service_name: "OrdersService",
          lease_status: "active",
          is_ready: true,
        }),
      ],
      serviceInstanceTransports: [
        expect.objectContaining({
          uuid: "orders-transport-1",
          service_instance_id: "orders-1",
        }),
      ],
      signalToTaskMaps: [
        expect.objectContaining({
          signal_name: "global.orders.updated",
          service_name: "OrdersService",
        }),
      ],
      intentToTaskMaps: [
        expect.objectContaining({
          intent_name: "orders.lookup",
          service_name: "OrdersService",
          task_name: "LookupOrders",
        }),
      ],
    });
  });

  it("registers the authority service communication handler only on CadenzaDB", () => {
    const registry = ServiceRegistry.instance as any;

    registry.serviceName = "OrdersService";
    expect(registry.ensureAuthorityServiceCommunicationPersistenceTask()).toBeNull();

    registry.serviceName = "CadenzaDB";
    const task = registry.ensureAuthorityServiceCommunicationPersistenceTask();

    expect(task).toBeDefined();
    expect(task).toMatchObject({
      name: "Handle service communication established",
      register: true,
      isHidden: false,
    });
    expect(Cadenza.get("Handle service communication established")).toBe(task);
  });

  it("retries authority service communication persistence until both service instances exist", async () => {
    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "CadenzaDB";
    registry.ensureAuthorityServiceCommunicationPersistenceTask();

    const processTask = Cadenza.get(
      "Process authority service communication persistence",
    );
    expect(processTask).toBeDefined();

    const queryAuthorityTableRowsSpy = vi
      .spyOn(DatabaseController.instance, "queryAuthorityTableRows")
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([{ uuid: "runner-1" }]);
    const persistAuthorityInsertSpy = vi.spyOn(
      DatabaseController.instance,
      "persistAuthorityInsert",
    );
    const scheduleSpy = vi.spyOn(Cadenza, "schedule").mockImplementation(() => undefined);

    const result = await (processTask as any).taskFunction(
      buildServiceCommunicationEstablishedContext({
        serviceInstanceId: "telemetry-1",
        serviceInstanceClientId: "runner-1",
        communicationType: "delegation",
      }),
    );

    expect(result).toBe(false);
    expect(queryAuthorityTableRowsSpy).toHaveBeenCalledTimes(2);
    expect(persistAuthorityInsertSpy).not.toHaveBeenCalled();
    expect(scheduleSpy).toHaveBeenCalledWith(
      META_SERVICE_COMMUNICATION_PERSIST_RETRY_SIGNAL,
      expect.objectContaining({
        data: expect.objectContaining({
          serviceInstanceId: "telemetry-1",
          serviceInstanceClientId: "runner-1",
          communicationType: "delegation",
        }),
        __serviceCommunicationRetryAttempt: 1,
      }),
      expect.any(Number),
    );
    expect((scheduleSpy.mock.calls[0] ?? [])[2]).toBeGreaterThanOrEqual(250);
    expect((scheduleSpy.mock.calls[0] ?? [])[2]).toBeLessThanOrEqual(300);
  });

  it("persists authority service communication edges once both parents exist", async () => {
    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "CadenzaDB";
    registry.ensureAuthorityServiceCommunicationPersistenceTask();

    const processTask = Cadenza.get(
      "Process authority service communication persistence",
    );
    expect(processTask).toBeDefined();

    vi.spyOn(DatabaseController.instance, "queryAuthorityTableRows")
      .mockResolvedValueOnce([{ uuid: "telemetry-1" }])
      .mockResolvedValueOnce([{ uuid: "runner-1" }]);
    const persistAuthorityInsertSpy = vi
      .spyOn(DatabaseController.instance, "persistAuthorityInsert")
      .mockResolvedValue({
        rowCount: 1,
        __success: true,
      });

    const result = await (processTask as any).taskFunction(
      buildServiceCommunicationEstablishedContext({
        serviceInstanceId: "telemetry-1",
        serviceInstanceClientId: "runner-1",
        communicationType: "signal",
      }),
    );

    expect(persistAuthorityInsertSpy).toHaveBeenCalledWith(
      "service_to_service_communication_map",
      buildServiceCommunicationEstablishedContext({
        serviceInstanceId: "telemetry-1",
        serviceInstanceClientId: "runner-1",
        communicationType: "signal",
      }),
    );
    expect(result).toMatchObject({
      rowCount: 1,
      __success: true,
    });
  });

  it("keeps fetch-client helper tasks internal-only", () => {
    RestController.instance;
    const debounceSpy = vi
      .spyOn(Cadenza, "debounce")
      .mockImplementation(() => true as any);

    const setupFetchClientTask = Cadenza.get("Setup fetch client");
    expect(setupFetchClientTask).toBeDefined();

    Cadenza.run(setupFetchClientTask!, {
      serviceName: "OrdersService",
      serviceOrigin: "http://orders.example:7000",
      serviceTransportId: "orders-public-1",
      communicationTypes: ["rest"],
      handshakeData: {},
    });

    const handshakeTask = Cadenza.get(
      `Send Handshake to http://orders.example:7000 (${buildHandleKey("orders-public-1", "rest")})`,
    );
    const statusTask = Cadenza.get(
      `Request status from http://orders.example:7000 (${buildHandleKey("orders-public-1", "rest")})`,
    );
    const destroyTask = Cadenza.get(
      `Destroy fetch client ${buildHandleKey("orders-public-1", "rest")}`,
    );

    expect(handshakeTask).toMatchObject({
      register: false,
      isHidden: true,
    });
    expect(statusTask).toMatchObject({
      register: false,
      isHidden: true,
    });
    expect(destroyTask).toMatchObject({
      register: false,
      isHidden: true,
    });
    expect(debounceSpy).toHaveBeenCalledWith(
      `meta.fetch.handshake_requested:${buildHandleKey("orders-public-1", "rest")}`,
      expect.objectContaining({
        serviceName: "OrdersService",
        serviceTransportId: "orders-public-1",
        fetchId: buildHandleKey("orders-public-1", "rest"),
      }),
      50,
    );
  });

  it("does not bootstrap dependee clients for self-service intent maps", async () => {
    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "OrdersService";
    const ensureDependeeClientsSpy = vi.spyOn(
      registry,
      "ensureDependeeClientsForService",
    );

    const handleGlobalIntentRegistrationTask = Cadenza.get(
      "Handle global intent registration",
    );
    expect(handleGlobalIntentRegistrationTask).toBeDefined();

    Cadenza.run(handleGlobalIntentRegistrationTask!, {
      data: {
        intent_name: "orders.sync",
        service_name: "OrdersService",
        task_name: "Handle order sync",
        task_version: 1,
      },
    });

    await new Promise((resolve) => setTimeout(resolve, 25));

    expect(ensureDependeeClientsSpy).not.toHaveBeenCalledWith(
      "OrdersService",
      expect.anything(),
      expect.anything(),
    );
  });

  it("does not create dependee clients for duplicate self-service instances", () => {
    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "OrdersService";
    registry.serviceInstanceId = "orders-self-1";
    registry.remoteSignals.set("OrdersService", new Set(["global.orders.updated"]));

    const emit = vi.fn();
    const connected = registry.ensureDependeeClientForInstance(
      {
        uuid: "orders-self-2",
        serviceName: "OrdersService",
        numberOfRunningGraphs: 0,
        isPrimary: false,
        isActive: true,
        isNonResponsive: false,
        isBlocked: false,
        runtimeState: "healthy",
        acceptingWork: true,
        health: {},
        isFrontend: false,
        isDatabase: false,
        transports: [
          {
            uuid: "orders-self-transport",
            serviceInstanceId: "orders-self-2",
            role: "internal",
            origin: "http://orders.internal",
            protocols: ["rest"],
            securityProfile: null,
            authStrategy: null,
            deleted: false,
          },
        ],
      },
      emit,
      {},
    );

    expect(connected).toBe(false);
    expect(emit).not.toHaveBeenCalled();
  });

  it("retires older local instances that advertise the same routeable transport endpoint", async () => {
    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "OrdersService";
    registry.serviceInstanceId = "orders-new";

    registry.instances.set("OrdersService", [
      {
        uuid: "orders-old",
        serviceName: "OrdersService",
        numberOfRunningGraphs: 0,
        isPrimary: false,
        isActive: true,
        isNonResponsive: false,
        isBlocked: false,
        runtimeState: "healthy",
        acceptingWork: true,
        health: {},
        isFrontend: false,
        isDatabase: false,
        transports: [
          {
            uuid: "orders-old-internal",
            serviceInstanceId: "orders-old",
            role: "internal",
            origin: "http://orders.internal",
            protocols: ["rest"],
            securityProfile: null,
            authStrategy: null,
            deleted: false,
          },
        ],
      },
      {
        uuid: "orders-new",
        serviceName: "OrdersService",
        numberOfRunningGraphs: 0,
        isPrimary: false,
        isActive: true,
        isNonResponsive: false,
        isBlocked: false,
        runtimeState: "healthy",
        acceptingWork: true,
        health: {},
        isFrontend: false,
        isDatabase: false,
        transports: [],
      },
    ]);

    const handleTransportUpdateTask = Cadenza.get("Handle Transport Update");
    expect(handleTransportUpdateTask).toBeDefined();

    Cadenza.run(handleTransportUpdateTask!, {
      data: {
        uuid: "orders-new-internal",
        service_instance_id: "orders-new",
        role: "internal",
        origin: "http://orders.internal",
        protocols: ["rest"],
      },
    });

    await waitForCondition(
      () => (registry.instances.get("OrdersService") ?? []).length === 1,
      1_500,
    );

    expect(registry.instances.get("OrdersService")).toEqual([
      expect.objectContaining({
        uuid: "orders-new",
        transports: expect.arrayContaining([
          expect.objectContaining({
            uuid: "orders-new-internal",
            origin: "http://orders.internal",
          }),
        ]),
      }),
    ]);
  });

  it("retires older remote CadenzaDB instances for the same origin once the newer route is handshake-ready", async () => {
    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "TelemetryCollectorService";
    registry.serviceInstanceId = "telemetry-1";

    const retiredInstanceUpdates: Array<Record<string, unknown>> = [];
    const retiredTransportUpdates: Array<Record<string, unknown>> = [];
    const persistedInstanceUpdates: Array<Record<string, unknown>> = [];
    const persistedTransportUpdates: Array<Record<string, unknown>> = [];

    Cadenza.createEphemeralMetaTask(
      "Capture remote superseded instance updates",
      (ctx) => {
        retiredInstanceUpdates.push(ctx);
        return true;
      },
    ).doOn("global.meta.service_instance.updated");

    Cadenza.createEphemeralMetaTask(
      "Capture remote superseded transport updates",
      (ctx) => {
        retiredTransportUpdates.push(ctx);
        return true;
      },
    ).doOn("global.meta.service_instance_transport.updated");

    Cadenza.createEphemeralMetaTask(
      "Capture remote superseded instance persistence",
      (ctx) => {
        persistedInstanceUpdates.push(ctx);
        return true;
      },
    ).doOn("meta.service_registry.instance_update_requested");

    Cadenza.createEphemeralMetaTask(
      "Capture remote superseded transport persistence",
      (ctx) => {
        persistedTransportUpdates.push(ctx);
        return true;
      },
    ).doOn("meta.service_registry.transport_update_requested");

    registry.instances.set("TelemetryCollectorService", [
      {
        uuid: "telemetry-1",
        serviceName: "TelemetryCollectorService",
        numberOfRunningGraphs: 0,
        isPrimary: false,
        isActive: true,
        isNonResponsive: false,
        isBlocked: false,
        runtimeState: "healthy",
        acceptingWork: true,
        health: {},
        isFrontend: false,
        isDatabase: false,
        transports: [],
      },
    ]);
    registry.instances.set("CadenzaDB", [
      {
        uuid: "cadenza-db-old",
        serviceName: "CadenzaDB",
        numberOfRunningGraphs: 0,
        isPrimary: false,
        isActive: true,
        isNonResponsive: false,
        isBlocked: false,
        runtimeState: "healthy",
        acceptingWork: true,
        reportedAt: "2026-03-23T21:39:39.000Z",
        health: {},
        isFrontend: false,
        isDatabase: true,
        transports: [
          {
            uuid: "33333333-3333-4333-8333-333333333333",
            serviceInstanceId: "cadenza-db-old",
            role: "internal",
            origin: "http://cadenza-db-service:8080",
            protocols: ["rest", "socket"],
            securityProfile: null,
            authStrategy: null,
            deleted: false,
          },
        ],
      },
      {
        uuid: "cadenza-db-new",
        serviceName: "CadenzaDB",
        numberOfRunningGraphs: 0,
        isPrimary: false,
        isActive: true,
        isNonResponsive: false,
        isBlocked: false,
        runtimeState: "healthy",
        acceptingWork: true,
        reportedAt: "2026-03-23T21:47:24.000Z",
        health: {},
        isFrontend: false,
        isDatabase: true,
        transports: [
          {
            uuid: "44444444-4444-4444-8444-444444444444",
            serviceInstanceId: "cadenza-db-new",
            role: "internal",
            origin: "http://cadenza-db-service:8080",
            protocols: ["rest", "socket"],
            securityProfile: null,
            authStrategy: null,
            deleted: false,
          },
        ],
      },
    ]);

    registry.markTransportClientReady(registry.instances.get("CadenzaDB")[1], {
      uuid: "44444444-4444-4444-8444-444444444444",
      serviceInstanceId: "cadenza-db-new",
      role: "internal",
      origin: "http://cadenza-db-service:8080",
    });

    const handleTransportUpdateTask = Cadenza.get("Handle Transport Update");
    expect(handleTransportUpdateTask).toBeDefined();

    Cadenza.run(handleTransportUpdateTask!, {
      data: {
        uuid: "33333333-3333-4333-8333-333333333333",
        service_instance_id: "cadenza-db-old",
        role: "internal",
        origin: "http://cadenza-db-service:8080",
        protocols: ["rest", "socket"],
      },
    });
    Cadenza.run(handleTransportUpdateTask!, {
      data: {
        uuid: "44444444-4444-4444-8444-444444444444",
        service_instance_id: "cadenza-db-new",
        role: "internal",
        origin: "http://cadenza-db-service:8080",
        protocols: ["rest", "socket"],
      },
    });

    await waitForCondition(
      () => (registry.instances.get("CadenzaDB") ?? []).length === 1,
      1_500,
    );

    expect(registry.instances.get("CadenzaDB")).toEqual([
      expect.objectContaining({
        uuid: "cadenza-db-new",
      }),
    ]);
    expect(retiredInstanceUpdates).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          data: expect.objectContaining({
            is_active: false,
            is_non_responsive: false,
            deleted: false,
          }),
          filter: {
            uuid: "cadenza-db-old",
          },
        }),
      ]),
    );
    expect(retiredTransportUpdates).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          data: expect.objectContaining({
            deleted: true,
          }),
          filter: {
            uuid: "33333333-3333-4333-8333-333333333333",
          },
        }),
      ]),
    );
    const persistedInstanceUpdate = persistedInstanceUpdates.find(
      (ctx) => ctx.queryData?.filter?.uuid === "cadenza-db-old",
    );
    expect(persistedInstanceUpdate).toBeUndefined();
    expect(persistedTransportUpdates).toEqual([]);
  });

  it("authority defers same-origin instance retirement to cadenza-db canonicalization on transport update", async () => {
    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "CadenzaDB";
    registry.serviceInstanceId = "cadenza-db-self";

    const retiredInstanceUpdates: Array<Record<string, unknown>> = [];
    const retiredTransportUpdates: Array<Record<string, unknown>> = [];

    Cadenza.createEphemeralMetaTask(
      "Capture authority superseded instance updates",
      (ctx) => {
        retiredInstanceUpdates.push(ctx);
        return true;
      },
    ).doOn("global.meta.service_instance.updated");

    Cadenza.createEphemeralMetaTask(
      "Capture authority superseded transport updates",
      (ctx) => {
        retiredTransportUpdates.push(ctx);
        return true;
      },
    ).doOn("global.meta.service_instance_transport.updated");

    registry.instances.set("IotDbService", [
      {
        uuid: "iot-old",
        serviceName: "IotDbService",
        numberOfRunningGraphs: 0,
        isPrimary: false,
        isActive: true,
        isNonResponsive: false,
        isBlocked: false,
        runtimeState: "healthy",
        acceptingWork: true,
        reportedAt: "2026-03-25T10:00:00.000Z",
        health: {},
        isFrontend: false,
        isDatabase: true,
        transports: [
          {
            uuid: "11111111-1111-4111-8111-111111111111",
            serviceInstanceId: "iot-old",
            role: "internal",
            origin: "http://iot-db-service:3001",
            protocols: ["rest"],
            securityProfile: null,
            authStrategy: null,
            deleted: false,
          },
        ],
      },
      {
        uuid: "iot-new",
        serviceName: "IotDbService",
        numberOfRunningGraphs: 0,
        isPrimary: false,
        isActive: true,
        isNonResponsive: false,
        isBlocked: false,
        runtimeState: "healthy",
        acceptingWork: true,
        reportedAt: "2026-03-26T10:00:00.000Z",
        health: {},
        isFrontend: false,
        isDatabase: true,
        transports: [
          {
            uuid: "22222222-2222-4222-8222-222222222222",
            serviceInstanceId: "iot-new",
            role: "internal",
            origin: "http://iot-db-service:3001",
            protocols: ["rest"],
            securityProfile: null,
            authStrategy: null,
            deleted: false,
          },
        ],
      },
    ]);

    const handleTransportUpdateTask = Cadenza.get("Handle Transport Update");
    expect(handleTransportUpdateTask).toBeDefined();

    Cadenza.run(handleTransportUpdateTask!, {
      data: {
        uuid: "22222222-2222-4222-8222-222222222222",
        service_instance_id: "iot-new",
        role: "internal",
        origin: "http://iot-db-service:3001",
        protocols: ["rest"],
      },
    });

    await new Promise((resolve) => setTimeout(resolve, 25));
    await new Promise((resolve) => setTimeout(resolve, 25));

    expect(registry.instances.get("IotDbService")).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          uuid: "iot-old",
        }),
        expect.objectContaining({
          uuid: "iot-new",
        }),
      ]),
    );
    expect(retiredInstanceUpdates).toEqual([]);
    expect(retiredTransportUpdates).toEqual([]);
  });

  it("authority defers same-origin public transport retirement to cadenza-db canonicalization too", async () => {
    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "CadenzaDB";
    registry.serviceInstanceId = "cadenza-db-self";

    const retiredInstanceUpdates: Array<Record<string, unknown>> = [];
    const retiredTransportUpdates: Array<Record<string, unknown>> = [];

    Cadenza.createEphemeralMetaTask(
      "Capture authority public superseded instance updates",
      (ctx) => {
        retiredInstanceUpdates.push(ctx);
        return true;
      },
    ).doOn("global.meta.service_instance.updated");

    Cadenza.createEphemeralMetaTask(
      "Capture authority public superseded transport updates",
      (ctx) => {
        retiredTransportUpdates.push(ctx);
        return true;
      },
    ).doOn("global.meta.service_instance_transport.updated");

    registry.instances.set("IotDbService", [
      {
        uuid: "iot-old-public",
        serviceName: "IotDbService",
        numberOfRunningGraphs: 0,
        isPrimary: false,
        isActive: true,
        isNonResponsive: false,
        isBlocked: false,
        runtimeState: "healthy",
        acceptingWork: true,
        reportedAt: "2026-03-25T10:00:00.000Z",
        health: {},
        isFrontend: false,
        isDatabase: true,
        transports: [
          {
            uuid: "77777777-7777-4777-8777-777777777777",
            serviceInstanceId: "iot-old-public",
            role: "public",
            origin: "http://iot-db.localhost",
            protocols: ["rest"],
            securityProfile: null,
            authStrategy: null,
            deleted: false,
          },
        ],
      },
      {
        uuid: "iot-new-public",
        serviceName: "IotDbService",
        numberOfRunningGraphs: 0,
        isPrimary: false,
        isActive: true,
        isNonResponsive: false,
        isBlocked: false,
        runtimeState: "healthy",
        acceptingWork: true,
        reportedAt: "2026-03-26T10:00:00.000Z",
        health: {},
        isFrontend: false,
        isDatabase: true,
        transports: [
          {
            uuid: "88888888-8888-4888-8888-888888888888",
            serviceInstanceId: "iot-new-public",
            role: "public",
            origin: "http://iot-db.localhost",
            protocols: ["rest"],
            securityProfile: null,
            authStrategy: null,
            deleted: false,
          },
        ],
      },
    ]);

    const handleTransportUpdateTask = Cadenza.get("Handle Transport Update");
    expect(handleTransportUpdateTask).toBeDefined();

    Cadenza.run(handleTransportUpdateTask!, {
      data: {
        uuid: "88888888-8888-4888-8888-888888888888",
        service_instance_id: "iot-new-public",
        role: "public",
        origin: "http://iot-db.localhost",
        protocols: ["rest"],
      },
    });

    await new Promise((resolve) => setTimeout(resolve, 25));
    await new Promise((resolve) => setTimeout(resolve, 25));

    expect(registry.instances.get("IotDbService")).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          uuid: "iot-old-public",
        }),
        expect.objectContaining({
          uuid: "iot-new-public",
        }),
      ]),
    );
    expect(retiredInstanceUpdates).toEqual([]);
    expect(retiredTransportUpdates).toEqual([]);
  });

  it("does not clear a same-origin successor route when a stale deleted transport update arrives", async () => {
    const registry = ServiceRegistry.instance as any;
    const routeKey = buildRouteKey(
      "PredictorService",
      "internal",
      "http://predictor-b:3005",
    );

    registry.instances.set("PredictorService", [
      {
        uuid: "predictor-old",
        serviceName: "PredictorService",
        numberOfRunningGraphs: 0,
        isPrimary: false,
        isActive: false,
        isNonResponsive: true,
        isBlocked: false,
        runtimeState: "degraded",
        acceptingWork: false,
        reportedAt: "2026-03-25T10:00:00.000Z",
        health: {},
        isFrontend: false,
        isDatabase: false,
        transports: [
          {
            uuid: "transport-old",
            serviceInstanceId: "predictor-old",
            role: "internal",
            origin: "http://predictor-b:3005",
            protocols: ["rest"],
            securityProfile: null,
            authStrategy: null,
            deleted: false,
          },
        ],
        clientCreatedTransportIds: [],
        clientPendingTransportIds: [],
        clientReadyTransportIds: [],
      },
      {
        uuid: "predictor-new",
        serviceName: "PredictorService",
        numberOfRunningGraphs: 0,
        isPrimary: false,
        isActive: true,
        isNonResponsive: false,
        isBlocked: false,
        runtimeState: "healthy",
        acceptingWork: true,
        reportedAt: "2026-03-26T10:00:00.000Z",
        health: {},
        isFrontend: false,
        isDatabase: false,
        transports: [
          {
            uuid: "transport-new",
            serviceInstanceId: "predictor-new",
            role: "internal",
            origin: "http://predictor-b:3005",
            protocols: ["rest"],
            securityProfile: null,
            authStrategy: null,
            deleted: false,
          },
        ],
        clientCreatedTransportIds: [routeKey],
        clientPendingTransportIds: [],
        clientReadyTransportIds: [routeKey],
      },
    ]);

    registry.remoteRoutesByKey.set(routeKey, {
      key: routeKey,
      serviceName: "PredictorService",
      role: "internal",
      origin: "http://predictor-b:3005",
      protocols: ["rest"],
      serviceInstanceId: "predictor-new",
      serviceTransportId: "transport-new",
      generation: "predictor-new|transport-new",
      protocolState: {
        rest: {
          clientCreated: true,
          clientPending: false,
          clientReady: true,
        },
      },
      balancing: {
        selectionCount: 2,
        lastSelectedAt: Date.now(),
        lastSuccessAt: Date.now(),
        lastFailureAt: null,
        failurePenaltyUntil: null,
        activeDispatchTokens: new Set(),
      },
      lastUpdatedAt: Date.now(),
    });

    const handleTransportUpdateTask = Cadenza.get("Handle Transport Update");
    expect(handleTransportUpdateTask).toBeDefined();

    Cadenza.run(handleTransportUpdateTask!, {
      data: {
        uuid: "transport-old",
        service_instance_id: "predictor-old",
        role: "internal",
        origin: "http://predictor-b:3005",
        protocols: ["rest"],
        deleted: true,
      },
    });

    await waitForCondition(
      () =>
        registry.instances
          .get("PredictorService")
          ?.find((instance: any) => instance.uuid === "predictor-old")
          ?.transports?.length === 0,
      1_500,
    );

    expect(registry.remoteRoutesByKey.get(routeKey)).toMatchObject({
      serviceInstanceId: "predictor-new",
      serviceTransportId: "transport-new",
      protocolState: {
        rest: {
          clientCreated: true,
          clientPending: false,
          clientReady: true,
        },
      },
    });

    expect(
      registry.instances
        .get("PredictorService")
        ?.find((instance: any) => instance.uuid === "predictor-new"),
    ).toMatchObject({
      clientCreatedTransportIds: expect.arrayContaining([routeKey]),
      clientReadyTransportIds: expect.arrayContaining([routeKey]),
    });
  });

  it("does not persist synthetic bootstrap transport retirements", async () => {
    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "TelemetryCollectorService";
    registry.serviceInstanceId = "telemetry-1";

    const persistedTransportUpdates: Array<Record<string, unknown>> = [];

    Cadenza.createEphemeralMetaTask(
      "Capture synthetic transport retirement persistence",
      (ctx) => {
        persistedTransportUpdates.push(ctx);
        return true;
      },
    ).doOn("meta.service_registry.transport_update_requested");

    registry.instances.set("TelemetryCollectorService", [
      {
        uuid: "telemetry-1",
        serviceName: "TelemetryCollectorService",
        numberOfRunningGraphs: 0,
        isPrimary: true,
        isActive: true,
        isNonResponsive: false,
        isBlocked: false,
        runtimeState: "healthy",
        acceptingWork: true,
        reportedAt: new Date().toISOString(),
        health: {},
        isFrontend: false,
        isDatabase: false,
        transports: [],
      },
    ]);
    registry.instances.set("CadenzaDB", [
      {
        uuid: "00000000-0000-4000-8000-000000000001",
        serviceName: "CadenzaDB",
        numberOfRunningGraphs: 0,
        isPrimary: false,
        isActive: true,
        isNonResponsive: false,
        isBlocked: false,
        runtimeState: "healthy",
        acceptingWork: true,
        reportedAt: "2026-03-24T09:00:00.000Z",
        health: {},
        isFrontend: false,
        isDatabase: true,
        transports: [
          {
            uuid: "cadenza-db-internal-bootstrap",
            serviceInstanceId: "00000000-0000-4000-8000-000000000001",
            role: "internal",
            origin: "http://cadenza-db-service:8080",
            protocols: ["rest", "socket"],
            securityProfile: null,
            authStrategy: null,
            deleted: false,
          },
        ],
      },
      {
        uuid: "00000000-0000-4000-8000-000000000002",
        serviceName: "CadenzaDB",
        numberOfRunningGraphs: 0,
        isPrimary: false,
        isActive: true,
        isNonResponsive: false,
        isBlocked: false,
        runtimeState: "healthy",
        acceptingWork: true,
        reportedAt: "2026-03-24T10:00:00.000Z",
        health: {},
        isFrontend: false,
        isDatabase: true,
        transports: [
          {
            uuid: "22222222-2222-4222-8222-222222222222",
            serviceInstanceId: "00000000-0000-4000-8000-000000000002",
            role: "internal",
            origin: "http://cadenza-db-service:8080",
            protocols: ["rest", "socket"],
            securityProfile: null,
            authStrategy: null,
            deleted: false,
          },
        ],
        clientReadyTransportIds: ["22222222-2222-4222-8222-222222222222"],
      },
    ]);

    Cadenza.run(registry.handleTransportUpdateTask, {
      data: {
        uuid: "22222222-2222-4222-8222-222222222222",
        service_instance_id: "00000000-0000-4000-8000-000000000002",
        role: "internal",
        origin: "http://cadenza-db-service:8080",
        protocols: ["rest", "socket"],
      },
    });

    await new Promise((resolve) => setTimeout(resolve, 25));

    expect(
      persistedTransportUpdates.some(
        (ctx) =>
          ctx?.queryData?.filter?.uuid === "cadenza-db-internal-bootstrap",
      ),
    ).toBe(false);
  });

  it("flushes local runtime status updates to authority through the lightweight runtime-status intent", async () => {
    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "OrdersService";
    registry.serviceInstanceId = "orders-1";
    registry.connectsToCadenzaDB = true;
    registry.useSocket = false;
    registry.latestRuntimeMetricsSnapshot = {
      sampledAt: "2026-04-07T10:00:00.000Z",
      cpuUsage: 0.3,
      memoryUsage: 0.45,
      eventLoopLag: 9,
      rssBytes: 300,
      heapUsedBytes: 180,
      heapTotalBytes: 220,
      memoryLimitBytes: 700,
    };
    registry.instances.set("OrdersService", [
      {
        uuid: "orders-1",
        serviceName: "OrdersService",
        numberOfRunningGraphs: 2,
        isPrimary: false,
        isActive: true,
        isNonResponsive: false,
        isBlocked: false,
        runtimeState: "healthy",
        acceptingWork: true,
        reportedAt: "2026-03-24T10:00:00.000Z",
        health: {
          oversizedNestedBlob: {
            diagnostics: {
              shouldNot: "leave the local service",
            },
          },
        },
        isFrontend: false,
        isDatabase: false,
        transports: [
          {
            uuid: "22222222-2222-4222-8222-222222222222",
            serviceInstanceId: "orders-1",
            role: "internal",
            origin: "http://orders:5000",
            protocols: ["rest"],
            securityProfile: null,
            authStrategy: null,
            deleted: false,
          },
        ],
      },
    ]);

    let authorityReport: Record<string, unknown> | null = null;
    let durableUpdate: Record<string, unknown> | null = null;
    let peerRuntimeStatus: Record<string, unknown> | null = null;

    Cadenza.createMetaTask(
      "Capture authority runtime status report",
      (ctx) => {
        authorityReport = { ...ctx };
        return {
          applied: true,
          serviceName: ctx.serviceName,
          serviceInstanceId: ctx.serviceInstanceId,
        };
      },
      "",
      { register: false },
    ).respondsTo(AUTHORITY_RUNTIME_STATUS_REPORT_INTENT);

    Cadenza.createEphemeralMetaTask(
      "Capture unexpected durable runtime status update",
      (ctx) => {
        durableUpdate = { ...ctx };
        return ctx;
      },
      "",
      { register: false },
    ).doOn("meta.service_registry.instance_update_requested");

    Cadenza.createEphemeralMetaTask(
      "Capture peer runtime status update",
      (ctx) => {
        peerRuntimeStatus = { ...ctx };
        return ctx;
      },
      "",
      { register: false },
    ).doOn("meta.service.updated");

    Cadenza.emit("meta.service_registry.runtime_status_broadcast_requested", {
      force: true,
      reason: "test",
    });

    await waitForCondition(() => authorityReport !== null, 1_500);

    expect(authorityReport).toMatchObject({
      serviceName: "OrdersService",
      serviceInstanceId: "orders-1",
      cpuUsage: 0.3,
      memoryUsage: 0.45,
      eventLoopLag: 9,
      isActive: true,
      isNonResponsive: false,
      isBlocked: false,
      health: {
        runtimeMetrics: {
          sampledAt: "2026-04-07T10:00:00.000Z",
          rssBytes: 300,
          heapUsedBytes: 180,
          heapTotalBytes: 220,
          memoryLimitBytes: 700,
        },
      },
    });
    expect(authorityReport?.health).not.toHaveProperty("oversizedNestedBlob");
    expect(durableUpdate).toBeNull();
    expect(peerRuntimeStatus).toMatchObject({
      serviceName: "OrdersService",
      serviceInstanceId: "orders-1",
      cpuUsage: 0.3,
      memoryUsage: 0.45,
      eventLoopLag: 9,
    });
    expect(peerRuntimeStatus?.health).toBeUndefined();
  });

  it("broadcasts significant runtime metric changes to peers without increasing authority flush cadence", async () => {
    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "OrdersService";
    registry.serviceInstanceId = "orders-1";
    registry.connectsToCadenzaDB = true;
    registry.useSocket = false;
    registry.latestRuntimeMetricsSnapshot = {
      sampledAt: "2026-04-07T10:00:00.000Z",
      cpuUsage: 0.1,
      memoryUsage: 0.2,
      eventLoopLag: 9,
      rssBytes: 300,
      heapUsedBytes: 180,
      heapTotalBytes: 220,
      memoryLimitBytes: 700,
    };
    registry.instances.set("OrdersService", [
      {
        uuid: "orders-1",
        serviceName: "OrdersService",
        numberOfRunningGraphs: 2,
        isPrimary: false,
        isActive: true,
        isNonResponsive: false,
        isBlocked: false,
        runtimeState: "healthy",
        acceptingWork: true,
        reportedAt: "2026-03-24T10:00:00.000Z",
        health: {},
        isFrontend: false,
        isDatabase: false,
        transports: [
          {
            uuid: "22222222-2222-4222-8222-222222222222",
            serviceInstanceId: "orders-1",
            role: "internal",
            origin: "http://orders:5000",
            protocols: ["rest"],
            securityProfile: null,
            authStrategy: null,
            deleted: false,
          },
        ],
      },
    ]);

    const authorityReports: Record<string, unknown>[] = [];
    const peerRuntimeUpdates: Record<string, unknown>[] = [];

    Cadenza.createMetaTask(
      "Capture authority runtime status report for metric delta test",
      (ctx) => {
        authorityReports.push({ ...ctx });
        return {
          applied: true,
          serviceName: ctx.serviceName,
          serviceInstanceId: ctx.serviceInstanceId,
        };
      },
      "",
      { register: false },
    ).respondsTo(AUTHORITY_RUNTIME_STATUS_REPORT_INTENT);

    Cadenza.createMetaTask(
      "Capture peer runtime status metric delta update",
      (ctx) => {
        peerRuntimeUpdates.push({ ...ctx });
        return ctx;
      },
      "",
      { register: false },
    ).doOn("meta.service.updated");

    Cadenza.emit("meta.service_registry.runtime_status_broadcast_requested", {
      force: true,
      reason: "test",
    });

    await waitForCondition(() => authorityReports.length === 1, 1_500);
    await waitForCondition(() => peerRuntimeUpdates.length === 1, 1_500);

    registry.latestRuntimeMetricsSnapshot = {
      sampledAt: "2026-04-07T10:00:05.000Z",
      cpuUsage: 0.18,
      memoryUsage: 0.31,
      eventLoopLag: 18,
      rssBytes: 420,
      heapUsedBytes: 250,
      heapTotalBytes: 300,
      memoryLimitBytes: 700,
    };

    Cadenza.emit("meta.service_registry.runtime_status.peer_update_requested", {
      reason: "runtime_metrics_sample",
      detailLevel: "minimal",
      peerOnly: true,
    });

    await waitForCondition(() => peerRuntimeUpdates.length === 2, 1_500);

    expect(authorityReports).toHaveLength(1);
    expect(peerRuntimeUpdates.at(-1)).toMatchObject({
      serviceName: "OrdersService",
      serviceInstanceId: "orders-1",
      cpuUsage: 0.18,
      memoryUsage: 0.31,
      eventLoopLag: 18,
      health: undefined,
    });
  });

  it("refreshes stale REST-only dependee runtime metrics without waiting for heartbeat fallback", async () => {
    const registry = ServiceRegistry.instance as any;
    const now = Date.now();

    registry.instances.set("PredictorService", [
      {
        uuid: "predictor-b-1",
        serviceName: "PredictorService",
        numberOfRunningGraphs: 2,
        isPrimary: false,
        isActive: true,
        isNonResponsive: false,
        isBlocked: false,
        runtimeState: "healthy",
        acceptingWork: true,
        reportedAt: new Date(now - 30_000).toISOString(),
        health: {
          cpuUsage: 0.02,
          memoryUsage: 0.05,
          eventLoopLag: 12,
        },
        isFrontend: false,
        isDatabase: false,
        transports: [
          {
            uuid: "predictor-b-transport",
            serviceInstanceId: "predictor-b-1",
            role: "internal",
            origin: "http://predictor-b:3005",
            protocols: ["rest"],
            securityProfile: null,
            authStrategy: null,
            deleted: false,
          },
        ],
      },
    ]);
    registry.dependeesByService.set(
      "PredictorService",
      new Set(["predictor-b-1"]),
    );
    registry.lastHeartbeatAtByInstance.set("predictor-b-1", now - 30_000);
    registry.missedHeartbeatsByInstance.set("predictor-b-1", 0);

    const fetchSpy = vi
      .spyOn(globalThis, "fetch")
      .mockResolvedValue({
        ok: true,
        json: async () => ({
          serviceName: "PredictorService",
          serviceInstanceId: "predictor-b-1",
          reportedAt: "2026-04-07T10:00:10.000Z",
          state: "overloaded",
          acceptingWork: true,
          numberOfRunningGraphs: 800,
          cpuUsage: 0.72,
          memoryUsage: 0.61,
          eventLoopLag: 48,
          isActive: true,
          isNonResponsive: false,
          isBlocked: false,
          health: {
            runtimeMetrics: {
              sampledAt: "2026-04-07T10:00:10.000Z",
              cpuUsage: 0.72,
              memoryUsage: 0.61,
              eventLoopLag: 48,
              rssBytes: 2_000,
              heapUsedBytes: 900,
              heapTotalBytes: 1_100,
              memoryLimitBytes: 4_000,
            },
          },
        }),
      } as Response);

    Cadenza.emit("meta.service_registry.runtime_status.rest_refresh_tick", {});

    await waitForCondition(() => {
      const instance = registry.instances.get("PredictorService")?.[0];
      return instance?.health?.cpuUsage === 0.72;
    }, 1_500);

    expect(fetchSpy).toHaveBeenCalledWith("http://predictor-b:3005/status", {
      method: "GET",
      signal: expect.any(Object),
    });
    expect(registry.instances.get("PredictorService")?.[0]).toMatchObject({
      runtimeState: "overloaded",
      numberOfRunningGraphs: 800,
      health: {
        cpuUsage: 0.72,
        memoryUsage: 0.61,
        eventLoopLag: 48,
        runtimeMetrics: {
          rssBytes: 2_000,
          heapUsedBytes: 900,
          heapTotalBytes: 1_100,
          memoryLimitBytes: 4_000,
        },
      },
    });
  });

  it("prefers lower runtime metrics before graph count when balancing equivalent routes", () => {
    const registry = ServiceRegistry.instance as any;

    const cooler = {
      availability: "available",
      runtimeState: "healthy",
      acceptingWork: true,
      inFlightDelegations: 0,
      cpuUsage: 0.2,
      memoryUsage: 0.3,
      eventLoopLag: 5,
      numberOfRunningGraphs: 12,
      selectionCount: 0,
      lastSelectedAt: 0,
      lastSuccessAt: null,
      lastFailureAt: null,
      failurePenaltyUntil: null,
    };
    const hotterButShorterQueue = {
      availability: "available",
      runtimeState: "healthy",
      acceptingWork: true,
      inFlightDelegations: 0,
      cpuUsage: 0.8,
      memoryUsage: 0.7,
      eventLoopLag: 20,
      numberOfRunningGraphs: 1,
      selectionCount: 0,
      lastSelectedAt: 0,
      lastSuccessAt: null,
      lastFailureAt: null,
      failurePenaltyUntil: null,
    };

    expect(
      registry.compareRouteBalancingSnapshots(cooler, hotterButShorterQueue),
    ).toBeLessThan(0);
  });

  it("does not penalize routes with missing runtime metrics during balancing", () => {
    const registry = ServiceRegistry.instance as any;

    const withoutMetrics = {
      availability: "available",
      runtimeState: "healthy",
      acceptingWork: true,
      inFlightDelegations: 0,
      cpuUsage: null,
      memoryUsage: null,
      eventLoopLag: null,
      numberOfRunningGraphs: 4,
      selectionCount: 0,
      lastSelectedAt: 0,
      lastSuccessAt: null,
      lastFailureAt: null,
      failurePenaltyUntil: null,
    };
    const withMetrics = {
      ...withoutMetrics,
      cpuUsage: 0.9,
      memoryUsage: 0.9,
      eventLoopLag: 30,
    };

    expect(
      registry.compareRouteBalancingSnapshots(withoutMetrics, withMetrics),
    ).toBe(0);
  });

  it("ignores tiny cpu deltas when other runtime metrics are materially worse", () => {
    const registry = ServiceRegistry.instance as any;

    const slightlyLowerCpuButHotter = {
      availability: "available",
      runtimeState: "overloaded",
      acceptingWork: true,
      inFlightDelegations: 0,
      cpuUsage: 0.01,
      memoryUsage: 0.12,
      eventLoopLag: 48,
      numberOfRunningGraphs: 1200,
      selectionCount: 1,
      lastSelectedAt: 100,
      lastSuccessAt: 100,
      lastFailureAt: null,
      failurePenaltyUntil: null,
    };
    const slightlyHigherCpuButCooler = {
      availability: "available",
      runtimeState: "overloaded",
      acceptingWork: true,
      inFlightDelegations: 0,
      cpuUsage: 0.03,
      memoryUsage: 0.05,
      eventLoopLag: 36,
      numberOfRunningGraphs: 120,
      selectionCount: 0,
      lastSelectedAt: null,
      lastSuccessAt: null,
      lastFailureAt: null,
      failurePenaltyUntil: null,
    };

    expect(
      registry.compareRouteBalancingSnapshots(
        slightlyLowerCpuButHotter,
        slightlyHigherCpuButCooler,
      ),
    ).toBeGreaterThan(0);
  });

  it("does not retire remote CadenzaDB duplicates from gathered sync before any route is handshake-ready", async () => {
    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "TelemetryCollectorService";
    registry.serviceInstanceId = "telemetry-1";

    registry.instances.set("TelemetryCollectorService", [
      {
        uuid: "telemetry-1",
        serviceName: "TelemetryCollectorService",
        numberOfRunningGraphs: 0,
        isPrimary: false,
        isActive: true,
        isNonResponsive: false,
        isBlocked: false,
        runtimeState: "healthy",
        acceptingWork: true,
        health: {},
        isFrontend: false,
        isDatabase: false,
        transports: [],
      },
    ]);
    registry.instances.set("CadenzaDB", [
      {
        uuid: "cadenza-db-old",
        serviceName: "CadenzaDB",
        numberOfRunningGraphs: 0,
        isPrimary: false,
        isActive: true,
        isNonResponsive: false,
        isBlocked: false,
        runtimeState: "healthy",
        acceptingWork: true,
        reportedAt: "2026-03-23T21:39:39.000Z",
        health: {},
        isFrontend: false,
        isDatabase: true,
        transports: [
          {
            uuid: "cadenza-db-old-internal",
            serviceInstanceId: "cadenza-db-old",
            role: "internal",
            origin: "http://cadenza-db-service:8080",
            protocols: ["rest", "socket"],
            securityProfile: null,
            authStrategy: null,
            deleted: false,
          },
        ],
      },
    ]);

    const handleInstanceUpdateTask = Cadenza.get("Handle Instance Update");
    expect(handleInstanceUpdateTask).toBeDefined();

    Cadenza.run(handleInstanceUpdateTask!, {
      serviceInstance: {
        uuid: "cadenza-db-new",
        serviceName: "CadenzaDB",
        numberOfRunningGraphs: 0,
        isPrimary: false,
        isActive: true,
        isNonResponsive: false,
        isBlocked: false,
        runtimeState: "healthy",
        acceptingWork: true,
        reportedAt: "2026-03-23T21:47:24.000Z",
        health: {},
        isFrontend: false,
        isDatabase: true,
        transports: [
          {
            uuid: "cadenza-db-new-internal",
            serviceInstanceId: "cadenza-db-new",
            role: "internal",
            origin: "http://cadenza-db-service:8080",
            protocols: ["rest", "socket"],
            securityProfile: null,
            authStrategy: null,
            deleted: false,
          },
        ],
      },
    });

    await new Promise((resolve) => setTimeout(resolve, 50));

    expect(registry.instances.get("CadenzaDB")).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          uuid: "cadenza-db-old",
        }),
        expect.objectContaining({
          uuid: "cadenza-db-new",
          transports: expect.arrayContaining([
            expect.objectContaining({
              uuid: "cadenza-db-new-internal",
              origin: "http://cadenza-db-service:8080",
            }),
          ]),
        }),
      ]),
    );
  });

  it("ignores deleted service instances when hydrating gathered sync service lists", async () => {
    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "TelemetryCollectorService";
    registry.serviceInstanceId = "telemetry-1";

    const splitServiceInstancesTask = Cadenza.get("Split service instances");
    expect(splitServiceInstancesTask).toBeDefined();

    Cadenza.run(splitServiceInstancesTask!, {
      service_instances: [
        {
          uuid: "cadenza-db-deleted",
          service_name: "CadenzaDB",
          is_active: true,
          is_non_responsive: false,
          is_blocked: false,
          is_frontend: false,
          is_database: true,
          deleted: true,
          health: {},
        },
        {
          uuid: "cadenza-db-live",
          service_name: "CadenzaDB",
          is_active: true,
          is_non_responsive: false,
          is_blocked: false,
          is_frontend: false,
          is_database: true,
          health: {},
        },
      ],
      service_instance_transports: [
        {
          uuid: "cadenza-db-deleted-internal",
          service_instance_id: "cadenza-db-deleted",
          role: "internal",
          origin: "http://cadenza-db-service:8080",
          protocols: ["rest", "socket"],
          deleted: true,
        },
        {
          uuid: "cadenza-db-live-internal",
          service_instance_id: "cadenza-db-live",
          role: "internal",
          origin: "http://cadenza-db-service:8080",
          protocols: ["rest", "socket"],
        },
      ],
    });

    await waitForCondition(
      () => (registry.instances.get("CadenzaDB") ?? []).length === 1,
      1_500,
    );

    expect(registry.instances.get("CadenzaDB")).toEqual([
      expect.objectContaining({
        uuid: "cadenza-db-live",
        transports: expect.arrayContaining([
          expect.objectContaining({
            uuid: "cadenza-db-live-internal",
          }),
        ]),
      }),
    ]);
  });

  it("persists each yielded transport registration without collapsing them into one resolver execution", async () => {
    const insertedTransportRows: Array<Record<string, unknown>> = [];

    Cadenza.createMetaTask("Insert service_instance_transport", (ctx) => {
      insertedTransportRows.push({
        ...(ctx.queryData?.data ?? {}),
      });
      return ctx;
    });

    const prepareTransportsTask = Cadenza.get("Prepare service transport inserts");
    expect(prepareTransportsTask).toBeDefined();

    Cadenza.run(prepareTransportsTask!, {
      __serviceInstanceId: "orders-3",
      __serviceName: "OrdersService",
      __transportData: [
        {
          uuid: "transport-a",
          role: "internal",
          origin: "http://orders.internal",
          protocols: ["rest"],
        },
        {
          uuid: "transport-b",
          role: "public",
          origin: "http://orders.localhost",
          protocols: ["rest"],
        },
      ],
    });

    await waitForCondition(() => insertedTransportRows.length === 2, 1_500);

    expect(insertedTransportRows).toEqual([
      expect.objectContaining({
        uuid: "transport-a",
        role: "internal",
        service_instance_id: "orders-3",
        origin: "http://orders.internal",
      }),
      expect.objectContaining({
        uuid: "transport-b",
        role: "public",
        service_instance_id: "orders-3",
        origin: "http://orders.localhost",
      }),
    ]);
    expect(insertedTransportRows[0]).not.toHaveProperty("name");
    expect(insertedTransportRows[1]).not.toHaveProperty("name");
  });

  it("does not leak service insert onConflict settings into service_instance inserts", async () => {
    const insertPayloads: Array<Record<string, unknown>> = [];

    Cadenza.createMetaTask("Insert service_instance", (ctx) => {
      insertPayloads.push({
        ...(ctx.queryData ?? {}),
      });
      return {
        ...ctx,
        uuid: ctx.queryData?.data?.uuid ?? ctx.data?.uuid,
      };
    });

    const resolveInsertTask = Cadenza.get(
      "Resolve service registry insert for service_instance",
    );
    expect(resolveInsertTask).toBeDefined();

    Cadenza.run(resolveInsertTask!, {
      queryData: {
        onConflict: {
          target: ["name"],
          action: {
            do: "nothing",
          },
        },
      },
      data: {
        uuid: "orders-instance-1",
        process_pid: 42,
        service_name: "OrdersService",
        is_active: true,
      },
    });

    await waitForCondition(() => insertPayloads.length === 1, 1_500);

    expect(insertPayloads[0]).toMatchObject({
      data: expect.objectContaining({
        uuid: "orders-instance-1",
        service_name: "OrdersService",
      }),
      onConflict: expect.objectContaining({
        target: ["uuid"],
      }),
    });
    expect(insertPayloads[0].onConflict).not.toMatchObject({
      target: ["name"],
    });
  });

  it("skips no-op updates for identical service_instance_transport upserts", async () => {
    const insertPayloads: Array<Record<string, unknown>> = [];

    Cadenza.createMetaTask("Insert service_instance_transport", (ctx) => {
      insertPayloads.push({
        ...(ctx.queryData ?? {}),
      });
      return {
        ...ctx,
        uuid:
          ctx.queryData?.data?.uuid ??
          ctx.data?.uuid ??
          "transport-1",
      };
    });

    const resolveInsertTask = Cadenza.get(
      "Resolve service registry insert for service_instance_transport",
    );
    expect(resolveInsertTask).toBeDefined();

    Cadenza.run(resolveInsertTask!, {
      data: {
        uuid: "transport-1",
        service_instance_id: "orders-instance-1",
        role: "internal",
        origin: "http://orders.internal",
        protocols: ["rest", "socket"],
        security_profile: "low",
        auth_strategy: null,
      },
    });

    await waitForCondition(() => insertPayloads.length === 1, 1_500);

    expect(insertPayloads[0]).toMatchObject({
      data: expect.objectContaining({
        service_instance_id: "orders-instance-1",
        role: "internal",
        origin: "http://orders.internal",
      }),
      onConflict: expect.objectContaining({
        target: ["service_instance_id", "role", "origin"],
        action: expect.objectContaining({
          do: "update",
          where: expect.stringContaining(
            "service_instance_transport.protocols IS DISTINCT FROM excluded.protocols",
          ),
        }),
      }),
    });
  });

  it("keeps transport insert resolvers alive until the matching resolver signal arrives", async () => {
    const resolveInsertTask = Cadenza.get(
      "Resolve service registry insert for service_instance_transport",
    );
    expect(resolveInsertTask).toBeDefined();

    const insertedContexts: Array<Record<string, unknown>> = [];
    const resolvedContexts: Array<Record<string, unknown>> = [];
    let releaseInsert: (() => void) | null = null;

    const localInsertTask = Cadenza.createMetaTask(
      "Test local service_instance_transport insert",
      async (ctx) => {
        insertedContexts.push({ ...(ctx as Record<string, unknown>) });
        await new Promise<void>((resolve) => {
          releaseInsert = resolve;
        });

        return {
          ...ctx,
          uuid:
            (ctx as any).queryData?.data?.uuid ??
            (ctx as any).data?.uuid ??
            "transport-1",
        };
      },
      "",
      {
        register: false,
        isHidden: true,
      },
    );

    const getLocalInsertTaskSpy = vi.spyOn(Cadenza, "getLocalCadenzaDBInsertTask");
    getLocalInsertTaskSpy.mockImplementation((tableName: string) => {
      if (tableName === "service_instance_transport") {
        return localInsertTask;
      }

      return undefined;
    });

    const probeTask = Cadenza.createMetaTask(
      "Capture resolved transport insert",
      (ctx) => {
        resolvedContexts.push({ ...(ctx as Record<string, unknown>) });
        return true;
      },
      "",
      {
        register: false,
        isHidden: true,
      },
    );

    resolveInsertTask!.then(probeTask);

    Cadenza.run(resolveInsertTask!, {
      data: {
        uuid: "transport-1",
        service_instance_id: "orders-instance-1",
        origin: "http://orders.localhost",
        role: "public",
      },
      queryData: {
        data: {
          uuid: "transport-1",
          service_instance_id: "orders-instance-1",
          origin: "http://orders.localhost",
          role: "public",
        },
      },
    });

    await waitForCondition(() => insertedContexts.length === 1, 1_500);

    Cadenza.signalBroker.emit(
      "meta.service_registry.insert_execution_resolved:service_instance_transport",
      {
        __resolverRequestId: "resolver-other-1",
        queryData: {
          data: {
            uuid: "transport-other",
          },
        },
      },
    );

    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(resolvedContexts).toHaveLength(0);

    releaseInsert?.();

    await waitForCondition(() => resolvedContexts.length === 1, 1_500);
    expect(resolvedContexts[0]).toMatchObject({
      uuid: "transport-1",
    });
  });

  it("registers rest-only transports when socket serving is disabled", async () => {
    RestController.instance;

    const registrationPromise = new Promise<any>((resolve) => {
      Cadenza.createEphemeralMetaTask(
        "Capture rest-only instance registration",
        (ctx) => {
          resolve(ctx);
          return true;
        },
        "Captures service registration payloads for protocol assertions.",
        { register: false },
      ).doOn("meta.service_registry.instance_registration_requested");
    });

    Cadenza.emit("meta.service_registry.service_inserted", {
      data: {
        name: "OrdersService",
        description: "Orders service with REST-only transport exposure.",
        displayName: "",
        isMeta: false,
      },
      __serviceName: "OrdersService",
      __serviceInstanceId: "orders-rest-only",
      __port: 0,
      __useSocket: false,
      __networkMode: "dev",
      __securityProfile: "medium",
      __declaredTransports: [
        {
          uuid: "orders-public",
          role: "public",
          origin: "http://orders.localhost",
          protocols: ["rest"],
        },
      ],
      __isFrontend: false,
      __retryCount: 3,
    });

    const registrationContext = await registrationPromise;
    const transports = registrationContext.__transportData as Array<Record<string, unknown>>;

    expect(transports).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          role: "internal",
          protocols: ["rest"],
        }),
        expect.objectContaining({
          role: "public",
          origin: "http://orders.localhost",
          protocols: ["rest"],
        }),
      ]),
    );
  });

  it("does not restart the REST bootstrap routine from its own instance-registration signal", async () => {
    RestController.instance;

    const createServerSpy = vi
      .spyOn(http, "createServer")
      .mockImplementation((() => {
        const server = {
          listen: (_port: number, callback?: () => void) => {
            callback?.();
            return server;
          },
          address: () => ({
            address: "::",
            port: 3002,
          }),
          close: () => undefined,
          on: () => server,
        };

        return server as any;
      }) as any);

    const registrationContexts: Array<Record<string, unknown>> = [];
    Cadenza.createMetaTask(
      "Capture rest bootstrap registration loop",
      (ctx) => {
        registrationContexts.push(ctx);
        return true;
      },
      "Captures rest bootstrap registration emissions for loop assertions.",
      {
        register: false,
        isHidden: true,
      },
    ).doOn("meta.service_registry.instance_registration_requested");

    Cadenza.emit("meta.service_registry.service_inserted", {
      data: {
        name: "ScheduledRunnerService",
        description: "Runner service",
        displayName: "",
        isMeta: false,
      },
      __serviceName: "ScheduledRunnerService",
      __serviceInstanceId: "scheduled-runner-1",
      __port: 3002,
      __useSocket: true,
      __networkMode: "dev",
      __securityProfile: "medium",
      __declaredTransports: [],
      __isFrontend: false,
      __retryCount: 3,
    });

    await waitForCondition(() => registrationContexts.length >= 1, 1_500);
    await new Promise((resolve) => setTimeout(resolve, 25));

    expect(createServerSpy).toHaveBeenCalledTimes(1);
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

  it("preserves transport metadata through resolved service_instance insert flow", async () => {
    const registrations: Array<Record<string, unknown>> = [];
    const registry = ServiceRegistry.instance as any;

    Cadenza.createMetaTask("Capture resolved transport registration request", (ctx) => {
      registrations.push(ctx.data);
      return true;
    }).doOn("meta.service_registry.transport_registration_requested");

    Cadenza.createMetaTask("Insert service_instance", (ctx) => {
      return {
        ...ctx,
        uuid:
          (ctx.queryData as any)?.data?.uuid ??
          (ctx.data as any)?.uuid ??
          "missing-uuid",
        data: (ctx.queryData as any)?.data ?? ctx.data,
      };
    });

    Cadenza.run(registry.insertServiceInstanceTask, {
      data: {
        uuid: "orders-transport-flow",
        process_pid: 1,
        service_name: "OrdersService",
        is_active: true,
      },
      __transportData: [
        {
          uuid: "transport-flow-public",
          role: "public",
          origin: "http://orders-transport-flow.localhost",
          protocols: ["rest", "socket"],
        },
      ],
      __useSocket: true,
      __retryCount: 3,
      __isFrontend: false,
    });

    await waitForCondition(() => registrations.length === 1, 1_500);

    expect(registrations[0]).toEqual(
      expect.objectContaining({
        uuid: "transport-flow-public",
        role: "public",
        origin: "http://orders-transport-flow.localhost",
        service_instance_id: "orders-transport-flow",
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

  it("reschedules local CadenzaDB service_instance inserts until the generated insert task exists", async () => {
    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "CadenzaDB";
    registry.serviceInstanceId = "cadenza-db-1";

    const scheduleSpy = vi.spyOn(Cadenza, "schedule");
    const remoteInsertExecutions: Array<Record<string, unknown>> = [];

    Cadenza.createEphemeralMetaTask(
      "Capture remote service instance insert execution request",
      (ctx) => {
        remoteInsertExecutions.push(ctx);
        return true;
      },
    ).doOn("meta.service_registry.insert_execution_requested:service_instance:remote");

    expect(Cadenza.get("Insert service_instance")).toBeUndefined();

    const result = await registry.insertServiceInstanceTask.taskFunction(
      {
        data: {
          uuid: "cadenza-db-1",
          process_pid: 1,
          service_name: "CadenzaDB",
          is_active: true,
        },
        __serviceName: "CadenzaDB",
        __serviceInstanceId: "cadenza-db-1",
      },
      Cadenza.emit.bind(Cadenza),
    );

    expect(result).toBe(false);
    expect(scheduleSpy).toHaveBeenCalledWith(
      "meta.service_registry.instance_registration_requested",
      expect.objectContaining({
        __serviceName: "CadenzaDB",
        __serviceInstanceId: "cadenza-db-1",
      }),
      250,
    );
    expect(remoteInsertExecutions).toEqual([]);
  });

  it("reconstructs service_instance registration payloads before insert resolution", () => {
    const prepareTask = Cadenza.get("Prepare service instance registration") as any;

    expect(prepareTask).toBeDefined();

    const result = prepareTask.taskFunction({
      __registrationData: {
        name: "OrdersService",
        description: "Orders",
        display_name: "Orders",
        is_meta: false,
      },
      __serviceName: "OrdersService",
      queryData: {
        data: {
          uuid: "orders-4",
          process_pid: 42,
          service_name: "OrdersService",
          is_active: true,
        },
      },
    });

    expect(result).toMatchObject({
      data: {
        uuid: "orders-4",
        process_pid: 42,
        service_name: "OrdersService",
        is_active: true,
      },
      __registrationData: {
        uuid: "orders-4",
        process_pid: 42,
        service_name: "OrdersService",
        is_active: true,
      },
      __serviceName: "OrdersService",
      __serviceInstanceId: "orders-4",
    });
    expect(result.data).not.toMatchObject({
      name: "OrdersService",
      description: "Orders",
      display_name: "Orders",
      is_meta: false,
    });
  });

  it("does not reschedule empty service_instance failure payloads", async () => {
    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "OrdersService";

    const scheduleSpy = vi
      .spyOn(Cadenza, "schedule")
      .mockImplementation(() => undefined as any);

    Cadenza.emit("meta.service_registry.instance_insertion_failed", {
      __serviceName: "OrdersService",
      __error: "No data provided for insert",
    });

    await new Promise((resolve) => setTimeout(resolve, 20));

    expect(scheduleSpy).not.toHaveBeenCalled();
  });

  it("reschedules service_instance failures with reconstructed registration payloads", async () => {
    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "OrdersService";

    const scheduleSpy = vi
      .spyOn(Cadenza, "schedule")
      .mockImplementation(() => undefined as any);

    Cadenza.emit("meta.service_registry.instance_insertion_failed", {
      __serviceName: "OrdersService",
      queryData: {
        data: {
          uuid: "orders-5",
          process_pid: 99,
          service_name: "OrdersService",
          is_active: true,
        },
      },
    });

    await waitForCondition(() => scheduleSpy.mock.calls.length === 1, 1_000);

    expect(scheduleSpy).toHaveBeenCalledWith(
      "meta.service_registry.instance_registration_requested",
      expect.objectContaining({
        data: expect.objectContaining({
          uuid: "orders-5",
          process_pid: 99,
          service_name: "OrdersService",
          is_active: true,
        }),
        __registrationData: expect.objectContaining({
          uuid: "orders-5",
          process_pid: 99,
          service_name: "OrdersService",
          is_active: true,
        }),
        __serviceName: "OrdersService",
        __serviceInstanceId: "orders-5",
      }),
      5000,
    );
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

  it("strips stale delegation result snapshots before service_instance inserts", async () => {
    resetRuntimeState();

    const capturedContexts: Array<Record<string, unknown>> = [];

    vi.spyOn(Cadenza, "createCadenzaDBInsertTask").mockImplementation(
      () =>
        Cadenza.createMetaTask(
          "Mock remote service_instance insert",
          (ctx) => {
            capturedContexts.push({
              data: ctx.data,
              batch: ctx.batch,
              onConflict: ctx.onConflict,
              queryData: ctx.queryData,
            });

            return {
              ...ctx,
              uuid:
                (ctx.data as AnyObject | undefined)?.uuid ??
                (ctx.queryData as AnyObject | undefined)?.data?.uuid ??
                "missing-uuid",
            };
          },
          "Captures remote service_instance insert payloads for resolver tests.",
          {
            register: false,
            isHidden: true,
          },
        ),
    );

    Cadenza.bootstrap();
    Cadenza.setMode("production");

    const registry = ServiceRegistry.instance as any;
    const result = await registry.insertServiceInstanceTask.taskFunction(
      {
        __status: "success",
        __delegationRequestContext: {
          __serviceName: "DemoFrontend",
          __serviceInstanceId: "frontend-1",
          data: {
            name: "DemoFrontend",
          },
          queryData: {
            onConflict: {
              target: ["name"],
              action: {
                do: "nothing",
              },
            },
            data: {
              name: "DemoFrontend",
            },
          },
        },
        onConflict: {
          target: ["name"],
          action: {
            do: "nothing",
          },
        },
        data: {
          uuid: "frontend-1",
          process_pid: 123,
          service_name: "DemoFrontend",
          is_active: true,
        },
      },
      Cadenza.emit.bind(Cadenza),
    );

    expect(capturedContexts).toEqual([
      expect.objectContaining({
        data: expect.objectContaining({
          uuid: "frontend-1",
          process_pid: 123,
          service_name: "DemoFrontend",
        }),
        onConflict: expect.objectContaining({
          target: ["uuid"],
        }),
        queryData: expect.objectContaining({
          data: expect.objectContaining({
            uuid: "frontend-1",
            process_pid: 123,
            service_name: "DemoFrontend",
          }),
          onConflict: expect.objectContaining({
            target: ["uuid"],
          }),
        }),
      }),
    ]);
    expect(capturedContexts[0].queryData).not.toMatchObject({
      onConflict: {
        target: ["name"],
      },
    });
    expect(result).toMatchObject({
      uuid: "frontend-1",
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

  it("clears stale delegation errors before rerouting a fetch target", async () => {
    const registry = ServiceRegistry.instance as any;
    const selectedContexts: Array<Record<string, unknown>> = [];

    Cadenza.createMetaTask("Capture clean fetch selection", (ctx) => {
      selectedContexts.push(ctx);
      return true;
    }).doOn(
      `meta.service_registry.selected_instance_for_fetch:${buildHandleKey(buildRouteKey("OrdersService", "internal", "http://orders.internal"), "rest")}`,
    );

    registry.instances.set("OrdersService", [
      {
        uuid: "orders-1",
        serviceName: "OrdersService",
        numberOfRunningGraphs: 0,
        isPrimary: true,
        isActive: true,
        isNonResponsive: false,
        isBlocked: false,
        runtimeState: "healthy",
        acceptingWork: true,
        reportedAt: new Date().toISOString(),
        health: {},
        isFrontend: false,
        isDatabase: false,
        transports: [
          {
            uuid: "orders-transport-1",
            serviceInstanceId: "orders-1",
            role: "internal",
            origin: "http://orders.internal",
            protocols: ["rest"],
            securityProfile: null,
            authStrategy: null,
            deleted: false,
            clientCreated: true,
          },
        ],
        clientCreatedTransportIds: [
          buildRouteKey("OrdersService", "internal", "http://orders.internal"),
        ],
        clientReadyTransportIds: [
          buildRouteKey("OrdersService", "internal", "http://orders.internal"),
        ],
      },
    ]);

    Cadenza.run(registry.getBalancedInstance, {
      __serviceName: "OrdersService",
      __remoteRoutineName: "HandleOrdersDelegation",
      errored: true,
      failed: true,
      __error: "Previous delegation failed",
      error: "Previous delegation failed",
      returnedValue: {},
      __metadata: {
        __deputyExecId: "retry-routing-1",
      },
    });

    await waitForCondition(() => selectedContexts.length === 1, 1_500);

    expect(selectedContexts[0]).toMatchObject({
      __instance: "orders-1",
      __fetchId: buildHandleKey(
        buildRouteKey("OrdersService", "internal", "http://orders.internal"),
        "rest",
      ),
      __transportOrigin: "http://orders.internal",
    });
    expect(selectedContexts[0].errored).toBeUndefined();
    expect(selectedContexts[0].failed).toBeUndefined();
    expect(selectedContexts[0].__error).toBeUndefined();
    expect(selectedContexts[0].error).toBeUndefined();
    expect(selectedContexts[0].returnedValue).toBeUndefined();
  });

  it("prefers the freshest ready instance when duplicate routes share the same origin", async () => {
    const registry = ServiceRegistry.instance as any;
    const selectedContexts: Array<Record<string, unknown>> = [];

    Cadenza.createMetaTask("Capture freshest duplicate route selection", (ctx) => {
      selectedContexts.push(ctx);
      return true;
    }).doOn(
      `meta.service_registry.selected_instance_for_fetch:${buildHandleKey(buildRouteKey("OrdersService", "internal", "http://orders.internal"), "rest")}`,
    );

    registry.instances.set("OrdersService", [
      {
        uuid: "orders-old",
        serviceName: "OrdersService",
        numberOfRunningGraphs: 0,
        isPrimary: true,
        isActive: true,
        isNonResponsive: false,
        isBlocked: false,
        runtimeState: "healthy",
        acceptingWork: true,
        reportedAt: "2026-03-25T10:00:00.000Z",
        health: {},
        isFrontend: false,
        isDatabase: false,
        transports: [
          {
            uuid: "orders-transport-old",
            serviceInstanceId: "orders-old",
            role: "internal",
            origin: "http://orders.internal",
            protocols: ["rest"],
            securityProfile: null,
            authStrategy: null,
            deleted: false,
          },
        ],
        clientCreatedTransportIds: [
          buildRouteKey("OrdersService", "internal", "http://orders.internal"),
        ],
        clientReadyTransportIds: [
          buildRouteKey("OrdersService", "internal", "http://orders.internal"),
        ],
      },
      {
        uuid: "orders-new",
        serviceName: "OrdersService",
        numberOfRunningGraphs: 1,
        isPrimary: false,
        isActive: true,
        isNonResponsive: false,
        isBlocked: false,
        runtimeState: "healthy",
        acceptingWork: true,
        reportedAt: "2026-03-26T10:00:00.000Z",
        health: {},
        isFrontend: false,
        isDatabase: false,
        transports: [
          {
            uuid: "orders-transport-new",
            serviceInstanceId: "orders-new",
            role: "internal",
            origin: "http://orders.internal",
            protocols: ["rest"],
            securityProfile: null,
            authStrategy: null,
            deleted: false,
          },
        ],
        clientCreatedTransportIds: [
          buildRouteKey("OrdersService", "internal", "http://orders.internal"),
        ],
        clientReadyTransportIds: [
          buildRouteKey("OrdersService", "internal", "http://orders.internal"),
        ],
      },
    ]);

    Cadenza.run(registry.getBalancedInstance, {
      __serviceName: "OrdersService",
      __remoteRoutineName: "HandleOrdersDelegation",
      __metadata: {
        __deputyExecId: "freshest-route-1",
      },
    });

    await waitForCondition(() => selectedContexts.length === 1, 1_500);

    expect(selectedContexts[0]).toMatchObject({
      __instance: "orders-new",
      __fetchId: buildHandleKey(
        buildRouteKey("OrdersService", "internal", "http://orders.internal"),
        "rest",
      ),
      __transportOrigin: "http://orders.internal",
    });
  });

  it("rotates across equivalent healthy routes on repeated selections", async () => {
    const registry = ServiceRegistry.instance as any;
    const routeKeyA = buildRouteKey(
      "OrdersService",
      "internal",
      "http://orders-a.internal",
    );
    const routeKeyB = buildRouteKey(
      "OrdersService",
      "internal",
      "http://orders-b.internal",
    );

    registry.instances.set("OrdersService", [
      {
        uuid: "orders-1",
        serviceName: "OrdersService",
        numberOfRunningGraphs: 0,
        isPrimary: true,
        isActive: true,
        isNonResponsive: false,
        isBlocked: false,
        runtimeState: "healthy",
        acceptingWork: true,
        reportedAt: "2026-04-04T10:00:00.000Z",
        health: {},
        isFrontend: false,
        isDatabase: false,
        transports: [
          {
            uuid: "orders-transport-1",
            serviceInstanceId: "orders-1",
            role: "internal",
            origin: "http://orders-a.internal",
            protocols: ["rest"],
            securityProfile: null,
            authStrategy: null,
            deleted: false,
          },
        ],
        clientCreatedTransportIds: [
          routeKeyA,
        ],
        clientReadyTransportIds: [
          routeKeyA,
        ],
      },
      {
        uuid: "orders-2",
        serviceName: "OrdersService",
        numberOfRunningGraphs: 0,
        isPrimary: false,
        isActive: true,
        isNonResponsive: false,
        isBlocked: false,
        runtimeState: "healthy",
        acceptingWork: true,
        reportedAt: "2026-04-04T10:00:00.000Z",
        health: {},
        isFrontend: false,
        isDatabase: false,
        transports: [
          {
            uuid: "orders-transport-2",
            serviceInstanceId: "orders-2",
            role: "internal",
            origin: "http://orders-b.internal",
            protocols: ["rest"],
            securityProfile: null,
            authStrategy: null,
            deleted: false,
          },
        ],
        clientCreatedTransportIds: [
          routeKeyB,
        ],
        clientReadyTransportIds: [
          routeKeyB,
        ],
      },
    ]);

    const [ordersA, ordersB] = registry.instances.get("OrdersService");
    const transportA = ordersA.transports[0];
    const transportB = ordersB.transports[0];

    const routeA = registry.upsertRemoteRouteRecord(
      "OrdersService",
      ordersA.uuid,
      transportA,
    );
    const routeB = registry.upsertRemoteRouteRecord(
      "OrdersService",
      ordersB.uuid,
      transportB,
    );

    registry.recordBalancedRouteSelection(routeA.key, {
      __deputyExecId: "balanced-route-1",
      __routeKey: routeA.key,
      __fetchId: buildHandleKey(routeA.key, "rest"),
    });

    const snapshotA = registry.buildRouteBalancingSnapshot(routeA, ordersA);
    const snapshotB = registry.buildRouteBalancingSnapshot(routeB, ordersB);

    expect(
      registry.compareRouteBalancingSnapshots(snapshotA, snapshotB),
    ).toBeGreaterThan(0);
  });

  it("filters broadcast selections to matching receivers", async () => {
    const registry = ServiceRegistry.instance as any;
    const selectedContexts: Array<Record<string, unknown>> = [];

    Cadenza.createMetaTask("Capture filtered broadcast selection", (ctx) => {
      selectedContexts.push(ctx);
      return true;
    }).doOn(
      `meta.service_registry.selected_instance_for_fetch:${buildHandleKey(buildRouteKey("OrdersService", "internal", "http://orders-b.internal"), "rest")}`,
    );

    registry.instances.set("OrdersService", [
      {
        uuid: "orders-1",
        serviceName: "OrdersService",
        numberOfRunningGraphs: 0,
        isPrimary: true,
        isActive: true,
        isNonResponsive: false,
        isBlocked: false,
        runtimeState: "healthy",
        acceptingWork: true,
        reportedAt: "2026-04-04T10:00:00.000Z",
        health: {},
        isFrontend: false,
        isDatabase: false,
        transports: [
          {
            uuid: "orders-transport-1",
            serviceInstanceId: "orders-1",
            role: "internal",
            origin: "http://orders-a.internal",
            protocols: ["rest"],
            securityProfile: null,
            authStrategy: null,
            deleted: false,
          },
        ],
        clientCreatedTransportIds: [
          buildRouteKey("OrdersService", "internal", "http://orders-a.internal"),
        ],
        clientReadyTransportIds: [
          buildRouteKey("OrdersService", "internal", "http://orders-a.internal"),
        ],
      },
      {
        uuid: "orders-2",
        serviceName: "OrdersService",
        numberOfRunningGraphs: 0,
        isPrimary: false,
        isActive: true,
        isNonResponsive: false,
        isBlocked: false,
        runtimeState: "healthy",
        acceptingWork: true,
        reportedAt: "2026-04-04T10:00:00.000Z",
        health: {},
        isFrontend: false,
        isDatabase: false,
        transports: [
          {
            uuid: "orders-transport-2",
            serviceInstanceId: "orders-2",
            role: "internal",
            origin: "http://orders-b.internal",
            protocols: ["rest"],
            securityProfile: null,
            authStrategy: null,
            deleted: false,
          },
        ],
        clientCreatedTransportIds: [
          buildRouteKey("OrdersService", "internal", "http://orders-b.internal"),
        ],
        clientReadyTransportIds: [
          buildRouteKey("OrdersService", "internal", "http://orders-b.internal"),
        ],
      },
    ]);

    Cadenza.run(registry.getBalancedInstance, {
      __serviceName: "OrdersService",
      __remoteRoutineName: "BroadcastOrdersUpdate",
      __broadcast: true,
      __broadcastFilter: {
        serviceInstanceIds: ["orders-2"],
      },
      __metadata: {
        __deputyExecId: "broadcast-filter-1",
      },
    });

    await waitForCondition(() => selectedContexts.length === 1, 1_500);

    expect(selectedContexts[0]).toMatchObject({
      __instance: "orders-2",
      __transportOrigin: "http://orders-b.internal",
    });
  });

  it("restores the original delegation request shape before rerouting a failed fetch delegation", async () => {
    const registry = ServiceRegistry.instance as any;
    const selectedContexts: Array<Record<string, unknown>> = [];

    Cadenza.createMetaTask("Capture restored fetch selection", (ctx) => {
      selectedContexts.push(ctx);
      return true;
    }).doOn(
      `meta.service_registry.selected_instance_for_fetch:${buildHandleKey(buildRouteKey("OrdersService", "internal", "http://orders.internal"), "rest")}`,
    );

    registry.instances.set("OrdersService", [
      {
        uuid: "orders-1",
        serviceName: "OrdersService",
        numberOfRunningGraphs: 0,
        isPrimary: true,
        isActive: true,
        isNonResponsive: false,
        isBlocked: false,
        runtimeState: "healthy",
        acceptingWork: true,
        reportedAt: new Date().toISOString(),
        health: {},
        isFrontend: false,
        isDatabase: false,
        transports: [
          {
            uuid: "orders-transport-1",
            serviceInstanceId: "orders-1",
            role: "internal",
            origin: "http://orders.internal",
            protocols: ["rest"],
            securityProfile: null,
            authStrategy: null,
            deleted: false,
            clientCreated: true,
          },
        ],
        clientCreatedTransportIds: [
          buildRouteKey("OrdersService", "internal", "http://orders.internal"),
        ],
        clientReadyTransportIds: [
          buildRouteKey("OrdersService", "internal", "http://orders.internal"),
        ],
      },
    ]);

    const originalRequest = {
      __serviceName: "OrdersService",
      __remoteRoutineName: "Insert actor_session_state",
      __metadata: {
        __deputyExecId: "actor-session-retry-1",
      },
      data: {
        actor_name: "TelemetrySessionActor",
        actor_version: 1,
        actor_key: "device-1",
        durable_state: { foo: "bar" },
        durable_version: 2,
        service_name: "TelemetryCollectorService",
      },
      queryData: {
        data: {
          actor_name: "TelemetrySessionActor",
          actor_version: 1,
          actor_key: "device-1",
          durable_state: { foo: "bar" },
          durable_version: 2,
          service_name: "TelemetryCollectorService",
        },
        onConflict: {
          keys: ["actor_name", "actor_key"],
        },
      },
    };

    Cadenza.run(registry.getBalancedInstance, {
      __serviceName: "OrdersService",
      __remoteRoutineName: "Insert actor_session_state",
      __metadata: {
        __deputyExecId: "actor-session-retry-1",
      },
      __deputyExecId: "actor-session-retry-1",
      __delegationRequestContext: originalRequest,
      actor_name: "TelemetrySessionActor",
      actor_version: 1,
      actor_key: "device-1",
      durable_state: { foo: "bar" },
      durable_version: 2,
      service_name: "TelemetryCollectorService",
      onConflict: {
        keys: ["actor_name", "actor_key"],
      },
      rowCount: 0,
      __status: "success",
      __success: true,
      __isDeputy: true,
      __signalEmission: {
        fullSignalName: `meta.fetch.delegate_failed:${buildHandleKey(buildRouteKey("OrdersService", "internal", "http://orders.internal"), "rest")}`,
      },
      __retries: 1,
      __triedInstances: ["orders-1"],
    });

    await waitForCondition(() => selectedContexts.length === 1, 1_500);

    expect(selectedContexts[0]).toMatchObject({
      __instance: "orders-1",
      __fetchId: buildHandleKey(
        buildRouteKey("OrdersService", "internal", "http://orders.internal"),
        "rest",
      ),
      __transportOrigin: "http://orders.internal",
      __deputyExecId: "actor-session-retry-1",
      data: expect.objectContaining({
        actor_name: "TelemetrySessionActor",
        actor_key: "device-1",
      }),
      queryData: expect.objectContaining({
        data: expect.objectContaining({
          actor_name: "TelemetrySessionActor",
          actor_key: "device-1",
        }),
        onConflict: expect.objectContaining({
          keys: ["actor_name", "actor_key"],
        }),
      }),
      __retries: 2,
      __triedInstances: ["orders-1"],
    });
    expect(selectedContexts[0].rowCount).toBeUndefined();
    expect(selectedContexts[0].__status).toBeUndefined();
    expect(selectedContexts[0].__success).toBeUndefined();
  });

  it("preserves the live deputy identity over a stale embedded snapshot during routing", async () => {
    const registry = ServiceRegistry.instance as any;
    const selectedContexts: Array<Record<string, unknown>> = [];

    Cadenza.createMetaTask("Capture live deputy fetch selection", (ctx) => {
      selectedContexts.push(ctx);
      return true;
    }).doOn(
      `meta.service_registry.selected_instance_for_fetch:${buildHandleKey(buildRouteKey("OrdersService", "internal", "http://orders.internal"), "rest")}`,
    );

    registry.instances.set("OrdersService", [
      {
        uuid: "orders-1",
        serviceName: "OrdersService",
        numberOfRunningGraphs: 0,
        isPrimary: true,
        isActive: true,
        isNonResponsive: false,
        isBlocked: false,
        runtimeState: "healthy",
        acceptingWork: true,
        reportedAt: new Date().toISOString(),
        health: {},
        isFrontend: false,
        isDatabase: false,
        transports: [
          {
            uuid: "orders-transport-1",
            serviceInstanceId: "orders-1",
            role: "internal",
            origin: "http://orders.internal",
            protocols: ["rest"],
            securityProfile: null,
            authStrategy: null,
            deleted: false,
            clientCreated: true,
          },
        ],
        clientCreatedTransportIds: [
          buildRouteKey("OrdersService", "internal", "http://orders.internal"),
        ],
        clientReadyTransportIds: [
          buildRouteKey("OrdersService", "internal", "http://orders.internal"),
        ],
      },
    ]);

    Cadenza.run(registry.getBalancedInstance, {
      __serviceName: "OrdersService",
      __remoteRoutineName: "Insert service_instance",
      __localTaskName: "Insert service_instance in CadenzaDB",
      __metadata: {
        __deputyExecId: "live-routing-1",
      },
      __deputyExecId: "live-routing-1",
      __resolverRequestId: "resolver-live-1",
      __resolverQueryData: {
        data: {
          uuid: "orders-live-1",
          process_pid: 123,
          service_name: "OrdersService",
        },
        onConflict: {
          target: ["uuid"],
        },
      },
      data: {
        uuid: "orders-live-1",
        process_pid: 123,
        service_name: "OrdersService",
      },
      queryData: {
        data: {
          uuid: "orders-live-1",
          process_pid: 123,
          service_name: "OrdersService",
        },
        onConflict: {
          target: ["uuid"],
        },
      },
      __delegationRequestContext: {
        __serviceName: "OrdersService",
        __remoteRoutineName: "Insert service_instance",
        __localTaskName: "Insert service_instance in CadenzaDB",
        __metadata: {
          __deputyExecId: "stale-routing-1",
        },
        __deputyExecId: "stale-routing-1",
        __resolverRequestId: "resolver-stale-1",
        queryData: {
          data: {
            name: "stale",
          },
        },
      },
    });

    await waitForCondition(() => selectedContexts.length === 1, 1_500);

    expect(selectedContexts[0]).toMatchObject({
      __deputyExecId: "live-routing-1",
      __resolverRequestId: "resolver-live-1",
      __serviceName: "OrdersService",
      __remoteRoutineName: "Insert service_instance",
      __localTaskName: "Insert service_instance in CadenzaDB",
      queryData: expect.objectContaining({
        data: expect.objectContaining({
          uuid: "orders-live-1",
          process_pid: 123,
          service_name: "OrdersService",
        }),
      }),
    });
    expect(selectedContexts[0].__metadata).toMatchObject({
      __deputyExecId: "live-routing-1",
    });
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

  it("does not eagerly create dependee clients from authority transport updates", async () => {
    const registry = ServiceRegistry.instance as any;
    const dependeeRegistrations: Array<Record<string, unknown>> = [];

    registry.serviceName = "CadenzaDB";
    registry.serviceInstanceId = "cadenza-db-1";
    registry.remoteSignals.set("TelemetryCollectorService", new Set(["signal.test"]));
    registry.instances.set("TelemetryCollectorService", [
      {
        uuid: "telemetry-1",
        serviceName: "TelemetryCollectorService",
        numberOfRunningGraphs: 0,
        isPrimary: false,
        isActive: true,
        isNonResponsive: false,
        isBlocked: false,
        runtimeState: "healthy",
        acceptingWork: true,
        reportedAt: new Date().toISOString(),
        health: {},
        isFrontend: false,
        isDatabase: false,
        transports: [],
        clientCreatedTransportIds: [],
        clientPendingTransportIds: [],
        clientReadyTransportIds: [],
      },
    ]);

    Cadenza.createMetaTask(
      "Capture authority-skipped dependee registration",
      (ctx) => {
        dependeeRegistrations.push(ctx.data ?? ctx);
        return true;
      },
    ).doOn("meta.service_registry.dependee_registered");

    Cadenza.run(registry.handleTransportUpdateTask, {
      serviceTransport: {
        uuid: "telemetry-internal-1",
        serviceInstanceId: "telemetry-1",
        role: "internal",
        origin: "http://telemetry-collector:3004",
        protocols: ["rest"],
      },
    });

    await new Promise((resolve) => setTimeout(resolve, 25));

    expect(dependeeRegistrations).toEqual([]);
    expect(
      registry.instances.get("TelemetryCollectorService")?.[0]
        ?.clientPendingTransportIds,
    ).toEqual([]);
  });

  it("does not demand-create authority clients for global meta signal routing", async () => {
    const registry = ServiceRegistry.instance as any;
    const dependeeRegistrations: Array<Record<string, unknown>> = [];

    registry.serviceName = "CadenzaDB";
    registry.serviceInstanceId = "cadenza-db-1";
    registry.remoteSignals.set(
      "ScheduledRunnerService",
      new Set(["global.meta.service_instance.updated"]),
    );
    registry.instances.set("ScheduledRunnerService", [
      {
        uuid: "scheduled-runner-1",
        serviceName: "ScheduledRunnerService",
        numberOfRunningGraphs: 0,
        isPrimary: false,
        isActive: true,
        isNonResponsive: false,
        isBlocked: false,
        runtimeState: "healthy",
        acceptingWork: true,
        reportedAt: new Date().toISOString(),
        health: {},
        isFrontend: false,
        isDatabase: false,
        transports: [
          {
            uuid: "scheduled-runner-internal",
            serviceInstanceId: "scheduled-runner-1",
            role: "internal",
            origin: "http://scheduled-runner:3002",
            protocols: ["rest"],
            securityProfile: null,
            authStrategy: null,
          },
        ],
        clientCreatedTransportIds: [],
        clientPendingTransportIds: [],
        clientReadyTransportIds: [],
      },
    ]);

    Cadenza.createMetaTask(
      "Capture authority meta-signal dependee registration",
      (ctx) => {
        dependeeRegistrations.push(ctx.data ?? ctx);
        return true;
      },
    ).doOn("meta.service_registry.dependee_registered");

    Cadenza.run(registry.getBalancedInstance, {
      __serviceName: "ScheduledRunnerService",
      __signalName: "global.meta.service_instance.updated",
      __metadata: {
        __deputyExecId: "authority-meta-signal-1",
      },
    });

    await new Promise((resolve) => setTimeout(resolve, 25));

    expect(dependeeRegistrations).toEqual([]);
    expect(
      registry.instances.get("ScheduledRunnerService")?.[0]
        ?.clientPendingTransportIds,
    ).toEqual([]);
  });

  it("connects existing internal transports when remote intent deputies register after full sync", async () => {
    const registry = ServiceRegistry.instance as any;
    const dependeeRegistrations: Array<Record<string, unknown>> = [];

    registry.serviceName = "TelemetryCollectorService";
    registry.serviceInstanceId = "telemetry-collector-1";

    Cadenza.createMetaTask(
      "Capture dependee registration after remote intent sync",
      (ctx) => {
        dependeeRegistrations.push(ctx.data ?? ctx);
        return true;
      },
    ).doOn("meta.service_registry.dependee_registered");

    registry.instances.set("IotDbService", [
      {
        uuid: "iot-db-1",
        serviceName: "IotDbService",
        numberOfRunningGraphs: 0,
        isPrimary: false,
        isActive: true,
        isNonResponsive: false,
        isBlocked: false,
        runtimeState: "healthy",
        acceptingWork: true,
        reportedAt: new Date().toISOString(),
        health: {},
        isFrontend: false,
        isDatabase: true,
        transports: [
          {
            uuid: "iot-db-internal",
            serviceInstanceId: "iot-db-1",
            role: "internal",
            origin: "http://iot-db-service:3001",
            protocols: ["rest"],
            securityProfile: null,
            authStrategy: null,
          },
          {
            uuid: "iot-db-public",
            serviceInstanceId: "iot-db-1",
            role: "public",
            origin: "http://iot-db.localhost",
            protocols: ["rest"],
            securityProfile: null,
            authStrategy: null,
          },
        ],
        clientCreatedTransportIds: [],
      },
    ]);

    Cadenza.run(registry.handleGlobalIntentRegistrationTask, {
      intentToTaskMaps: [
        {
          intentName: "iot-db-telemetry-insert",
          taskName: "Normalize telemetry insert queryData",
          taskVersion: 1,
          serviceName: "IotDbService",
        },
      ],
    });

    await waitForCondition(() => dependeeRegistrations.length === 1, 1_500);

    expect(dependeeRegistrations[0]).toMatchObject({
      serviceName: "IotDbService",
      serviceInstanceId: "iot-db-1",
      serviceTransportId: "iot-db-internal",
      serviceOrigin: "http://iot-db-service:3001",
      transportProtocols: ["rest"],
    });
    expect(
      registry.instances.get("IotDbService")?.[0]?.clientPendingTransportIds,
    ).toContain(
      buildRouteKey("IotDbService", "internal", "http://iot-db-service:3001"),
    );
  });

  it("backfills dependee clients when deputy registration happens after an authoritative transport update", async () => {
    const registry = ServiceRegistry.instance as any;
    const dependeeRegistrations: Array<Record<string, unknown>> = [];

    registry.serviceName = "TelemetryCollectorService";
    registry.serviceInstanceId = "telemetry-collector-1";

    Cadenza.createMetaTask(
      "Capture dependee registration after deputy registration",
      (ctx) => {
        dependeeRegistrations.push(ctx.data ?? ctx);
        return true;
      },
    ).doOn("meta.service_registry.dependee_registered");

    registry.instances.set("CadenzaDB", [
      {
        uuid: "cadenza-db-1",
        serviceName: "CadenzaDB",
        numberOfRunningGraphs: 0,
        isPrimary: false,
        isActive: true,
        isNonResponsive: false,
        isBlocked: false,
        runtimeState: "healthy",
        acceptingWork: true,
        reportedAt: new Date().toISOString(),
        health: {},
        isFrontend: false,
        isDatabase: true,
        transports: [
          {
            uuid: "cadenza-db-internal-authority",
            serviceInstanceId: "cadenza-db-1",
            role: "internal",
            origin: "http://cadenza-db-service:8080",
            protocols: ["rest", "socket"],
            securityProfile: null,
            authStrategy: null,
          },
        ],
        clientCreatedTransportIds: ["cadenza-db-internal-bootstrap"],
      },
    ]);

    Cadenza.emit("meta.deputy.created", {
      serviceName: "CadenzaDB",
      remoteRoutineName: "Insert intent_to_task_map",
      localTaskName: "Insert intent_to_task_map in CadenzaDB",
      communicationType: "rest",
    });

    await waitForCondition(() => dependeeRegistrations.length === 1, 1_500);

    expect(dependeeRegistrations[0]).toMatchObject({
      serviceName: "CadenzaDB",
      serviceInstanceId: "cadenza-db-1",
      serviceTransportId: "cadenza-db-internal-authority",
      serviceOrigin: "http://cadenza-db-service:8080",
    });
    expect(
      registry.instances.get("CadenzaDB")?.[0]?.clientPendingTransportIds,
    ).toEqual(
      expect.arrayContaining([
        buildRouteKey("CadenzaDB", "internal", "http://cadenza-db-service:8080"),
      ]),
    );
  });

  it("waits briefly for pending transport handshakes before failing delegated work", async () => {
    const registry = ServiceRegistry.instance as any;
    const selectedContexts: Array<Record<string, unknown>> = [];
    const failedContexts: Array<Record<string, unknown>> = [];

    Cadenza.createMetaTask("Capture pending fetch selection", (ctx) => {
      selectedContexts.push(ctx);
      return true;
    }).doOn(
      `meta.service_registry.selected_instance_for_fetch:${buildHandleKey(buildRouteKey("OrdersService", "internal", "http://orders.internal"), "rest")}`,
    );

    Cadenza.createMetaTask("Capture pending fetch failure", (ctx) => {
      failedContexts.push(ctx);
      return true;
    }).doOn("meta.service_registry.load_balance_failed:pending-routing-1");

    registry.instances.set("OrdersService", [
      {
        uuid: "orders-1",
        serviceName: "OrdersService",
        numberOfRunningGraphs: 0,
        isPrimary: true,
        isActive: true,
        isNonResponsive: false,
        isBlocked: false,
        runtimeState: "healthy",
        acceptingWork: true,
        reportedAt: new Date().toISOString(),
        health: {},
        isFrontend: false,
        isDatabase: false,
        transports: [
          {
            uuid: "orders-transport-1",
            serviceInstanceId: "orders-1",
            role: "internal",
            origin: "http://orders.internal",
            protocols: ["rest"],
            securityProfile: null,
            authStrategy: null,
            deleted: false,
          },
        ],
        clientPendingTransportIds: [
          buildRouteKey("OrdersService", "internal", "http://orders.internal"),
        ],
      },
    ]);

    Cadenza.run(registry.getBalancedInstance, {
      __serviceName: "OrdersService",
      __remoteRoutineName: "HandleOrdersDelegation",
      __signalName: "meta.deputy.delegation_requested",
      __metadata: {
        __deputyExecId: "pending-routing-1",
      },
    });

    setTimeout(() => {
      Cadenza.emit("meta.fetch.handshake_complete", {
        serviceName: "OrdersService",
        serviceInstanceId: "orders-1",
        serviceTransportId: "orders-transport-1",
        communicationTypes: ["rest"],
      });
    }, 60);

    await waitForCondition(() => selectedContexts.length === 1, 1_500);

    expect(failedContexts).toEqual([]);
    expect(selectedContexts[0]).toMatchObject({
      __instance: "orders-1",
      __transportId: "orders-transport-1",
      __transportOrigin: "http://orders.internal",
    });
  });

  it("enters no-route cooldown after repeated misses and wakes on matching internal transport readiness", async () => {
    const registry = ServiceRegistry.instance as any;
    const failedContexts: Array<Record<string, unknown>> = [];
    const selectedContexts: Array<Record<string, unknown>> = [];

    registry.serviceName = "TelemetryCollectorService";
    registry.serviceInstanceId = "telemetry-collector-1";
    registry.retryCount = 0;

    Cadenza.createMetaTask("Capture routing cooldown failures", (ctx) => {
      failedContexts.push(ctx);
      return true;
    }).doOn("meta.service_registry.load_balance_failed:cooldown-routing-1");

    Cadenza.createMetaTask("Capture routing cooldown wake selection", (ctx) => {
      selectedContexts.push(ctx);
      return true;
    }).doOn(
      `meta.service_registry.selected_instance_for_fetch:${buildHandleKey(buildRouteKey("IotDbService", "internal", "http://iot-db-service:3001"), "rest")}`,
    );

    registry.instances.set("IotDbService", [
      {
        uuid: "iot-1",
        serviceName: "IotDbService",
        numberOfRunningGraphs: 0,
        isPrimary: true,
        isActive: true,
        isNonResponsive: false,
        isBlocked: false,
        runtimeState: "healthy",
        acceptingWork: true,
        reportedAt: new Date().toISOString(),
        health: {},
        isFrontend: false,
        isDatabase: true,
        transports: [
          {
            uuid: "iot-internal-1",
            serviceInstanceId: "iot-1",
            role: "internal",
            origin: "http://iot-db-service:3001",
            protocols: ["rest"],
            securityProfile: null,
            authStrategy: null,
            deleted: false,
          },
        ],
        clientPendingTransportIds: ["iot-internal-1"],
        clientReadyTransportIds: [],
      },
    ]);

    const failureContext = {
      __serviceName: "IotDbService",
      __remoteRoutineName: "Query telemetry",
      __metadata: {
        __deputyExecId: "cooldown-routing-1",
      },
    };

    Cadenza.run(registry.getBalancedInstance, { ...failureContext });
    Cadenza.run(registry.getBalancedInstance, { ...failureContext });
    Cadenza.run(registry.getBalancedInstance, { ...failureContext });
    Cadenza.run(registry.getBalancedInstance, { ...failureContext });

    await waitForCondition(() => failedContexts.length === 4, 1_500);

    expect(
      registry.routingCooldownsByKey.get("IotDbService|internal|rest"),
    ).toEqual(
      expect.objectContaining({
        serviceName: "IotDbService",
        role: "internal",
        protocol: "rest",
      }),
    );
    expect(String(failedContexts[3]?.__error ?? "")).toContain(
      "Waiting for authority route updates before retrying",
    );

    Cadenza.emit("meta.fetch.handshake_complete", {
      serviceName: "IotDbService",
      serviceInstanceId: "iot-1",
      serviceTransportId: "iot-internal-1",
      communicationTypes: ["rest"],
    });

    await waitForCondition(
      () => !registry.routingCooldownsByKey.has("IotDbService|internal|rest"),
      1_500,
    );

    Cadenza.run(registry.getBalancedInstance, {
      __serviceName: "IotDbService",
      __remoteRoutineName: "Query telemetry",
      __metadata: {
        __deputyExecId: "cooldown-routing-2",
      },
    });

    await waitForCondition(() => selectedContexts.length === 1, 1_500);

    expect(selectedContexts[0]).toMatchObject({
      __instance: "iot-1",
      __transportId: "iot-internal-1",
      __transportOrigin: "http://iot-db-service:3001",
    });
  });

  it("does not wake internal routing cooldowns from public-only transport readiness", async () => {
    const registry = ServiceRegistry.instance as any;
    const failedContexts: Array<Record<string, unknown>> = [];

    registry.serviceName = "TelemetryCollectorService";
    registry.serviceInstanceId = "telemetry-collector-1";
    registry.retryCount = 0;

    Cadenza.createMetaTask("Capture public-only cooldown failures", (ctx) => {
      failedContexts.push(ctx);
      return true;
    }).doOn("meta.service_registry.load_balance_failed:cooldown-routing-public");

    registry.instances.set("IotDbService", [
      {
        uuid: "iot-public-only",
        serviceName: "IotDbService",
        numberOfRunningGraphs: 0,
        isPrimary: true,
        isActive: true,
        isNonResponsive: false,
        isBlocked: false,
        runtimeState: "healthy",
        acceptingWork: true,
        reportedAt: new Date().toISOString(),
        health: {},
        isFrontend: false,
        isDatabase: true,
        transports: [
          {
            uuid: "iot-public-1",
            serviceInstanceId: "iot-public-only",
            role: "public",
            origin: "http://iot-db.localhost",
            protocols: ["rest"],
            securityProfile: null,
            authStrategy: null,
            deleted: false,
          },
        ],
        clientPendingTransportIds: ["iot-public-1"],
        clientReadyTransportIds: [],
      },
    ]);

    const failureContext = {
      __serviceName: "IotDbService",
      __remoteRoutineName: "Query telemetry",
      __metadata: {
        __deputyExecId: "cooldown-routing-public",
      },
    };

    Cadenza.run(registry.getBalancedInstance, { ...failureContext });
    Cadenza.run(registry.getBalancedInstance, { ...failureContext });
    Cadenza.run(registry.getBalancedInstance, { ...failureContext });

    await waitForCondition(
      () => registry.routingCooldownsByKey.has("IotDbService|internal|rest"),
      1_500,
    );

    Cadenza.emit("meta.fetch.handshake_complete", {
      serviceName: "IotDbService",
      serviceInstanceId: "iot-public-only",
      serviceTransportId: "iot-public-1",
      communicationTypes: ["rest"],
    });

    await new Promise((resolve) => setTimeout(resolve, 25));

    expect(
      registry.routingCooldownsByKey.has("IotDbService|internal|rest"),
    ).toBe(true);

    Cadenza.run(registry.getBalancedInstance, { ...failureContext });

    await waitForCondition(() => failedContexts.length === 4, 1_500);

    expect(String(failedContexts[3]?.__error ?? "")).toContain(
      "Waiting for authority route updates before retrying",
    );
  });

  it("retries pending internal route recovery before honoring a stale no-route cooldown", async () => {
    const registry = ServiceRegistry.instance as any;
    const failedContexts: Array<Record<string, unknown>> = [];
    const scheduleSpy = vi
      .spyOn(Cadenza, "schedule")
      .mockImplementation(() => undefined as any);

    registry.serviceName = "TelemetryCollectorService";
    registry.serviceInstanceId = "telemetry-collector-1";
    registry.retryCount = 0;
    registry.routingCooldownsByKey.set("IotDbService|internal|rest", {
      serviceName: "IotDbService",
      role: "internal",
      protocol: "rest",
      failureCount: 3,
      lastFailureAt: Date.now(),
      cooldownUntil: Date.now() + 20_000,
      reason: "no_routeable_instance",
    });

    Cadenza.createMetaTask("Capture stale cooldown failure", (ctx) => {
      failedContexts.push(ctx);
      return true;
    }).doOn("meta.service_registry.load_balance_failed:cooldown-routing-recovery");

    registry.deputies.set("IotDbService", [
      {
        serviceName: "IotDbService",
        intentName: "insert-pg-iot-db-service-postgres-actor-telemetry",
        localTaskName: "Insert telemetry (Proxy)",
        communicationType: "delegation",
      },
    ]);
    registry.instances.set("IotDbService", [
      {
        uuid: "iot-1",
        serviceName: "IotDbService",
        numberOfRunningGraphs: 0,
        isPrimary: true,
        isActive: true,
        isNonResponsive: false,
        isBlocked: false,
        runtimeState: "healthy",
        acceptingWork: true,
        reportedAt: new Date().toISOString(),
        health: {},
        isFrontend: false,
        isDatabase: true,
        transports: [
          {
            uuid: "iot-internal-1",
            serviceInstanceId: "iot-1",
            role: "internal",
            origin: "http://iot-db-service:3001",
            protocols: ["rest"],
            securityProfile: null,
            authStrategy: null,
            deleted: false,
          },
        ],
        clientCreatedTransportIds: [],
        clientPendingTransportIds: [],
        clientReadyTransportIds: [],
      },
    ]);

    Cadenza.run(registry.getBalancedInstance, {
      __serviceName: "IotDbService",
      __remoteRoutineName: "Insert telemetry",
      __signalName: "meta.deputy.delegation_requested",
      __metadata: {
        __deputyExecId: "cooldown-routing-recovery",
      },
    });

    await waitForCondition(
      () =>
        scheduleSpy.mock.calls.some(
          ([signal]) => signal === "meta.deputy.delegation_requested",
        ),
      1_500,
    );

    expect(failedContexts).toHaveLength(0);
    expect(scheduleSpy.mock.calls).toEqual(
      expect.arrayContaining([
        [
          "meta.deputy.delegation_requested",
          expect.objectContaining({
            __serviceName: "IotDbService",
            __pendingRouteSelectionAttempt: 1,
            __pendingRouteSelectionServiceName: "IotDbService",
          }),
          expect.any(Number),
        ],
      ]),
    );
    expect(
      registry.instances.get("IotDbService")?.[0]?.clientPendingTransportIds,
    ).toContain("IotDbService|internal|http://iot-db-service:3001");
  });

  it("demotes repeated timeout transport failures before rebalancing", async () => {
    const registry = ServiceRegistry.instance as any;
    const serviceNotRespondingContexts: Array<Record<string, unknown>> = [];

    registry.serviceName = "TelemetryCollectorService";
    registry.serviceInstanceId = "telemetry-collector-1";
    registry.retryCount = 0;

    Cadenza.createMetaTask("Capture dead-route demotion", (ctx) => {
      serviceNotRespondingContexts.push(ctx);
      return true;
    }).doOn("global.meta.service_registry.service_not_responding");

    registry.instances.set("IotDbService", [
      {
        uuid: "iot-dead-route",
        serviceName: "IotDbService",
        numberOfRunningGraphs: 0,
        isPrimary: true,
        isActive: true,
        isNonResponsive: false,
        isBlocked: false,
        runtimeState: "healthy",
        acceptingWork: true,
        reportedAt: new Date().toISOString(),
        health: {},
        isFrontend: false,
        isDatabase: true,
        transports: [
          {
            uuid: "iot-dead-transport",
            serviceInstanceId: "iot-dead-route",
            role: "internal",
            origin: "http://iot-db-service:3001",
            protocols: ["rest"],
            securityProfile: null,
            authStrategy: null,
            deleted: false,
          },
        ],
        clientCreatedTransportIds: ["iot-dead-transport"],
        clientPendingTransportIds: [],
        clientReadyTransportIds: ["iot-dead-transport"],
      },
    ]);

    const failureContext = {
      __serviceName: "IotDbService",
      __remoteRoutineName: "Query telemetry",
      __instance: "iot-dead-route",
      __transportId: "iot-dead-transport",
      __fetchId: "iot-dead-transport",
      __error:
        "Error: AbortError: The operation was aborted after timeout while contacting http://iot-db-service:3001/delegation",
      __metadata: {
        __deputyExecId: "dead-route-demotion-1",
      },
      __signalEmission: {
        fullSignalName: "meta.fetch.delegate_failed:iot-dead-transport",
      },
    };

    Cadenza.run(registry.getBalancedInstance, { ...failureContext });
    Cadenza.run(registry.getBalancedInstance, { ...failureContext });

    await waitForCondition(
      () =>
        (registry.instances.get("IotDbService")?.[0]?.clientReadyTransportIds ?? [])
          .length === 0 &&
        serviceNotRespondingContexts.length === 1,
      1_500,
    );

    expect(serviceNotRespondingContexts[0]).toMatchObject({
      filter: {
        uuid: "iot-dead-route",
      },
      data: {
        isActive: false,
        isNonResponsive: true,
      },
    });
    expect(registry.instances.get("IotDbService")?.[0]).toMatchObject({
      isActive: false,
      isNonResponsive: true,
      clientReadyTransportIds: [],
    });
  });

  it("demotes a hard network transport failure on the first ECONNREFUSED", async () => {
    const registry = ServiceRegistry.instance as any;
    const serviceNotRespondingContexts: Array<Record<string, unknown>> = [];

    registry.serviceName = "TelemetryCollectorService";
    registry.serviceInstanceId = "telemetry-collector-1";
    registry.retryCount = 0;

    Cadenza.createMetaTask("Capture immediate hard-failure demotion", (ctx) => {
      serviceNotRespondingContexts.push(ctx);
      return true;
    }).doOn("global.meta.service_registry.service_not_responding");

    registry.instances.set("IotDbService", [
      {
        uuid: "iot-dead-route-immediate",
        serviceName: "IotDbService",
        numberOfRunningGraphs: 0,
        isPrimary: true,
        isActive: true,
        isNonResponsive: false,
        isBlocked: false,
        runtimeState: "healthy",
        acceptingWork: true,
        reportedAt: new Date().toISOString(),
        health: {},
        isFrontend: false,
        isDatabase: true,
        transports: [
          {
            uuid: "iot-dead-transport-immediate",
            serviceInstanceId: "iot-dead-route-immediate",
            role: "internal",
            origin: "http://iot-db-service:3001",
            protocols: ["rest"],
            securityProfile: null,
            authStrategy: null,
            deleted: false,
          },
        ],
        clientCreatedTransportIds: ["iot-dead-transport-immediate"],
        clientPendingTransportIds: [],
        clientReadyTransportIds: ["iot-dead-transport-immediate"],
      },
    ]);

    Cadenza.run(registry.getBalancedInstance, {
      __serviceName: "IotDbService",
      __remoteRoutineName: "Insert telemetry",
      __instance: "iot-dead-route-immediate",
      __transportId: "iot-dead-transport-immediate",
      __fetchId: "iot-dead-transport-immediate",
      __error:
        "Error: FetchError: request to http://iot-db-service:3001/delegation failed, reason: connect ECONNREFUSED 172.18.0.4:3001",
      __metadata: {
        __deputyExecId: "dead-route-demotion-immediate-1",
      },
      __signalEmission: {
        fullSignalName:
          "meta.fetch.delegate_failed:iot-dead-transport-immediate",
      },
    });

    await waitForCondition(
      () =>
        (registry.instances.get("IotDbService")?.[0]?.clientReadyTransportIds ?? [])
          .length === 0 &&
        serviceNotRespondingContexts.length === 1,
      1_500,
    );

    expect(serviceNotRespondingContexts[0]).toMatchObject({
      filter: {
        uuid: "iot-dead-route-immediate",
      },
      data: {
        isActive: false,
        isNonResponsive: true,
      },
    });
    expect(registry.instances.get("IotDbService")?.[0]).toMatchObject({
      isActive: false,
      isNonResponsive: true,
      clientReadyTransportIds: [],
    });
  });

  it("merges top-level sync transports into split service instances before dependee backfill", async () => {
    const registry = ServiceRegistry.instance as any;
    const dependeeRegistrations: Array<Record<string, unknown>> = [];

    registry.serviceName = "TelemetryCollectorService";
    registry.serviceInstanceId = "telemetry-collector-1";

    Cadenza.createMetaTask(
      "Capture dependee registration from merged full sync payload",
      (ctx) => {
        dependeeRegistrations.push(ctx.data ?? ctx);
        return true;
      },
    ).doOn("meta.service_registry.dependee_registered");

    Cadenza.run(registry.handleGlobalIntentRegistrationTask, {
      intentToTaskMaps: [
        {
          intentName: "iot-db-telemetry-insert",
          taskName: "Normalize telemetry insert queryData",
          taskVersion: 1,
          serviceName: "IotDbService",
        },
      ],
      serviceInstances: [
        {
          uuid: "iot-db-1",
          serviceName: "IotDbService",
          numberOfRunningGraphs: 0,
          isPrimary: false,
          isActive: true,
          isNonResponsive: false,
          isBlocked: false,
          runtimeState: "healthy",
          acceptingWork: true,
          reportedAt: new Date().toISOString(),
          health: {},
          isFrontend: false,
          isDatabase: true,
        },
      ],
      serviceInstanceTransports: [
        {
          uuid: "iot-db-internal",
          serviceInstanceId: "iot-db-1",
          role: "internal",
          origin: "http://iot-db-service:3001",
          protocols: ["rest"],
          securityProfile: null,
          authStrategy: null,
        },
        {
          uuid: "iot-db-public",
          serviceInstanceId: "iot-db-1",
          role: "public",
          origin: "http://iot-db.localhost",
          protocols: ["rest"],
          securityProfile: null,
          authStrategy: null,
        },
      ],
    });

    await waitForCondition(() => dependeeRegistrations.length === 1, 1_500);

    expect(dependeeRegistrations[0]).toMatchObject({
      serviceName: "IotDbService",
      serviceInstanceId: "iot-db-1",
      serviceTransportId: "iot-db-internal",
      serviceOrigin: "http://iot-db-service:3001",
      transportProtocols: ["rest"],
    });
    expect(
      registry.instances.get("IotDbService")?.[0]?.clientPendingTransportIds,
    ).toContain(
      buildRouteKey("IotDbService", "internal", "http://iot-db-service:3001"),
    );
    expect(registry.instances.get("IotDbService")?.[0]?.transports).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          uuid: "iot-db-internal",
          origin: "http://iot-db-service:3001",
          role: "internal",
        }),
      ]),
    );
  });

  it("preserves transport and manifest arrays through distributed bootstrap full sync", async () => {
    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "ScheduledRunnerService";
    registry.connectsToCadenzaDB = false;
    registry.serviceInstanceId = "scheduled-runner-1";
    registry.registerBootstrapFullSyncDeputies(() => {}, {});

    vi.spyOn(Cadenza, "inquire").mockResolvedValue({
      serviceInstances: [
        {
          uuid: "cadenza-db-1",
          service_name: "CadenzaDB",
          is_active: true,
          is_non_responsive: false,
          is_blocked: false,
          is_frontend: false,
          health: {},
        },
      ],
      serviceInstanceTransports: [
        {
          uuid: "cadenza-db-internal",
          service_instance_id: "cadenza-db-1",
          role: "internal",
          origin: "http://cadenza-db-service:8080",
          protocols: ["rest", "socket"],
        },
      ],
      serviceManifests: [
        {
          service_instance_id: "scheduled-runner-1",
          manifest: {
            serviceName: "ScheduledRunnerService",
            serviceInstanceId: "scheduled-runner-1",
            revision: 1,
            manifestHash: "runner-manifest-v1",
            publishedAt: "2026-03-30T11:30:00.000Z",
            tasks: [],
            signals: [],
            intents: [],
            actors: [],
            routines: [],
            directionalTaskMaps: [],
            signalTaskMaps: [],
            intentTaskMaps: [],
            actorTaskMaps: [],
            taskToRoutineMaps: [],
          },
        },
      ],
      signalToTaskMaps: [
        {
          signal_name: "global.meta.service_instance.inserted",
          service_name: "ScheduledRunnerService",
          task_name: "Handle Instance Update",
          task_version: 1,
          is_global: true,
        },
      ],
      intentToTaskMaps: [],
      tasks: [],
      signals: [],
      intents: [],
      actors: [],
      routines: [],
      directionalTaskMaps: [],
      actorTaskMaps: [],
      taskToRoutineMaps: [],
      __inquiryMeta: {
        inquiry: "meta-service-registry-full-sync",
      },
    } as any);

    const result = await (registry.fullSyncTask as any).taskFunction({
      __syncing: true,
      __reason: "test_full_sync_transport_manifest_passthrough",
    });

    expect(result).toMatchObject({
      serviceInstanceTransports: [
        expect.objectContaining({
          uuid: "cadenza-db-internal",
          service_instance_id: "cadenza-db-1",
        }),
      ],
      serviceManifests: [
        expect.objectContaining({
          service_instance_id: "scheduled-runner-1",
        }),
      ],
      signalToTaskMaps: [
        expect.objectContaining({
          signal_name: "global.meta.service_instance.inserted",
          service_name: "ScheduledRunnerService",
          task_name: "Handle Instance Update",
          task_version: 1,
        }),
      ],
    });
  });

  it("queries authority control-plane rows directly for CadenzaDB full sync", async () => {
    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "CadenzaDB";
    registry.serviceInstanceId = "cadenza-db-1";

    const authorityQuerySpy = vi
      .spyOn(DatabaseController.instance, "queryAuthorityTableRows")
      .mockImplementation(async (tableName: string) => {
        switch (tableName) {
          case "service_instance":
            return [
              {
                uuid: "runner-1",
                service_name: "ScheduledRunnerService",
                is_active: true,
                is_non_responsive: false,
                is_blocked: false,
                is_frontend: false,
                is_database: false,
                health: {},
              },
            ];
          case "service_instance_lease":
            return [
              {
                service_instance_id: "runner-1",
                status: "active",
                is_ready: true,
                readiness_reason: "accepting_work",
                lease_expires_at: "2026-03-30T12:00:45.000Z",
                last_lease_renewed_at: "2026-03-30T12:00:00.000Z",
                last_ready_at: "2026-03-30T12:00:00.000Z",
              },
            ];
          case "service_instance_transport":
            return [
              {
                uuid: "runner-transport-1",
                service_instance_id: "runner-1",
                role: "internal",
                origin: "http://scheduled-runner:3002",
                protocols: ["rest"],
                deleted: false,
              },
            ];
          case "service_manifest":
            return [
              {
                service_instance_id: "runner-1",
                manifest: {
                  serviceName: "ScheduledRunnerService",
                  serviceInstanceId: "runner-1",
                  revision: 1,
                  manifestHash: "runner-manifest-v1",
                  publishedAt: "2026-03-30T12:00:00.000Z",
                  tasks: [],
                  signals: [],
                  intents: [],
                  actors: [],
                  routines: [],
                  directionalTaskMaps: [],
                  signalToTaskMaps: [],
                  intentToTaskMaps: [],
                  actorTaskMaps: [],
                  taskToRoutineMaps: [],
                },
              },
            ];
          case "signal_to_task_map":
            return [
              {
                signal_name: "global.meta.service_instance.inserted",
                service_name: "ScheduledRunnerService",
                task_name: "Handle Instance Update",
                task_version: 1,
                is_global: true,
                deleted: false,
              },
            ];
          case "intent_to_task_map":
            return [
              {
                intent_name: "scheduled-runner-refresh",
                service_name: "ScheduledRunnerService",
                task_name: "Refresh Runner Cache",
                task_version: 2,
                deleted: false,
              },
            ];
          default:
            return [];
        }
      });

    registry.ensureAuthorityFullSyncResponderTask();

    const result = await Cadenza.inquire("meta-service-registry-full-sync", {
      syncScope: "service-registry-full-sync",
      __syncing: true,
    }, {
      requireComplete: true,
    });

    expect(authorityQuerySpy).toHaveBeenCalledTimes(6);
    expect(result).toMatchObject({
      serviceInstances: [
        expect.objectContaining({
          uuid: "runner-1",
          service_name: "ScheduledRunnerService",
          lease_status: "active",
          is_ready: true,
        }),
      ],
      serviceInstanceTransports: [
        expect.objectContaining({
          uuid: "runner-transport-1",
          service_instance_id: "runner-1",
        }),
      ],
      serviceManifests: [
        expect.objectContaining({
          service_instance_id: "runner-1",
        }),
      ],
      signalToTaskMaps: [
        expect.objectContaining({
          signal_name: "global.meta.service_instance.inserted",
          service_name: "ScheduledRunnerService",
          task_name: "Handle Instance Update",
          task_version: 1,
        }),
      ],
      intentToTaskMaps: [
        expect.objectContaining({
          intent_name: "scheduled-runner-refresh",
          service_name: "ScheduledRunnerService",
          task_name: "Refresh Runner Cache",
          task_version: 2,
        }),
      ],
    });
  });

  it("prefers direct authority routing rows over manifest-derived routing maps during CadenzaDB full sync", async () => {
    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "CadenzaDB";
    registry.serviceInstanceId = "cadenza-db-1";

    vi.spyOn(DatabaseController.instance, "queryAuthorityTableRows").mockImplementation(
      async (tableName: string) => {
        switch (tableName) {
          case "service_instance":
            return [
              {
                uuid: "runner-1",
                service_name: "ScheduledRunnerService",
                is_active: true,
                is_non_responsive: false,
                is_blocked: false,
                is_frontend: false,
                is_database: false,
                health: {},
              },
            ];
          case "service_instance_transport":
            return [
              {
                uuid: "runner-transport-1",
                service_instance_id: "runner-1",
                role: "internal",
                origin: "http://scheduled-runner:3002",
                protocols: ["rest"],
                deleted: false,
              },
            ];
          case "service_manifest":
            return [
              {
                service_instance_id: "runner-1",
                manifest: {
                  serviceName: "ScheduledRunnerService",
                  serviceInstanceId: "runner-1",
                  revision: 1,
                  manifestHash: "runner-manifest-v1",
                  publishedAt: "2026-03-30T12:00:00.000Z",
                  tasks: [],
                  signals: [],
                  intents: [],
                  actors: [],
                  routines: [],
                  directionalTaskMaps: [],
                  signalToTaskMaps: [
                    {
                      signal_name: "global.manifest.only",
                      service_name: "ScheduledRunnerService",
                      task_name: "Manifest Only Signal Handler",
                      task_version: 1,
                      is_global: true,
                    },
                  ],
                  intentToTaskMaps: [
                    {
                      intent_name: "manifest-only-intent",
                      service_name: "ScheduledRunnerService",
                      task_name: "Manifest Only Intent Handler",
                      task_version: 1,
                    },
                  ],
                  actorTaskMaps: [],
                  taskToRoutineMaps: [],
                },
              },
            ];
          case "signal_to_task_map":
            return [
              {
                signal_name: "global.dynamic.only",
                service_name: "ScheduledRunnerService",
                task_name: "Dynamic Signal Handler",
                task_version: 3,
                is_global: true,
                deleted: false,
              },
            ];
          case "intent_to_task_map":
            return [
              {
                intent_name: "dynamic-only-intent",
                service_name: "ScheduledRunnerService",
                task_name: "Dynamic Intent Handler",
                task_version: 4,
                deleted: false,
              },
            ];
          default:
            return [];
        }
      },
    );

    registry.ensureAuthorityFullSyncResponderTask();

    const result = await Cadenza.inquire(
      "meta-service-registry-full-sync",
      {
        syncScope: "service-registry-full-sync",
        __syncing: true,
      },
      {
        requireComplete: true,
      },
    );

    expect(result.signalToTaskMaps).toEqual([
      expect.objectContaining({
        signal_name: "global.dynamic.only",
        service_name: "ScheduledRunnerService",
        task_name: "Dynamic Signal Handler",
        task_version: 3,
      }),
    ]);
    expect(result.intentToTaskMaps).toEqual([
      expect.objectContaining({
        intent_name: "dynamic-only-intent",
        service_name: "ScheduledRunnerService",
        task_name: "Dynamic Intent Handler",
        task_version: 4,
      }),
    ]);
  });

  it("falls back to manifest-derived routing maps when direct authority rows are absent during CadenzaDB full sync", async () => {
    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "CadenzaDB";
    registry.serviceInstanceId = "cadenza-db-1";

    vi.spyOn(DatabaseController.instance, "queryAuthorityTableRows").mockImplementation(
      async (tableName: string) => {
        switch (tableName) {
          case "service_instance":
            return [
              {
                uuid: "runner-1",
                service_name: "ScheduledRunnerService",
                is_active: true,
                is_non_responsive: false,
                is_blocked: false,
                is_frontend: false,
                is_database: false,
                health: {},
              },
            ];
          case "service_instance_transport":
            return [
              {
                uuid: "runner-transport-1",
                service_instance_id: "runner-1",
                role: "internal",
                origin: "http://scheduled-runner:3002",
                protocols: ["rest"],
                deleted: false,
              },
            ];
          case "service_manifest":
            return [
              {
                service_instance_id: "runner-1",
                manifest: {
                  serviceName: "ScheduledRunnerService",
                  serviceInstanceId: "runner-1",
                  revision: 1,
                  manifestHash: "runner-manifest-v1",
                  publishedAt: "2026-03-30T12:00:00.000Z",
                  tasks: [],
                  signals: [],
                  intents: [],
                  actors: [],
                  routines: [],
                  directionalTaskMaps: [],
                  signalToTaskMaps: [
                    {
                      signal_name: "global.manifest.fallback",
                      service_name: "ScheduledRunnerService",
                      task_name: "Manifest Fallback Signal Handler",
                      task_version: 1,
                      is_global: true,
                    },
                  ],
                  intentToTaskMaps: [
                    {
                      intent_name: "manifest-fallback-intent",
                      service_name: "ScheduledRunnerService",
                      task_name: "Manifest Fallback Intent Handler",
                      task_version: 2,
                    },
                  ],
                  actorTaskMaps: [],
                  taskToRoutineMaps: [],
                },
              },
            ];
          case "signal_to_task_map":
          case "intent_to_task_map":
            return [];
          default:
            return [];
        }
      },
    );

    registry.ensureAuthorityFullSyncResponderTask();

    const result = await Cadenza.inquire(
      "meta-service-registry-full-sync",
      {
        syncScope: "service-registry-full-sync",
        __syncing: true,
      },
      {
        requireComplete: true,
      },
    );

    expect(result.signalToTaskMaps).toEqual([
      expect.objectContaining({
        signal_name: "global.manifest.fallback",
        service_name: "ScheduledRunnerService",
        task_name: "Manifest Fallback Signal Handler",
        task_version: 1,
      }),
    ]);
    expect(result.intentToTaskMaps).toEqual([
      expect.objectContaining({
        intent_name: "manifest-fallback-intent",
        service_name: "ScheduledRunnerService",
        task_name: "Manifest Fallback Intent Handler",
        task_version: 2,
      }),
    ]);
  });

  it("requests one extra full sync wave when authority reports a manifest update", async () => {
    const registry = ServiceRegistry.instance as any;
    const syncRequests: Array<Record<string, unknown>> = [];

    registry.serviceName = "ScheduledRunnerService";
    registry.serviceInstanceId = "scheduled-runner-1";
    registry.connectsToCadenzaDB = true;

    Cadenza.createMetaTask("Capture manifest-triggered sync request", (ctx) => {
      syncRequests.push(ctx);
      return true;
    }).doOn("meta.sync_requested");

    Cadenza.emit(AUTHORITY_SERVICE_MANIFEST_UPDATED_SIGNAL, {
      serviceName: "TelemetryCollectorService",
      serviceInstanceId: "telemetry-1",
      revision: 4,
      manifestHash: "manifest-4",
      publishedAt: new Date().toISOString(),
    });

    await waitForCondition(() => syncRequests.length === 1, 1_500);

    expect(syncRequests).toEqual([
      expect.objectContaining({
        __syncing: false,
        __reason: "cadenza_db_manifest_updated",
        __bootstrapFullSync: true,
        __bootstrapFullSyncAttempt: 1,
      }),
    ]);
  });

  it("seeds reserved authority bootstrap intent deputies without intent-to-task maps", () => {
    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "ScheduledRunnerService";
    registry.serviceInstanceId = "scheduled-runner-1";
    registry.connectsToCadenzaDB = true;
    registry.instances.set("CadenzaDB", [
      {
        uuid: "cadenza-db-1",
        serviceName: "CadenzaDB",
        isActive: true,
        isFrontend: false,
        isDatabase: true,
        transports: [
          {
            uuid: "cadenza-db-transport-1",
            serviceInstanceId: "cadenza-db-1",
            role: "internal",
            origin: "http://cadenza-db-service:8080",
            protocols: ["rest", "socket"],
            deleted: false,
          },
        ],
      },
    ]);

    expect(
      Cadenza.inquiryBroker.inquiryObservers.get(
        AUTHORITY_SERVICE_MANIFEST_REPORT_INTENT,
      )?.tasks.size ?? 0,
    ).toBe(0);
    expect(
      Cadenza.inquiryBroker.inquiryObservers.get(
        AUTHORITY_RUNTIME_STATUS_REPORT_INTENT,
      )?.tasks.size ?? 0,
    ).toBe(0);

    expect(
      registry.ensureBootstrapAuthorityControlPlaneForInquiry(
        AUTHORITY_SERVICE_MANIFEST_REPORT_INTENT,
        { __reason: "test" },
      ),
    ).toBe(true);
    expect(
      registry.ensureBootstrapAuthorityControlPlaneForInquiry(
        AUTHORITY_RUNTIME_STATUS_REPORT_INTENT,
        { __reason: "test" },
      ),
    ).toBe(true);

    const deputies = Array.from(registry.remoteIntentDeputiesByKey.values());
    expect(deputies).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          intentName: AUTHORITY_SERVICE_MANIFEST_REPORT_INTENT,
          serviceName: "CadenzaDB",
          remoteTaskName: "Report service manifest to authority",
        }),
        expect.objectContaining({
          intentName: AUTHORITY_RUNTIME_STATUS_REPORT_INTENT,
          serviceName: "CadenzaDB",
          remoteTaskName: "Record authority runtime status report",
        }),
      ]),
    );
    expect(
      Cadenza.inquiryBroker.inquiryObservers.get(
        AUTHORITY_SERVICE_MANIFEST_REPORT_INTENT,
      )?.tasks.size ?? 0,
    ).toBeGreaterThan(0);
    expect(
      Cadenza.inquiryBroker.inquiryObservers.get(
        AUTHORITY_RUNTIME_STATUS_REPORT_INTENT,
      )?.tasks.size ?? 0,
    ).toBeGreaterThan(0);
  });

  it("extends generic CadenzaDB meta deputy timeouts to the bootstrap budget", () => {
    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "ScheduledRunnerService";
    registry.serviceInstanceId = "scheduled-runner-1";
    registry.connectsToCadenzaDB = true;

    registry.registerRemoteIntentDeputy({
      intentName: "meta-test-intent",
      serviceName: "CadenzaDB",
      taskName: "Insert actor",
      taskVersion: 1,
      timeout: 15_000,
    });

    const descriptor = Array.from(registry.remoteIntentDeputiesByKey.values()).find(
      (entry: any) =>
        entry.intentName === "meta-test-intent" &&
        entry.serviceName === "CadenzaDB" &&
        entry.remoteTaskName === "Insert actor",
    ) as any;

    expect(descriptor).toBeTruthy();
    expect(descriptor.localTask?.timeout).toBe(60_000);
  });

  it("requests the direct authority bootstrap handshake when seeding reserved authority deputies without an emitter", async () => {
    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "ScheduledRunnerService";
    registry.serviceInstanceId = "scheduled-runner-1";
    registry.connectsToCadenzaDB = true;
    registry.seedAuthorityBootstrapRoute(
      "http://cadenza-db-service:8080",
      "internal",
    );
    registry.instances.set("CadenzaDB", [
      {
        uuid: "cadenza-db",
        serviceName: "CadenzaDB",
        isActive: true,
        isFrontend: false,
        isDatabase: true,
        isBootstrapPlaceholder: true,
        transports: [
          {
            uuid: "cadenza-db-internal-bootstrap",
            serviceInstanceId: "cadenza-db",
            role: "internal",
            origin: "http://cadenza-db-service:8080",
            protocols: ["rest", "socket"],
            deleted: false,
          },
        ],
        clientCreatedTransportIds: [],
        clientPendingTransportIds: [],
        clientReadyTransportIds: [],
      },
    ]);

    const fetchSpy = vi.spyOn(globalThis, "fetch").mockResolvedValue({
      ok: true,
      json: async () => ({
        __status: "success",
        __serviceInstanceId: "cadenza-db-live-1",
      }),
    } as any);
    const emitSpy = vi.spyOn(Cadenza, "emit");

    expect(
      registry.ensureBootstrapAuthorityControlPlaneForInquiry(
        AUTHORITY_SERVICE_MANIFEST_REPORT_INTENT,
        { __reason: "test" },
      ),
    ).toBe(true);

    await waitForCondition(() => fetchSpy.mock.calls.length === 1, 1_500);

    expect(fetchSpy).toHaveBeenCalledWith(
      "http://cadenza-db-service:8080/handshake",
      expect.objectContaining({
        method: "POST",
      }),
    );
    expect(emitSpy).toHaveBeenCalledWith(
      "meta.fetch.handshake_complete",
      expect.objectContaining({
        serviceName: "CadenzaDB",
        serviceInstanceId: "cadenza-db-live-1",
        serviceTransportId: "cadenza-db-internal-bootstrap",
        serviceOrigin: "http://cadenza-db-service:8080",
        routeKey: "CadenzaDB|internal|http://cadenza-db-service:8080",
        fetchId: buildHandleKey(
          "CadenzaDB|internal|http://cadenza-db-service:8080",
          "rest",
        ),
      }),
    );
  });

  it("routes bootstrap service-instance registration through the reserved authority intent", async () => {
    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "ScheduledRunnerService";
    registry.serviceInstanceId = "scheduled-runner-1";
    registry.connectsToCadenzaDB = true;

    vi.spyOn(Cadenza, "getLocalCadenzaDBInsertTask").mockImplementation(
      (tableName: string) =>
        tableName === "service_instance"
          ? undefined
          : ({
              name: `Insert ${tableName}`,
            } as any),
    );
    const inquireSpy = vi.spyOn(Cadenza, "inquire").mockResolvedValue({
      uuid: "scheduled-runner-1",
      data: {
        uuid: "scheduled-runner-1",
        process_pid: 42,
        service_name: "ScheduledRunnerService",
        is_active: true,
      },
      queryData: {
        data: {
          uuid: "scheduled-runner-1",
          process_pid: 42,
          service_name: "ScheduledRunnerService",
          is_active: true,
        },
      },
    } as any);

    Cadenza.emit("meta.service_registry.instance_registration_requested", {
      data: {
        uuid: "scheduled-runner-1",
        process_pid: 42,
        service_name: "ScheduledRunnerService",
        is_active: true,
      },
      __registrationData: {
        uuid: "scheduled-runner-1",
        process_pid: 42,
        service_name: "ScheduledRunnerService",
        is_active: true,
      },
      __serviceName: "ScheduledRunnerService",
      __serviceInstanceId: "scheduled-runner-1",
    });

    await waitForCondition(() => inquireSpy.mock.calls.length >= 1, 1_500);

    expect(inquireSpy).toHaveBeenCalledWith(
      AUTHORITY_SERVICE_INSTANCE_REGISTER_INTENT,
      expect.objectContaining({
        data: expect.objectContaining({
          uuid: "scheduled-runner-1",
          service_name: "ScheduledRunnerService",
        }),
      }),
      expect.objectContaining({
        requireComplete: true,
      }),
    );
  });

  it("does not emit service-instance resolved when bootstrap authority registration fails", async () => {
    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "ScheduledRunnerService";
    registry.serviceInstanceId = "scheduled-runner-1";
    registry.connectsToCadenzaDB = true;

    vi.spyOn(Cadenza, "getLocalCadenzaDBInsertTask").mockImplementation(
      (tableName: string) =>
        tableName === "service_instance"
          ? undefined
          : ({
              name: `Insert ${tableName}`,
            } as any),
    );
    vi.spyOn(Cadenza, "inquire").mockResolvedValue({
      errored: true,
      __error: "Authority bootstrap route is not established yet",
      data: {
        uuid: "scheduled-runner-1",
        process_pid: 42,
        service_name: "ScheduledRunnerService",
        is_active: true,
      },
      queryData: {
        data: {
          uuid: "scheduled-runner-1",
          process_pid: 42,
          service_name: "ScheduledRunnerService",
          is_active: true,
        },
      },
    } as any);

    const emitSpy = vi.fn();
    const result = await registry.insertServiceInstanceTask.taskFunction(
      {
        data: {
          uuid: "scheduled-runner-1",
          process_pid: 42,
          service_name: "ScheduledRunnerService",
          is_active: true,
        },
        __registrationData: {
          uuid: "scheduled-runner-1",
          process_pid: 42,
          service_name: "ScheduledRunnerService",
          is_active: true,
        },
        __serviceName: "ScheduledRunnerService",
        __serviceInstanceId: "scheduled-runner-1",
      },
      emitSpy,
    );

    expect(result).toMatchObject({
      errored: true,
    });
    expect(emitSpy).not.toHaveBeenCalledWith(
      "meta.service_registry.insert_resolved:service_instance",
      expect.anything(),
    );
  });

  it("routes bootstrap service transport registration through the reserved authority intent", async () => {
    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "ScheduledRunnerService";
    registry.serviceInstanceId = "scheduled-runner-1";
    registry.connectsToCadenzaDB = true;

    vi.spyOn(Cadenza, "getLocalCadenzaDBInsertTask").mockImplementation(
      (tableName: string) =>
        tableName === "service_instance_transport"
          ? undefined
          : ({
              name: `Insert ${tableName}`,
            } as any),
    );
    const inquireSpy = vi.spyOn(Cadenza, "inquire").mockResolvedValue({
      uuid: "scheduled-runner-transport-1",
      data: {
        uuid: "scheduled-runner-transport-1",
        service_instance_id: "scheduled-runner-1",
        role: "internal",
        origin: "http://scheduled-runner:3002",
        protocols: ["rest"],
      },
      queryData: {
        data: {
          uuid: "scheduled-runner-transport-1",
          service_instance_id: "scheduled-runner-1",
          role: "internal",
          origin: "http://scheduled-runner:3002",
          protocols: ["rest"],
        },
      },
    } as any);

    Cadenza.emit("meta.service_registry.transport_registration_requested", {
      data: {
        uuid: "scheduled-runner-transport-1",
        service_instance_id: "scheduled-runner-1",
        role: "internal",
        origin: "http://scheduled-runner:3002",
        protocols: ["rest"],
      },
      __registrationData: {
        uuid: "scheduled-runner-transport-1",
        service_instance_id: "scheduled-runner-1",
        role: "internal",
        origin: "http://scheduled-runner:3002",
        protocols: ["rest"],
      },
      __serviceName: "ScheduledRunnerService",
      __serviceInstanceId: "scheduled-runner-1",
    });

    await waitForCondition(() => inquireSpy.mock.calls.length === 1, 1_500);

    expect(inquireSpy).toHaveBeenCalledWith(
      AUTHORITY_SERVICE_INSTANCE_TRANSPORT_REGISTER_INTENT,
      expect.objectContaining({
        data: expect.objectContaining({
          uuid: "scheduled-runner-transport-1",
          service_instance_id: "scheduled-runner-1",
        }),
      }),
      expect.objectContaining({
        requireComplete: true,
      }),
    );
  });

  it("slims bootstrap service transport registration context before authority delegation", async () => {
    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "ScheduledRunnerService";
    registry.serviceInstanceId = "scheduled-runner-1";
    registry.connectsToCadenzaDB = true;

    vi.spyOn(Cadenza, "getLocalCadenzaDBInsertTask").mockImplementation(
      (tableName: string) =>
        tableName === "service_instance_transport"
          ? undefined
          : ({
              name: `Insert ${tableName}`,
            } as any),
    );
    const inquireSpy = vi.spyOn(Cadenza, "inquire").mockResolvedValue({
      uuid: "scheduled-runner-transport-1",
      data: {
        uuid: "scheduled-runner-transport-1",
        service_instance_id: "scheduled-runner-1",
        role: "internal",
        origin: "http://scheduled-runner:3002",
        protocols: ["rest"],
      },
      queryData: {
        data: {
          uuid: "scheduled-runner-transport-1",
          service_instance_id: "scheduled-runner-1",
          role: "internal",
          origin: "http://scheduled-runner:3002",
          protocols: ["rest"],
        },
      },
    } as any);

    Cadenza.emit("meta.service_registry.transport_registration_requested", {
      data: {
        name: "Do for each instance",
        description: "Large meta task payload that should not reach authority inserts.",
        signals: {
          emits: ["meta.task.created"],
        },
      },
      __registrationData: {
        uuid: "scheduled-runner-transport-1",
        service_instance_id: "scheduled-runner-1",
        role: "internal",
        origin: "http://scheduled-runner:3002",
        protocols: ["rest"],
      },
      __serviceName: "ScheduledRunnerService",
      __serviceInstanceId: "scheduled-runner-1",
      __declaredTransports: [
        {
          uuid: "scheduled-runner-transport-1",
          role: "internal",
          origin: "http://scheduled-runner:3002",
          protocols: ["rest"],
        },
      ],
      httpServer: {
        keepAliveTimeout: 5000,
      },
      serviceInstance: {
        uuid: "scheduled-runner-1",
        serviceName: "ScheduledRunnerService",
      },
    });

    await waitForCondition(() => inquireSpy.mock.calls.length === 1, 1_500);

    const delegatedContext = inquireSpy.mock.calls[0]?.[1] as Record<string, any>;

    expect(delegatedContext.data).toMatchObject({
      uuid: "scheduled-runner-transport-1",
      service_instance_id: "scheduled-runner-1",
      role: "internal",
      origin: "http://scheduled-runner:3002",
    });
    expect(delegatedContext.queryData).toMatchObject({
      data: {
        uuid: "scheduled-runner-transport-1",
        service_instance_id: "scheduled-runner-1",
        role: "internal",
        origin: "http://scheduled-runner:3002",
        protocols: ["rest"],
      },
    });
    expect(delegatedContext.httpServer).toBeUndefined();
    expect(delegatedContext.__declaredTransports).toBeUndefined();
    expect(delegatedContext.serviceInstance).toBeUndefined();
    expect(delegatedContext.__resolverOriginalContext).toBeUndefined();
  });

  it("preserves service-instance setup metadata while stripping bulky authority insert fields", async () => {
    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "ScheduledRunnerService";
    registry.serviceInstanceId = "scheduled-runner-1";
    registry.connectsToCadenzaDB = true;

    vi.spyOn(Cadenza, "getLocalCadenzaDBInsertTask").mockImplementation(
      (tableName: string) =>
        tableName === "service_instance"
          ? undefined
          : ({
              name: `Insert ${tableName}`,
            } as any),
    );
    const inquireSpy = vi.spyOn(Cadenza, "inquire").mockResolvedValue({
      uuid: "scheduled-runner-1",
      data: {
        uuid: "scheduled-runner-1",
        process_pid: 42,
        service_name: "ScheduledRunnerService",
        is_active: true,
      },
      queryData: {
        data: {
          uuid: "scheduled-runner-1",
          process_pid: 42,
          service_name: "ScheduledRunnerService",
          is_active: true,
        },
      },
    } as any);

    await registry.insertServiceInstanceTask.taskFunction(
      {
        data: {
          uuid: "scheduled-runner-1",
          process_pid: 42,
          service_name: "ScheduledRunnerService",
          is_active: true,
        },
        __registrationData: {
          uuid: "scheduled-runner-1",
          process_pid: 42,
          service_name: "ScheduledRunnerService",
          is_active: true,
        },
        __serviceName: "ScheduledRunnerService",
        __serviceInstanceId: "scheduled-runner-1",
        __transportData: [
          {
            uuid: "scheduled-runner-transport-1",
            service_instance_id: "scheduled-runner-1",
            role: "internal",
            origin: "http://scheduled-runner:3002",
            protocols: ["rest"],
          },
        ],
        __useSocket: true,
        __retryCount: 3,
        __isFrontend: false,
        __declaredTransports: [
          {
            uuid: "scheduled-runner-transport-1",
            role: "internal",
            origin: "http://scheduled-runner:3002",
            protocols: ["rest"],
          },
        ],
        httpServer: {
          keepAliveTimeout: 5000,
        },
      },
      Cadenza.emit.bind(Cadenza),
    );

    expect(inquireSpy).toHaveBeenCalledTimes(1);

    const delegatedContext = inquireSpy.mock.calls[0]?.[1] as Record<string, any>;

    expect(delegatedContext.__transportData).toEqual([
      expect.objectContaining({
        uuid: "scheduled-runner-transport-1",
        service_instance_id: "scheduled-runner-1",
        role: "internal",
      }),
    ]);
    expect(delegatedContext.__useSocket).toBe(true);
    expect(delegatedContext.httpServer).toBeUndefined();
    expect(delegatedContext.__declaredTransports).toBeUndefined();
    expect(delegatedContext.queryData).toMatchObject({
      data: {
        uuid: "scheduled-runner-1",
        process_pid: 42,
        service_name: "ScheduledRunnerService",
        is_active: true,
      },
    });
    expect(delegatedContext.__resolverOriginalContext).toBeUndefined();
  });

  it("creates reserved authority bootstrap signal transmissions without signal-to-task maps", () => {
    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "CadenzaDB";
    registry.serviceInstanceId = "cadenza-db-1";
    registry.instances.set("ScheduledRunnerService", [
      {
        uuid: "scheduled-runner-1",
        serviceName: "ScheduledRunnerService",
        isActive: true,
        isFrontend: false,
        isDatabase: false,
        transports: [
          {
            uuid: "scheduled-runner-transport-1",
            serviceInstanceId: "scheduled-runner-1",
            role: "internal",
            origin: "http://scheduled-runner:3002",
            protocols: ["rest"],
            deleted: false,
          },
        ],
      },
    ]);

    expect(
      Cadenza.get(
        `Transmit signal: ${AUTHORITY_SERVICE_MANIFEST_UPDATED_SIGNAL} to ScheduledRunnerService`,
      ),
    ).toBeUndefined();

    expect(registry.ensureAuthorityBootstrapSignalTransmissions()).toBe(true);

    expect(
      registry.remoteSignals.get("ScheduledRunnerService")?.has(
        AUTHORITY_SERVICE_MANIFEST_UPDATED_SIGNAL,
      ),
    ).toBe(true);
  });

  it("rechecks reserved authority responders after bootstrap control-plane seeding", async () => {
    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "ScheduledRunnerService";
    registry.serviceInstanceId = "scheduled-runner-1";
    registry.connectsToCadenzaDB = true;

    const ensureSpy = vi
      .spyOn(registry, "ensureBootstrapAuthorityControlPlaneForInquiry")
      .mockImplementation((intentName: string) => {
        Cadenza.createMetaTask(
          "Fake authority bootstrap manifest responder",
          () => ({
            applied: true,
          }),
          "",
          {
            register: false,
            isHidden: true,
          },
        ).respondsTo(intentName);
        return true;
      });

    const result = await Cadenza.inquire(
      AUTHORITY_SERVICE_MANIFEST_REPORT_INTENT,
      {},
      {
        requireComplete: true,
      },
    );

    expect(result).toMatchObject({
      applied: true,
    });
    expect(ensureSpy).toHaveBeenCalledWith(
      AUTHORITY_SERVICE_MANIFEST_REPORT_INTENT,
      {},
    );
  });

  it("routes reserved authority manifest inquiries through the post-handshake bootstrap channel", async () => {
    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "ScheduledRunnerService";
    registry.serviceInstanceId = "scheduled-runner-1";
    registry.connectsToCadenzaDB = true;
    registry.seedAuthorityBootstrapRoute(
      "http://cadenza-db-service:8080",
      "internal",
    );
    registry.noteAuthorityBootstrapHandshake({
      serviceName: "CadenzaDB",
      serviceInstanceId: "cadenza-db-live-1",
      serviceTransportId: "cadenza-db-transport-1",
      serviceOrigin: "http://cadenza-db-service:8080",
    });

    const fetchSpy = vi
      .spyOn(globalThis, "fetch")
      .mockResolvedValue({
        ok: true,
        json: async () => ({
          __status: "success",
          applied: true,
          authorityInstanceId: "cadenza-db-live-1",
        }),
      } as any);

    registry.ensureBootstrapAuthorityControlPlaneForInquiry(
      AUTHORITY_SERVICE_MANIFEST_REPORT_INTENT,
      {},
    );

    const result = await Cadenza.inquire(
      AUTHORITY_SERVICE_MANIFEST_REPORT_INTENT,
      {
        serviceName: "ScheduledRunnerService",
        serviceInstanceId: "scheduled-runner-1",
        __resolverOriginalContext: {
          joinedContexts: [{ huge: true }],
          data: {
            name: "Large bootstrap task payload",
          },
        },
        __resolverQueryData: {
          data: {
            name: "Large bootstrap query payload",
          },
        },
        __declaredTransports: [
          {
            uuid: "scheduled-runner-transport-1",
            role: "internal",
            origin: "http://scheduled-runner:3002",
            protocols: ["rest"],
          },
        ],
        httpServer: {
          keepAliveTimeout: 5000,
        },
        joinedContexts: [
          {
            payload: "should-not-cross-bootstrap-channel",
          },
        ],
      },
      {
        requireComplete: true,
      },
    );

    expect(result).toMatchObject({
      applied: true,
      authorityInstanceId: "cadenza-db-live-1",
    });
    expect(fetchSpy).toHaveBeenCalledWith(
      "http://cadenza-db-service:8080/delegation",
      expect.objectContaining({
        method: "POST",
      }),
    );

    const body = JSON.parse(String(fetchSpy.mock.calls[0]?.[1]?.body ?? "{}"));
    expect(body).toMatchObject({
      __remoteRoutineName: "Report service manifest to authority",
      __serviceName: "CadenzaDB",
      __routeKey: "CadenzaDB|internal|http://cadenza-db-service:8080",
      __fetchId: buildHandleKey(
        "CadenzaDB|internal|http://cadenza-db-service:8080",
        "rest",
      ),
    });
    expect(body.targetServiceInstanceId).toBeUndefined();
    expect(body.__transportId).toBeUndefined();
    expect(body.__resolverOriginalContext).toBeUndefined();
    expect(body.__resolverQueryData).toBeUndefined();
    expect(body.__declaredTransports).toBeUndefined();
    expect(body.httpServer).toBeUndefined();
    expect(body.joinedContexts).toBeUndefined();
  });

  it("strips duplicate db operation payload keys from bootstrap delegation bodies", async () => {
    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "ScheduledRunnerService";
    registry.serviceInstanceId = "scheduled-runner-1";
    registry.connectsToCadenzaDB = true;
    registry.seedAuthorityBootstrapRoute(
      "http://cadenza-db-service:8080",
      "internal",
    );
    registry.noteAuthorityBootstrapHandshake({
      serviceName: "CadenzaDB",
      serviceInstanceId: "cadenza-db-live-1",
      serviceTransportId: "cadenza-db-transport-1",
      serviceOrigin: "http://cadenza-db-service:8080",
    });

    const fetchSpy = vi.spyOn(globalThis, "fetch").mockResolvedValue({
      ok: true,
      json: async () => ({
        __status: "success",
        applied: true,
      }),
    } as any);

    await registry.invokeAuthorityBootstrapRoutine(
      "Insert task",
      {
        data: {
          name: "Insert service_instance",
          function_string: "function () { return true; }",
        },
        transaction: true,
        onConflict: {
          target: ["name"],
          action: {
            do: "update",
            set: {
              function_string: "excluded",
            },
          },
        },
        queryData: {
          data: {
            name: "Insert service_instance",
            function_string: "function () { return true; }",
          },
          transaction: true,
          onConflict: {
            target: ["name"],
            action: {
              do: "update",
              set: {
                function_string: "excluded",
              },
            },
          },
        },
      },
      5_000,
    );

    const body = JSON.parse(String(fetchSpy.mock.calls[0]?.[1]?.body ?? "{}"));
    expect(body.queryData).toMatchObject({
      data: {
        name: "Insert service_instance",
        function_string: "function () { return true; }",
      },
      transaction: true,
      onConflict: {
        target: ["name"],
      },
    });
    expect(body.data).toBeUndefined();
    expect(body.transaction).toBeUndefined();
    expect(body.onConflict).toBeUndefined();
  });

  it("hoists root db operation payload keys into queryData for bootstrap delegation bodies", async () => {
    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "ScheduledRunnerService";
    registry.serviceInstanceId = "scheduled-runner-1";
    registry.connectsToCadenzaDB = true;
    registry.seedAuthorityBootstrapRoute(
      "http://cadenza-db-service:8080",
      "internal",
    );
    registry.noteAuthorityBootstrapHandshake({
      serviceName: "CadenzaDB",
      serviceInstanceId: "cadenza-db-live-1",
      serviceTransportId: "cadenza-db-transport-1",
      serviceOrigin: "http://cadenza-db-service:8080",
    });

    const fetchSpy = vi.spyOn(globalThis, "fetch").mockResolvedValue({
      ok: true,
      json: async () => ({
        __status: "success",
        applied: true,
      }),
    } as any);

    await registry.invokeAuthorityBootstrapRoutine(
      "Insert intent_registry",
      {
        data: {
          name: "Generate health summary",
          input: { type: "object" },
          output: { type: "object" },
        },
        onConflict: {
          target: ["name"],
          action: {
            do: "nothing",
          },
        },
      },
      5_000,
    );

    const body = JSON.parse(String(fetchSpy.mock.calls[0]?.[1]?.body ?? "{}"));
    expect(body.queryData).toMatchObject({
      data: {
        name: "Generate health summary",
        input: { type: "object" },
        output: { type: "object" },
      },
      onConflict: {
        target: ["name"],
      },
    });
    expect(body.data).toBeUndefined();
    expect(body.onConflict).toBeUndefined();
  });

  it("does not immediately re-request authority bootstrap handshake from service lifecycle not-responding signals", () => {
    vi.useFakeTimers();
    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "ScheduledRunnerService";
    registry.serviceInstanceId = "scheduled-runner-1";
    registry.connectsToCadenzaDB = true;
    registry.bootstrapFullSyncRetryJitterRatio = 0;
    registry.seedAuthorityBootstrapRoute(
      "http://cadenza-db-service:8080",
      "internal",
    );
    registry.noteAuthorityBootstrapHandshake({
      serviceName: "CadenzaDB",
      serviceInstanceId: "cadenza-db-old-1",
      serviceTransportId: "cadenza-db-transport-old-1",
      serviceOrigin: "http://cadenza-db-service:8080",
    });

    const requestHandshakeSpy = vi
      .spyOn(registry, "requestAuthorityBootstrapHandshake")
      .mockReturnValue(true);

    Cadenza.emit("meta.service_registry.service_not_responding", {
      serviceName: "CadenzaDB",
      serviceInstanceId: "cadenza-db-old-1",
      serviceOrigin: "http://cadenza-db-service:8080",
    });

    expect(requestHandshakeSpy).not.toHaveBeenCalled();
  });

  it("ignores stale CadenzaDB transport failures once a newer authority bootstrap route is established", async () => {
    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "ScheduledRunnerService";
    registry.serviceInstanceId = "scheduled-runner-1";
    registry.connectsToCadenzaDB = true;
    registry.seedAuthorityBootstrapRoute(
      "http://cadenza-db-service:8080",
      "internal",
    );
    registry.noteAuthorityBootstrapHandshake({
      serviceName: "CadenzaDB",
      serviceInstanceId: "cadenza-db-live-2",
      serviceTransportId: "cadenza-db-transport-live-2",
      serviceOrigin: "http://cadenza-db-service:8080",
    });

    const requestHandshakeSpy = vi
      .spyOn(registry, "requestAuthorityBootstrapHandshake")
      .mockReturnValue(true);

    Cadenza.emit("meta.fetch.handshake_failed", {
      serviceName: "CadenzaDB",
      serviceInstanceId: "cadenza-db-old-1",
      serviceTransportId: "cadenza-db-transport-old-1",
      serviceOrigin: "http://cadenza-db-service:8080",
      __transportId: "cadenza-db-transport-old-1",
      __transportOrigin: "http://cadenza-db-service:8080",
      __routeKey: "CadenzaDB|internal|http://cadenza-db-service:8080",
      __fetchId: buildHandleKey(
        "CadenzaDB|internal|http://cadenza-db-service:8080",
        "rest",
      ),
    });

    await new Promise((resolve) => setTimeout(resolve, 50));

    expect(
      requestHandshakeSpy.mock.calls.some(
        ([ctx]) => ctx?.__reason === "cadenza_db_unreachable",
      ),
    ).toBe(false);
    expect(registry.hasAuthorityBootstrapHandshakeEstablished()).toBe(true);
  });

  it("retries current non-authority routes after a fetch handshake failure without marking the instance non-responsive", async () => {
    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "TelemetryCollectorService";
    registry.serviceInstanceId = "telemetry-1";

    const predictorInstance = {
      uuid: "predictor-2",
      serviceName: "PredictorService",
      isFrontend: false,
      isDatabase: false,
      isActive: true,
      isNonResponsive: false,
      isBlocked: false,
      numberOfRunningGraphs: 0,
      acceptingWork: true,
      reportedAt: new Date().toISOString(),
      transports: [
        {
          uuid: "predictor-transport-2",
          serviceInstanceId: "predictor-2",
          role: "internal",
          origin: "http://predictor-b:3005",
          protocols: ["rest"],
          deleted: false,
        },
      ],
      clientCreatedTransportIds: [],
      clientPendingTransportIds: [],
      clientReadyTransportIds: [],
    };

    registry.instances.set("PredictorService", [predictorInstance]);
    registry.deputies.set("PredictorService", [
      {
        serviceName: "PredictorService",
        intentName: "iot-prediction-compute",
        localTaskName: "Normalize prediction compute input (Proxy)",
      },
    ]);
    registry.upsertRemoteRouteRecord(
      "PredictorService",
      "predictor-2",
      predictorInstance.transports[0],
    );

    const scheduleSpy = vi
      .spyOn(Cadenza, "schedule")
      .mockImplementation(() => undefined as any);

    Cadenza.emit("meta.fetch.handshake_failed", {
      __signalName: "meta.fetch.handshake_failed",
      serviceName: "PredictorService",
      serviceInstanceId: "predictor-2",
      serviceTransportId: "predictor-transport-2",
      serviceOrigin: "http://predictor-b:3005",
      routeKey: "PredictorService|internal|http://predictor-b:3005",
      __routeKey: "PredictorService|internal|http://predictor-b:3005",
      __transportId: "predictor-transport-2",
      __transportOrigin: "http://predictor-b:3005",
      communicationTypes: ["delegation"],
    });

    await waitForCondition(
      () =>
        scheduleSpy.mock.calls.some(
          ([signal]) => signal === "meta.service_registry.dependee_registered",
        ),
      1_500,
    );

    expect(scheduleSpy).toHaveBeenCalledWith(
      "meta.service_registry.dependee_registered",
      expect.objectContaining({
        serviceName: "PredictorService",
        serviceInstanceId: "predictor-2",
        serviceTransportId: "predictor-transport-2",
        serviceOrigin: "http://predictor-b:3005",
        routeKey: "PredictorService|internal|http://predictor-b:3005",
        __reason: "fetch_handshake_failed_retry",
      }),
      expect.any(Number),
    );
    expect(
      registry.instances.get("PredictorService")?.[0]?.isNonResponsive,
    ).toBe(false);
  });

  it("demotes a dead route after a hard fetch handshake failure instead of retrying it", async () => {
    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "ScheduledRunnerService";
    registry.serviceInstanceId = "scheduled-runner-1";

    const telemetryInstance = {
      uuid: "telemetry-b-1",
      serviceName: "TelemetryCollectorService",
      isFrontend: false,
      isDatabase: false,
      isActive: true,
      isNonResponsive: false,
      isBlocked: false,
      numberOfRunningGraphs: 0,
      acceptingWork: true,
      reportedAt: new Date().toISOString(),
      transports: [
        {
          uuid: "telemetry-b-transport-1",
          serviceInstanceId: "telemetry-b-1",
          role: "internal",
          origin: "http://telemetry-collector-b:3003",
          protocols: ["rest"],
          deleted: false,
        },
      ],
      clientCreatedTransportIds: [],
      clientPendingTransportIds: [],
      clientReadyTransportIds: [],
    };

    registry.instances.set("TelemetryCollectorService", [telemetryInstance]);
    registry.deputies.set("TelemetryCollectorService", [
      {
        serviceName: "TelemetryCollectorService",
        intentName: "iot-telemetry-ingest",
        localTaskName: "Normalize telemetry ingest payload (Proxy)",
      },
    ]);
    registry.upsertRemoteRouteRecord(
      "TelemetryCollectorService",
      "telemetry-b-1",
      telemetryInstance.transports[0],
    );

    const scheduleSpy = vi
      .spyOn(Cadenza, "schedule")
      .mockImplementation(() => undefined as any);

    Cadenza.emit("meta.fetch.handshake_failed", {
      __signalName: "meta.fetch.handshake_failed",
      __error:
        "FetchError: request to http://telemetry-collector-b:3003/handshake failed, reason: getaddrinfo ENOTFOUND telemetry-collector-b",
      serviceName: "TelemetryCollectorService",
      serviceInstanceId: "telemetry-b-1",
      serviceTransportId: "telemetry-b-transport-1",
      serviceOrigin: "http://telemetry-collector-b:3003",
      routeKey: "TelemetryCollectorService|internal|http://telemetry-collector-b:3003",
      __routeKey: "TelemetryCollectorService|internal|http://telemetry-collector-b:3003",
      __transportId: "telemetry-b-transport-1",
      __transportOrigin: "http://telemetry-collector-b:3003",
      communicationTypes: ["delegation", "signal"],
    });

    await new Promise((resolve) => setTimeout(resolve, 50));

    expect(
      scheduleSpy.mock.calls.some(
        ([signal]) => signal === "meta.service_registry.dependee_registered",
      ),
    ).toBe(false);
    expect(
      registry.instances.get("TelemetryCollectorService")?.[0]?.isNonResponsive,
    ).toBe(true);
    expect(
      registry.remoteRoutesByKey.has(
        "TelemetryCollectorService|internal|http://telemetry-collector-b:3003",
      ),
    ).toBe(false);
  });

  it("retries a timed out fetch handshake without blacklisting a live route", async () => {
    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "TelemetryCollectorService";
    registry.serviceInstanceId = "telemetry-1";

    const predictorInstance = {
      uuid: "predictor-2",
      serviceName: "PredictorService",
      isFrontend: false,
      isDatabase: false,
      isActive: true,
      isNonResponsive: false,
      isBlocked: false,
      numberOfRunningGraphs: 0,
      acceptingWork: true,
      reportedAt: new Date().toISOString(),
      transports: [
        {
          uuid: "predictor-transport-2",
          serviceInstanceId: "predictor-2",
          role: "internal",
          origin: "http://predictor:3005",
          protocols: ["rest"],
          deleted: false,
        },
      ],
      clientCreatedTransportIds: [],
      clientPendingTransportIds: [],
      clientReadyTransportIds: [],
    };

    registry.instances.set("PredictorService", [predictorInstance]);
    registry.deputies.set("PredictorService", [
      {
        serviceName: "PredictorService",
        intentName: "iot-prediction-compute",
        localTaskName: "Normalize prediction compute input (Proxy)",
      },
    ]);
    registry.upsertRemoteRouteRecord(
      "PredictorService",
      "predictor-2",
      predictorInstance.transports[0],
    );

    const scheduleSpy = vi
      .spyOn(Cadenza, "schedule")
      .mockImplementation(() => undefined as any);

    Cadenza.emit("meta.fetch.handshake_failed", {
      __signalName: "meta.fetch.handshake_failed",
      __error:
        "FetchError: request to http://predictor:3005/handshake failed, reason: ETIMEDOUT",
      serviceName: "PredictorService",
      serviceInstanceId: "predictor-2",
      serviceTransportId: "predictor-transport-2",
      serviceOrigin: "http://predictor:3005",
      routeKey: "PredictorService|internal|http://predictor:3005",
      __routeKey: "PredictorService|internal|http://predictor:3005",
      __transportId: "predictor-transport-2",
      __transportOrigin: "http://predictor:3005",
      communicationTypes: ["delegation"],
    });

    await waitForCondition(
      () =>
        scheduleSpy.mock.calls.some(
          ([signal]) => signal === "meta.service_registry.dependee_registered",
        ),
      1_500,
    );

    expect(scheduleSpy).toHaveBeenCalledWith(
      "meta.service_registry.dependee_registered",
      expect.objectContaining({
        serviceName: "PredictorService",
        serviceInstanceId: "predictor-2",
        serviceTransportId: "predictor-transport-2",
        serviceOrigin: "http://predictor:3005",
        routeKey: "PredictorService|internal|http://predictor:3005",
        __reason: "fetch_handshake_failed_retry",
      }),
      expect.any(Number),
    );
    expect(registry.instances.get("PredictorService")?.[0]?.isNonResponsive).toBe(
      false,
    );
    expect(
      registry.remoteRoutesByKey.has(
        "PredictorService|internal|http://predictor:3005",
      ),
    ).toBe(true);
  });

  it("retries a refused fetch handshake during startup without blacklisting a live route", async () => {
    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "TelemetryCollectorService";
    registry.serviceInstanceId = "telemetry-1";

    const predictorInstance = {
      uuid: "predictor-3",
      serviceName: "PredictorService",
      isFrontend: false,
      isDatabase: false,
      isActive: true,
      isNonResponsive: false,
      isBlocked: false,
      numberOfRunningGraphs: 0,
      acceptingWork: true,
      reportedAt: new Date().toISOString(),
      transports: [
        {
          uuid: "predictor-transport-3",
          serviceInstanceId: "predictor-3",
          role: "internal",
          origin: "http://predictor:3005",
          protocols: ["rest"],
          deleted: false,
        },
      ],
      clientCreatedTransportIds: [],
      clientPendingTransportIds: [],
      clientReadyTransportIds: [],
    };

    registry.instances.set("PredictorService", [predictorInstance]);
    registry.deputies.set("PredictorService", [
      {
        serviceName: "PredictorService",
        intentName: "iot-prediction-compute",
        localTaskName: "Normalize prediction compute input (Proxy)",
      },
    ]);
    registry.upsertRemoteRouteRecord(
      "PredictorService",
      "predictor-3",
      predictorInstance.transports[0],
    );

    const scheduleSpy = vi
      .spyOn(Cadenza, "schedule")
      .mockImplementation(() => undefined as any);

    Cadenza.emit("meta.fetch.handshake_failed", {
      __signalName: "meta.fetch.handshake_failed",
      __error:
        "FetchError: request to http://predictor:3005/handshake failed, reason: connect ECONNREFUSED 172.18.0.7:3005",
      serviceName: "PredictorService",
      serviceInstanceId: "predictor-3",
      serviceTransportId: "predictor-transport-3",
      serviceOrigin: "http://predictor:3005",
      routeKey: "PredictorService|internal|http://predictor:3005",
      __routeKey: "PredictorService|internal|http://predictor:3005",
      __transportId: "predictor-transport-3",
      __transportOrigin: "http://predictor:3005",
      communicationTypes: ["delegation"],
    });

    await waitForCondition(
      () =>
        scheduleSpy.mock.calls.some(
          ([signal]) => signal === "meta.service_registry.dependee_registered",
        ),
      1_500,
    );

    expect(scheduleSpy).toHaveBeenCalledWith(
      "meta.service_registry.dependee_registered",
      expect.objectContaining({
        serviceName: "PredictorService",
        serviceInstanceId: "predictor-3",
        serviceTransportId: "predictor-transport-3",
        serviceOrigin: "http://predictor:3005",
        routeKey: "PredictorService|internal|http://predictor:3005",
        __reason: "fetch_handshake_failed_retry",
      }),
      expect.any(Number),
    );
    expect(registry.instances.get("PredictorService")?.[0]?.isNonResponsive).toBe(
      false,
    );
    expect(
      registry.remoteRoutesByKey.has(
        "PredictorService|internal|http://predictor:3005",
      ),
    ).toBe(true);
  });

  it("retries an aborted authority fetch handshake without blacklisting the authority", async () => {
    vi.useFakeTimers();
    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "ScheduledRunnerService";
    registry.serviceInstanceId = "scheduled-runner-1";
    registry.connectsToCadenzaDB = true;
    registry.bootstrapFullSyncRetryJitterRatio = 0;
    registry.seedAuthorityBootstrapRoute(
      "http://cadenza-db-service:8080",
      "internal",
    );
    registry.noteAuthorityBootstrapHandshake({
      serviceName: "CadenzaDB",
      serviceInstanceId: "cadenza-db-1",
      serviceTransportId: "cadenza-db-transport-1",
      serviceOrigin: "http://cadenza-db-service:8080",
    });

    const authorityInstance = {
      uuid: "cadenza-db-1",
      serviceName: "CadenzaDB",
      isFrontend: false,
      isDatabase: true,
      isActive: true,
      isNonResponsive: false,
      isBlocked: false,
      numberOfRunningGraphs: 0,
      acceptingWork: true,
      reportedAt: new Date().toISOString(),
      transports: [
        {
          uuid: "cadenza-db-transport-1",
          serviceInstanceId: "cadenza-db-1",
          role: "internal",
          origin: "http://cadenza-db-service:8080",
          protocols: ["rest", "socket"],
          deleted: false,
        },
      ],
      clientCreatedTransportIds: [],
      clientPendingTransportIds: [],
      clientReadyTransportIds: [],
    };

    registry.instances.set("CadenzaDB", [authorityInstance]);
    registry.upsertRemoteRouteRecord(
      "CadenzaDB",
      "cadenza-db-1",
      authorityInstance.transports[0],
    );

    const requestHandshakeSpy = vi
      .spyOn(registry, "requestAuthorityBootstrapHandshake")
      .mockReturnValue(true);

    Cadenza.emit("meta.fetch.handshake_failed", {
      __signalName: "meta.fetch.handshake_failed",
      __error: "AbortError: The operation was aborted.",
      serviceName: "CadenzaDB",
      serviceInstanceId: "cadenza-db-1",
      serviceTransportId: "cadenza-db-transport-1",
      serviceOrigin: "http://cadenza-db-service:8080",
      routeKey: "CadenzaDB|internal|http://cadenza-db-service:8080",
      __routeKey: "CadenzaDB|internal|http://cadenza-db-service:8080",
      __transportId: "cadenza-db-transport-1",
      __transportOrigin: "http://cadenza-db-service:8080",
      communicationTypes: ["delegation", "signal"],
    });

    expect(requestHandshakeSpy).not.toHaveBeenCalled();

    await vi.advanceTimersByTimeAsync(1_000);

    expect(registry.instances.get("CadenzaDB")?.[0]?.isNonResponsive).toBe(false);
    expect(
      registry.remoteRoutesByKey.has(
        "CadenzaDB|internal|http://cadenza-db-service:8080",
      ),
    ).toBe(true);
    expect(registry.hasAuthorityBootstrapHandshakeEstablished()).toBe(false);
  });

  it("demotes a dead route after a hard fetch delegation failure instead of keeping the stale hostname", async () => {
    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "IotDbService";
    registry.serviceInstanceId = "iot-db-1";

    const telemetryInstance = {
      uuid: "telemetry-b-1",
      serviceName: "TelemetryCollectorService",
      isFrontend: false,
      isDatabase: false,
      isActive: true,
      isNonResponsive: false,
      isBlocked: false,
      numberOfRunningGraphs: 0,
      acceptingWork: true,
      reportedAt: new Date().toISOString(),
      transports: [
        {
          uuid: "telemetry-b-transport-1",
          serviceInstanceId: "telemetry-b-1",
          role: "internal",
          origin: "http://telemetry-collector-b:3003",
          protocols: ["rest"],
          deleted: false,
        },
      ],
      clientCreatedTransportIds: ["TelemetryCollectorService|internal|http://telemetry-collector-b:3003"],
      clientPendingTransportIds: [],
      clientReadyTransportIds: ["TelemetryCollectorService|internal|http://telemetry-collector-b:3003"],
    };

    registry.instances.set("TelemetryCollectorService", [telemetryInstance]);
    registry.upsertRemoteRouteRecord(
      "TelemetryCollectorService",
      "telemetry-b-1",
      telemetryInstance.transports[0],
    );

    Cadenza.emit("meta.fetch.delegate_failed", {
      __signalName: "meta.fetch.delegate_failed",
      __error:
        "FetchError: request to http://telemetry-collector-b:3003/delegation failed, reason: getaddrinfo ENOTFOUND telemetry-collector-b",
      serviceName: "TelemetryCollectorService",
      serviceInstanceId: "telemetry-b-1",
      serviceTransportId: "telemetry-b-transport-1",
      serviceOrigin: "http://telemetry-collector-b:3003",
      routeKey: "TelemetryCollectorService|internal|http://telemetry-collector-b:3003",
      __routeKey: "TelemetryCollectorService|internal|http://telemetry-collector-b:3003",
      __transportId: "telemetry-b-transport-1",
      __transportOrigin: "http://telemetry-collector-b:3003",
      communicationTypes: ["delegation"],
    });

    await new Promise((resolve) => setTimeout(resolve, 50));

    expect(
      registry.instances.get("TelemetryCollectorService")?.[0]?.isNonResponsive,
    ).toBe(true);
    expect(
      registry.remoteRoutesByKey.has(
        "TelemetryCollectorService|internal|http://telemetry-collector-b:3003",
      ),
    ).toBe(false);
  });

  it("retries a reset fetch delegation failure without blacklisting a live route", async () => {
    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "TelemetryCollectorService";
    registry.serviceInstanceId = "telemetry-1";

    const predictorInstance = {
      uuid: "predictor-2",
      serviceName: "PredictorService",
      isFrontend: false,
      isDatabase: false,
      isActive: true,
      isNonResponsive: false,
      isBlocked: false,
      numberOfRunningGraphs: 0,
      acceptingWork: true,
      reportedAt: new Date().toISOString(),
      transports: [
        {
          uuid: "predictor-transport-2",
          serviceInstanceId: "predictor-2",
          role: "internal",
          origin: "http://predictor:3005",
          protocols: ["rest"],
          deleted: false,
        },
      ],
      clientCreatedTransportIds: [],
      clientPendingTransportIds: [],
      clientReadyTransportIds: [],
    };

    registry.instances.set("PredictorService", [predictorInstance]);
    registry.deputies.set("PredictorService", [
      {
        serviceName: "PredictorService",
        intentName: "iot-prediction-compute",
        localTaskName: "Normalize prediction compute input (Proxy)",
      },
    ]);
    registry.upsertRemoteRouteRecord(
      "PredictorService",
      "predictor-2",
      predictorInstance.transports[0],
    );

    const scheduleSpy = vi
      .spyOn(Cadenza, "schedule")
      .mockImplementation(() => undefined as any);

    Cadenza.emit("meta.fetch.delegate_failed", {
      __signalName: "meta.fetch.delegate_failed",
      __error:
        "FetchError: request to http://predictor:3005/delegation failed, reason: socket hang up",
      serviceName: "PredictorService",
      serviceInstanceId: "predictor-2",
      serviceTransportId: "predictor-transport-2",
      serviceOrigin: "http://predictor:3005",
      routeKey: "PredictorService|internal|http://predictor:3005",
      __routeKey: "PredictorService|internal|http://predictor:3005",
      __transportId: "predictor-transport-2",
      __transportOrigin: "http://predictor:3005",
      communicationTypes: ["delegation"],
    });

    await waitForCondition(
      () =>
        scheduleSpy.mock.calls.some(
          ([signal]) => signal === "meta.service_registry.dependee_registered",
        ),
      1_500,
    );

    expect(scheduleSpy).toHaveBeenCalledWith(
      "meta.service_registry.dependee_registered",
      expect.objectContaining({
        serviceName: "PredictorService",
        serviceInstanceId: "predictor-2",
        serviceTransportId: "predictor-transport-2",
        serviceOrigin: "http://predictor:3005",
        routeKey: "PredictorService|internal|http://predictor:3005",
        __reason: "fetch_delegate_failed_retry",
      }),
      expect.any(Number),
    );
    expect(registry.instances.get("PredictorService")?.[0]?.isNonResponsive).toBe(
      false,
    );
    expect(
      registry.remoteRoutesByKey.has(
        "PredictorService|internal|http://predictor:3005",
      ),
    ).toBe(true);
  });

  it("demotes a dead route when delegation failure context only carries internal route keys", async () => {
    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "IotDbService";
    registry.serviceInstanceId = "iot-db-1";

    const telemetryInstance = {
      uuid: "telemetry-b-1",
      serviceName: "TelemetryCollectorService",
      isFrontend: false,
      isDatabase: false,
      isActive: true,
      isNonResponsive: false,
      isBlocked: false,
      numberOfRunningGraphs: 0,
      acceptingWork: true,
      reportedAt: new Date().toISOString(),
      transports: [
        {
          uuid: "telemetry-b-transport-1",
          serviceInstanceId: "telemetry-b-1",
          role: "internal",
          origin: "http://telemetry-collector-b:3003",
          protocols: ["rest"],
          deleted: false,
        },
      ],
      clientCreatedTransportIds: ["TelemetryCollectorService|internal|http://telemetry-collector-b:3003"],
      clientPendingTransportIds: [],
      clientReadyTransportIds: ["TelemetryCollectorService|internal|http://telemetry-collector-b:3003"],
    };

    registry.instances.set("TelemetryCollectorService", [telemetryInstance]);
    registry.upsertRemoteRouteRecord(
      "TelemetryCollectorService",
      "telemetry-b-1",
      telemetryInstance.transports[0],
    );

    Cadenza.emit("meta.fetch.delegate_failed", {
      __signalName: "meta.fetch.delegate_failed",
      __error:
        "FetchError: request to http://telemetry-collector-b:3003/delegation failed, reason: getaddrinfo ENOTFOUND telemetry-collector-b",
      __serviceName: "TelemetryCollectorService",
      __instance: "telemetry-b-1",
      __transportId: "telemetry-b-transport-1",
      __transportOrigin: "http://telemetry-collector-b:3003",
      __routeKey: "TelemetryCollectorService|internal|http://telemetry-collector-b:3003",
      __fetchId:
        "TelemetryCollectorService|internal|http://telemetry-collector-b:3003|rest",
      communicationTypes: ["delegation"],
    });

    await new Promise((resolve) => setTimeout(resolve, 50));

    expect(
      registry.instances.get("TelemetryCollectorService")?.[0]?.isNonResponsive,
    ).toBe(true);
    expect(
      registry.instances.get("TelemetryCollectorService")?.[0]
        ?.clientReadyTransportIds,
    ).toEqual([]);
    expect(
      registry.remoteRoutesByKey.has(
        "TelemetryCollectorService|internal|http://telemetry-collector-b:3003",
      ),
    ).toBe(false);
  });

  it("retries manifest publication when authority has no eligible responder", async () => {
    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "ScheduledRunnerService";
    registry.serviceInstanceId = "scheduled-runner-1";
    registry.connectsToCadenzaDB = true;
    registry.seedAuthorityBootstrapRoute(
      "http://cadenza-db-service:8080",
      "internal",
    );
    registry.noteAuthorityBootstrapHandshake({
      serviceName: "CadenzaDB",
      serviceInstanceId: "cadenza-db-live-1",
      serviceTransportId: "cadenza-db-transport-1",
      serviceOrigin: "http://cadenza-db-service:8080",
    });

    (Cadenza as any).serviceManifestRevision = 0;
    (Cadenza as any).lastPublishedServiceManifestHashes = {};
    (Cadenza as any).serviceManifestPublishedAt = {};
    (Cadenza as any).localServiceManifestDefinitionInserted = true;
    (Cadenza as any).localServiceManifestInstanceInserted = true;
    (Cadenza as any).bootstrapSyncCompleted = true;
    (Cadenza as any).bootstrapSyncCompletedAt = Date.now() - 2_000;

    const retrySpy = vi
      .spyOn(Cadenza as any, "scheduleServiceManifestPublicationRetry")
      .mockImplementation(() => {});
    const inquireSpy = vi.spyOn(Cadenza, "inquire").mockRejectedValue({
      __error:
        "Inquiry 'meta-service-registry-authority-service-manifest-report' had no eligible responders",
      errored: true,
    });

    const result = await (Cadenza as any).publishServiceManifestIfNeeded(
      "service_setup_completed",
    );

    expect(result).toBe(false);
    expect(inquireSpy).toHaveBeenCalledWith(
      AUTHORITY_SERVICE_MANIFEST_REPORT_INTENT,
      expect.objectContaining({
        serviceName: "ScheduledRunnerService",
        serviceInstanceId: "scheduled-runner-1",
      }),
      expect.objectContaining({
        timeout: 15_000,
        requireComplete: true,
      }),
    );
    expect(retrySpy).toHaveBeenCalledWith(
      "service_setup_completed",
      "business_structural",
    );
    expect((Cadenza as any).lastPublishedServiceManifestHashes).toEqual({});
    expect((Cadenza as any).serviceManifestRevision).toBe(0);
  });

  it("defers manifest publication until the authority bootstrap handshake is established", async () => {
    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "ScheduledRunnerService";
    registry.serviceInstanceId = "scheduled-runner-1";
    registry.connectsToCadenzaDB = true;
    registry.seedAuthorityBootstrapRoute(
      "http://cadenza-db-service:8080",
      "internal",
    );

    (Cadenza as any).serviceManifestRevision = 0;
    (Cadenza as any).lastPublishedServiceManifestHashes = {};
    (Cadenza as any).serviceManifestPublishedAt = {};
    (Cadenza as any).localServiceManifestDefinitionInserted = true;
    (Cadenza as any).localServiceManifestInstanceInserted = true;
    (Cadenza as any).bootstrapSyncCompleted = true;
    (Cadenza as any).bootstrapSyncCompletedAt = Date.now() - 2_000;

    const retrySpy = vi
      .spyOn(Cadenza as any, "scheduleServiceManifestPublicationRetry")
      .mockImplementation(() => {});
    const inquireSpy = vi.spyOn(Cadenza, "inquire");

    const result = await (Cadenza as any).publishServiceManifestIfNeeded(
      "service_setup_completed",
    );

    expect(result).toBe(false);
    expect(inquireSpy).not.toHaveBeenCalled();
    expect(retrySpy).toHaveBeenCalledWith(
      "service_setup_completed",
      "business_structural",
    );
  });

  it("publishes the local CadenzaDB manifest without requiring a remote bootstrap handshake", async () => {
    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "CadenzaDB";
    registry.serviceInstanceId = "cadenza-db-local-1";
    registry.connectsToCadenzaDB = false;

    (Cadenza as any).serviceManifestRevision = 0;
    (Cadenza as any).lastPublishedServiceManifestHashes = {};
    (Cadenza as any).serviceManifestPublishedAt = {};
    (Cadenza as any).localServiceManifestDefinitionInserted = true;
    (Cadenza as any).localServiceManifestInstanceInserted = true;
    (Cadenza as any).bootstrapSyncCompleted = true;
    (Cadenza as any).bootstrapSyncCompletedAt = Date.now() - 2_000;

    const ensureBootstrapSpy = vi.spyOn(
      registry,
      "ensureBootstrapAuthorityControlPlaneForInquiry",
    );
    const retrySpy = vi
      .spyOn(Cadenza as any, "scheduleServiceManifestPublicationRetry")
      .mockImplementation(() => {});
    const inquireSpy = vi.spyOn(Cadenza, "inquire").mockResolvedValue({
      applied: true,
    } as any);

    const result = await (Cadenza as any).publishServiceManifestIfNeeded(
      "service_setup_completed",
    );

    expect(result).toMatchObject({
      published: true,
      publicationLayer: "routing_capability",
      serviceManifest: expect.objectContaining({
        serviceName: "CadenzaDB",
        serviceInstanceId: "cadenza-db-local-1",
      }),
    });
    expect(ensureBootstrapSpy).not.toHaveBeenCalled();
    expect(retrySpy).not.toHaveBeenCalled();
    expect(inquireSpy).toHaveBeenCalledWith(
      AUTHORITY_SERVICE_MANIFEST_REPORT_INTENT,
      expect.objectContaining({
        serviceName: "CadenzaDB",
        serviceInstanceId: "cadenza-db-local-1",
      }),
      expect.objectContaining({
        timeout: 60_000,
        requireComplete: true,
      }),
    );
  });

  it("coalesces manifest publication retries into a single scheduled attempt", async () => {
    vi.useFakeTimers();
    try {
      const registry = ServiceRegistry.instance as any;
      registry.serviceName = "ScheduledRunnerService";
      registry.serviceInstanceId = "scheduled-runner-1";
      registry.connectsToCadenzaDB = true;

      (Cadenza as any).serviceManifestPublicationRetryCount = 0;
      (Cadenza as any).serviceManifestPublicationPendingReason = null;
      (Cadenza as any).serviceManifestPublicationPendingLayer = null;
      (Cadenza as any).serviceManifestPublicationRetryReason = null;
      (Cadenza as any).serviceManifestPublicationRetryLayer = null;
      (Cadenza as any).serviceManifestPublicationRetryTimer = null;

      const publishSpy = vi
        .spyOn(Cadenza as any, "publishServiceManifestIfNeeded")
        .mockResolvedValue(false);

      (Cadenza as any).scheduleServiceManifestPublicationRetry(
        "service_registry_bootstrap_retry",
        "business_structural",
      );
      (Cadenza as any).scheduleServiceManifestPublicationRetry(
        "cadenza_db_fetch_handshake",
        "routing_capability",
      );

      expect((Cadenza as any).serviceManifestPublicationRetryCount).toBe(1);
      expect((Cadenza as any).serviceManifestPublicationRetryReason).toBe(
        "cadenza_db_fetch_handshake",
      );
      expect((Cadenza as any).serviceManifestPublicationRetryLayer).toBe(
        "business_structural",
      );

      await vi.advanceTimersByTimeAsync(1_000);

      expect(publishSpy).toHaveBeenCalledTimes(1);
      expect(publishSpy).toHaveBeenCalledWith(
        "cadenza_db_fetch_handshake",
        "business_structural",
      );
      expect((Cadenza as any).serviceManifestPublicationRetryTimer).toBeNull();
      expect((Cadenza as any).serviceManifestPublicationRetryReason).toBeNull();
      expect((Cadenza as any).serviceManifestPublicationRetryLayer).toBeNull();
    } finally {
      vi.useRealTimers();
    }
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
