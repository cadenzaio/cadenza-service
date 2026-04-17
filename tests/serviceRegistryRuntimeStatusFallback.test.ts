import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import Cadenza from "../src/Cadenza";
import DatabaseController from "@service-database-controller";
import GraphMetadataController from "../src/graph/controllers/GraphMetadataController";
import GraphSyncController from "../src/graph/controllers/GraphSyncController";
import RestController from "../src/network/RestController";
import ServiceRegistry from "../src/registry/ServiceRegistry";
import SignalController from "../src/signals/SignalController";
import SocketController from "../src/network/SocketController";
import { buildTransportHandleKey as buildHandleKey } from "../src/utils/transport";
import { selectTransportForRole } from "../src/utils/transport";

function buildRouteKey(
  serviceName: string,
  role: "internal" | "public",
  origin: string,
) {
  return `${serviceName}|${role}|${origin}`;
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

describe("service registry runtime status fallback", () => {
  const originalFetch = globalThis.fetch;
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
    globalThis.fetch = originalFetch;
    resetRuntimeState();
    consoleLogSpy.mockRestore();
    consoleWarnSpy.mockRestore();
    consoleErrorSpy.mockRestore();
    vi.restoreAllMocks();
  });

  it("prefers registered transports over bootstrap transports for routing", () => {
    const selected = selectTransportForRole(
      [
        {
          uuid: "cadenza-db-internal-bootstrap",
          serviceInstanceId: "cadenza-db",
          role: "internal",
          origin: "http://bootstrap.example:5000",
          protocols: ["rest", "socket"],
          securityProfile: null,
          authStrategy: null,
          deleted: false,
          clientCreated: true,
        },
        {
          uuid: "cadenza-db-transport-1",
          serviceInstanceId: "cadenza-db",
          role: "internal",
          origin: "http://cadenza-db:5000",
          protocols: ["rest", "socket"],
          securityProfile: null,
          authStrategy: null,
          deleted: false,
          clientCreated: true,
        },
      ],
      "internal",
      "rest",
    );

    expect(selected?.uuid).toBe("cadenza-db-transport-1");
  });

  it("serves local status from seeded startup instance state before durable registration resolves", () => {
    const registry = ServiceRegistry.instance;
    registry.serviceName = "CadenzaDB";
    registry.serviceInstanceId = "cadenza-db-local";
    registry.seedLocalInstance(
      {
        uuid: "cadenza-db-local",
        service_name: "CadenzaDB",
        is_active: true,
        is_non_responsive: false,
        is_blocked: false,
        is_database: true,
        transports: [
          {
            uuid: "cadenza-db-local-internal",
            service_instance_id: "cadenza-db-local",
            role: "internal",
            origin: "http://cadenza-db-service:8080",
            protocols: ["rest", "socket"],
          },
        ],
      },
      { markTransportsReady: true },
    );
    registry.latestRuntimeMetricsSnapshot = {
      sampledAt: "2026-04-07T10:00:00.000Z",
      cpuUsage: 0.2,
      memoryUsage: 0.4,
      eventLoopLag: 8,
      rssBytes: 200,
      heapUsedBytes: 120,
      heapTotalBytes: 160,
      memoryLimitBytes: 500,
    };

    expect(registry.resolveLocalStatusCheck()).toMatchObject({
      __status: "ok",
      serviceName: "CadenzaDB",
      serviceInstanceId: "cadenza-db-local",
      cpuUsage: 0.2,
      memoryUsage: 0.4,
      eventLoopLag: 8,
      isActive: true,
      isNonResponsive: false,
      isBlocked: false,
      health: {
        runtimeMetrics: {
          sampledAt: "2026-04-07T10:00:00.000Z",
          rssBytes: 200,
          heapUsedBytes: 120,
          heapTotalBytes: 160,
          memoryLimitBytes: 500,
        },
      },
    });
  });

  it("normalizes typed and legacy runtime health metrics from reports", () => {
    const registry = ServiceRegistry.instance as any;

    expect(
      registry.normalizeRuntimeStatusReport({
        serviceName: "OrdersService",
        serviceInstanceId: "orders-1",
        numberOfRunningGraphs: 1,
        isActive: true,
        isNonResponsive: false,
        isBlocked: false,
        health: {
          cpuLoad: 0.25,
          memoryPressure: 0.5,
          eventLoopLagMs: 12,
        },
      }),
    ).toMatchObject({
      cpuUsage: 0.25,
      memoryUsage: 0.5,
      eventLoopLag: 12,
    });

    expect(
      registry.normalizeRuntimeStatusReport({
        serviceName: "OrdersService",
        serviceInstanceId: "orders-1",
        numberOfRunningGraphs: 1,
        cpuUsage: 0.15,
        memoryUsage: 0.35,
        eventLoopLag: 6,
        isActive: true,
        isNonResponsive: false,
        isBlocked: false,
      }),
    ).toMatchObject({
      cpuUsage: 0.15,
      memoryUsage: 0.35,
      eventLoopLag: 6,
    });

    expect(
      registry.normalizeRuntimeStatusReport({
        serviceName: "OrdersService",
        serviceInstanceId: "orders-1",
        numberOfRunningGraphs: 1,
        isActive: true,
        isNonResponsive: false,
        isBlocked: false,
        state: "healthy",
        acceptingWork: true,
        reportedAt: "2026-04-12T10:00:00.000Z",
        health: {
          cpuUsage: 0.44,
          runtimeMetrics: {
            rssBytes: 2_000,
            heapUsedBytes: 900,
            heapTotalBytes: 1_100,
            memoryLimitBytes: 4_000,
            sampledAt: "2026-04-12T09:59:55.000Z",
            oversizedNested: {
              should: "be removed",
            },
          },
          runtimeStatus: {
            state: "unhealthy-custom",
            extra: "ignored",
          },
          giantNestedBlob: {
            payload: ["drop me"],
          },
        },
      }),
    ).toMatchObject({
      cpuUsage: 0.44,
      health: {
        cpuUsage: 0.44,
        runtimeMetrics: {
          rssBytes: 2_000,
          heapUsedBytes: 900,
          heapTotalBytes: 1_100,
          memoryLimitBytes: 4_000,
          sampledAt: "2026-04-12T09:59:55.000Z",
        },
        runtimeStatus: {
          state: "healthy",
          acceptingWork: true,
          reportedAt: "2026-04-12T10:00:00.000Z",
        },
      },
    });
    expect(
      registry.normalizeRuntimeStatusReport({
        serviceName: "OrdersService",
        serviceInstanceId: "orders-1",
        numberOfRunningGraphs: 1,
        isActive: true,
        isNonResponsive: false,
        isBlocked: false,
        state: "healthy",
        acceptingWork: true,
        reportedAt: "2026-04-12T10:00:00.000Z",
        health: {
          giantNestedBlob: {
            payload: ["drop me"],
          },
        },
      })?.health,
    ).not.toHaveProperty("giantNestedBlob");
  });

  it("resolves runtime status fallback directly via the REST status endpoint", async () => {
    const fetchMock = vi.fn(async () => ({
      ok: true,
      json: async () => ({
        __status: "ok",
        serviceName: "CadenzaDB",
        serviceInstanceId: "cadenza-db",
        numberOfRunningGraphs: 0,
        health: {},
        isActive: true,
        isNonResponsive: false,
        isBlocked: false,
        state: "healthy",
        acceptingWork: true,
        reportedAt: "2026-03-13T21:15:30.000Z",
      }),
    }));

    globalThis.fetch = fetchMock as typeof fetch;

    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "OrdersService";
    registry.serviceInstanceId = "99999999-9999-4999-8999-999999999999";
    registry.useSocket = true;
    registry.instances.set("OrdersService", [
      {
        uuid: "99999999-9999-4999-8999-999999999999",
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
    registry.instances.set("CadenzaDB", [
      {
        uuid: "cadenza-db",
        serviceName: "CadenzaDB",
        numberOfRunningGraphs: 0,
        isPrimary: false,
        isActive: true,
        isNonResponsive: false,
        isBlocked: false,
        runtimeState: "healthy",
        acceptingWork: true,
        health: {},
        isFrontend: false,
        isDatabase: true,
        transports: [
          {
            uuid: "cadenza-db-internal-bootstrap",
            serviceInstanceId: "cadenza-db",
            role: "internal",
            origin: "http://bootstrap.example:5000",
            protocols: ["rest", "socket"],
            securityProfile: null,
            authStrategy: null,
            deleted: false,
            clientCreated: true,
          },
          {
            uuid: "cadenza-db-transport-1",
            serviceInstanceId: "cadenza-db",
            role: "internal",
            origin: "http://cadenza-db:5000",
            protocols: ["rest", "socket"],
            securityProfile: null,
            authStrategy: null,
            deleted: false,
            clientCreated: true,
          },
        ],
      },
    ]);

    const inquireSpy = vi.spyOn(Cadenza, "inquire");

    const result = await (
      registry as {
        resolveRuntimeStatusFallbackInquiry: (
          serviceName: string,
          serviceInstanceId: string,
        ) => Promise<{
          report: Record<string, unknown>;
          inquiryMeta: Record<string, unknown>;
        }>;
      }
    ).resolveRuntimeStatusFallbackInquiry("CadenzaDB", "cadenza-db");

    expect(fetchMock).toHaveBeenCalledWith(
      "http://cadenza-db:5000/status",
      expect.objectContaining({
        method: "GET",
      }),
    );
    expect(inquireSpy).not.toHaveBeenCalled();
    expect(result.report).toMatchObject({
      serviceName: "CadenzaDB",
      serviceInstanceId: "cadenza-db",
      state: "healthy",
    });
    expect(result.inquiryMeta).toEqual(
      expect.objectContaining({
        inquiry: "meta-runtime-status",
        directStatusCheck: true,
        responded: 1,
      }),
    );
  });

  it("replaces a stale same-origin instance when direct status reports a new instance id", async () => {
    const fetchMock = vi.fn(async () => ({
      ok: true,
      json: async () => ({
        __status: "ok",
        serviceName: "IotDbService",
        serviceInstanceId: "iot-db-new",
        serviceTransportId: "iot-db-new-internal",
        transportRole: "internal",
        serviceOrigin: "http://iot-db-service:3001",
        transportProtocols: ["rest"],
        numberOfRunningGraphs: 0,
        health: {},
        isActive: true,
        isNonResponsive: false,
        isBlocked: false,
        state: "healthy",
        acceptingWork: true,
        reportedAt: "2026-03-27T18:12:40.671Z",
        isFrontend: false,
      }),
    }));

    globalThis.fetch = fetchMock as typeof fetch;

    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "ScheduledRunnerService";
    registry.serviceInstanceId = "runner-1";
    registry.useSocket = true;
    registry.instances.set("ScheduledRunnerService", [
      {
        uuid: "runner-1",
        serviceName: "ScheduledRunnerService",
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
    registry.instances.set("IotDbService", [
      {
        uuid: "iot-db-old",
        serviceName: "IotDbService",
        numberOfRunningGraphs: 0,
        isPrimary: false,
        isActive: true,
        isNonResponsive: false,
        isBlocked: false,
        runtimeState: "healthy",
        acceptingWork: true,
        reportedAt: "2026-03-27T18:11:40.671Z",
        health: {},
        isFrontend: false,
        isDatabase: true,
        transports: [
          {
            uuid: "iot-db-old-internal",
            serviceInstanceId: "iot-db-old",
            role: "internal",
            origin: "http://iot-db-service:3001",
            protocols: ["rest"],
            securityProfile: null,
            authStrategy: null,
            deleted: false,
            clientCreated: true,
          },
        ],
        clientCreatedTransportIds: ["iot-db-old-internal"],
        clientPendingTransportIds: [],
        clientReadyTransportIds: ["iot-db-old-internal"],
      },
    ]);

    const result = await (
      registry as {
        resolveRuntimeStatusFallbackInquiry: (
          serviceName: string,
          serviceInstanceId: string,
        ) => Promise<{
          report: Record<string, unknown>;
          inquiryMeta: Record<string, unknown>;
        }>;
      }
    ).resolveRuntimeStatusFallbackInquiry("IotDbService", "iot-db-old");

    expect(result.report).toMatchObject({
      serviceName: "IotDbService",
      serviceInstanceId: "iot-db-new",
    });
    expect(result.inquiryMeta).toEqual(
      expect.objectContaining({
        inquiry: "meta-runtime-status",
        directStatusCheck: true,
        identityReplacement: true,
      }),
    );

    const trackedIotDbInstances = registry.instances.get("IotDbService") ?? [];
    expect(trackedIotDbInstances).toHaveLength(1);
    expect(trackedIotDbInstances[0]).toMatchObject({
      uuid: "iot-db-new",
      isActive: true,
      isNonResponsive: false,
      clientReadyTransportIds: [
        buildRouteKey("IotDbService", "internal", "http://iot-db-service:3001"),
      ],
    });
    expect(trackedIotDbInstances[0].transports).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          uuid: "iot-db-new-internal",
          serviceInstanceId: "iot-db-new",
          role: "internal",
          origin: "http://iot-db-service:3001",
        }),
      ]),
    );
  });

  it("requests runtime status fallback in REST mode after missed heartbeats", async () => {
    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "OrdersService";
    registry.serviceInstanceId = "orders-1";
    registry.useSocket = false;
    registry.instances.set("OrdersService", [
      {
        uuid: "orders-1",
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
        transports: [
          {
            uuid: "telemetry-transport-1",
            serviceInstanceId: "telemetry-1",
            role: "internal",
            origin: "http://telemetry-collector:3003",
            protocols: ["rest"],
            securityProfile: null,
            authStrategy: null,
            deleted: false,
            clientCreated: true,
          },
        ],
      },
    ]);
    registry.dependeesByService.set("TelemetryCollectorService", new Set(["telemetry-1"]));
    registry.dependeeByInstance.set("telemetry-1", "TelemetryCollectorService");
    registry.lastHeartbeatAtByInstance.set("telemetry-1", 0);
    registry.missedHeartbeatsByInstance.set("telemetry-1", 2);

    let requestedCtx: Record<string, unknown> | null = null;
    Cadenza.createEphemeralMetaTask(
      "Capture runtime status fallback request",
      (ctx) => {
        requestedCtx = { ...ctx };
        return ctx;
      },
      "",
      { register: false },
    ).doOn("meta.service_registry.runtime_status_fallback_requested");

    Cadenza.emit("meta.service_registry.runtime_status.monitor_tick", {});

    await new Promise((resolve) => setTimeout(resolve, 25));

    expect(requestedCtx).toMatchObject({
      serviceName: "TelemetryCollectorService",
      serviceInstanceId: "telemetry-1",
      serviceTransportId: "telemetry-transport-1",
      serviceOrigin: "http://telemetry-collector:3003",
    });
  });

  it("does not request runtime status fallback for CadenzaDB dependees", async () => {
    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "OrdersService";
    registry.serviceInstanceId = "orders-1";
    registry.useSocket = false;
    registry.instances.set("OrdersService", [
      {
        uuid: "orders-1",
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
    registry.instances.set("CadenzaDB", [
      {
        uuid: "cadenza-db",
        serviceName: "CadenzaDB",
        numberOfRunningGraphs: 0,
        isPrimary: false,
        isActive: true,
        isNonResponsive: false,
        isBlocked: false,
        runtimeState: "healthy",
        acceptingWork: true,
        health: {},
        isFrontend: false,
        isDatabase: true,
        transports: [
          {
            uuid: "cadenza-db-transport-1",
            serviceInstanceId: "cadenza-db",
            role: "internal",
            origin: "http://cadenza-db:5000",
            protocols: ["rest"],
            securityProfile: null,
            authStrategy: null,
            deleted: false,
            clientCreated: true,
          },
        ],
      },
    ]);
    registry.dependeesByService.set("CadenzaDB", new Set(["cadenza-db"]));
    registry.dependeeByInstance.set("cadenza-db", "CadenzaDB");
    registry.lastHeartbeatAtByInstance.set("cadenza-db", 0);
    registry.missedHeartbeatsByInstance.set("cadenza-db", 2);

    let requestedCtx: Record<string, unknown> | null = null;
    Cadenza.createEphemeralMetaTask(
      "Capture skipped CadenzaDB runtime status fallback request",
      (ctx) => {
        requestedCtx = { ...ctx };
        return ctx;
      },
      "",
      { register: false },
    ).doOn("meta.service_registry.runtime_status_fallback_requested");

    Cadenza.emit("meta.service_registry.runtime_status.monitor_tick", {});

    await new Promise((resolve) => setTimeout(resolve, 25));

    expect(requestedCtx).toBeNull();
  });

  it("refreshes heartbeat freshness when a runtime status report is applied", () => {
    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "OrdersService";
    registry.serviceInstanceId = "orders-1";
    registry.instances.set("OrdersService", [
      {
        uuid: "orders-1",
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
    registry.instances.set("AnomalyDetectorService", [
      {
        uuid: "anomaly-1",
        serviceName: "AnomalyDetectorService",
        numberOfRunningGraphs: 0,
        isPrimary: false,
        isActive: true,
        isNonResponsive: true,
        isBlocked: false,
        runtimeState: "degraded",
        acceptingWork: true,
        health: {},
        isFrontend: false,
        isDatabase: false,
        transports: [],
      },
    ]);
    registry.lastHeartbeatAtByInstance.set("anomaly-1", 0);
    registry.missedHeartbeatsByInstance.set("anomaly-1", 3);
    registry.runtimeStatusFallbackInFlightByInstance.add("anomaly-1");

    const beforeApply = Date.now();
    const applied = registry.applyRuntimeStatusReport({
      serviceName: "AnomalyDetectorService",
      serviceInstanceId: "anomaly-1",
      reportedAt: "2026-03-26T20:00:00.000Z",
      state: "healthy",
      acceptingWork: true,
      numberOfRunningGraphs: 2,
      isActive: true,
      isNonResponsive: false,
      isBlocked: false,
      health: {
        ok: true,
      },
    });

    expect(applied).toBe(true);
    expect(registry.lastHeartbeatAtByInstance.get("anomaly-1")).toBeGreaterThanOrEqual(
      beforeApply,
    );
    expect(registry.missedHeartbeatsByInstance.get("anomaly-1")).toBe(0);
    expect(
      registry.runtimeStatusFallbackInFlightByInstance.has("anomaly-1"),
    ).toBe(false);
  });

  it("does not request runtime status fallback immediately after a fresh report", async () => {
    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "OrdersService";
    registry.serviceInstanceId = "orders-1";
    registry.useSocket = false;
    registry.instances.set("OrdersService", [
      {
        uuid: "orders-1",
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
    registry.instances.set("AnomalyDetectorService", [
      {
        uuid: "anomaly-1",
        serviceName: "AnomalyDetectorService",
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
            uuid: "anomaly-transport-1",
            serviceInstanceId: "anomaly-1",
            role: "internal",
            origin: "http://anomaly-detector:3004",
            protocols: ["rest"],
            securityProfile: null,
            authStrategy: null,
            deleted: false,
            clientCreated: true,
          },
        ],
      },
    ]);
    registry.dependeesByService.set("AnomalyDetectorService", new Set(["anomaly-1"]));
    registry.dependeeByInstance.set("anomaly-1", "AnomalyDetectorService");
    registry.lastHeartbeatAtByInstance.set("anomaly-1", 0);
    registry.missedHeartbeatsByInstance.set("anomaly-1", 2);

    let requestedCtx: Record<string, unknown> | null = null;
    Cadenza.createEphemeralMetaTask(
      "Capture fresh runtime status fallback request",
      (ctx) => {
        requestedCtx = { ...ctx };
        return ctx;
      },
      "",
      { register: false },
    ).doOn("meta.service_registry.runtime_status_fallback_requested");

    registry.applyRuntimeStatusReport({
      serviceName: "AnomalyDetectorService",
      serviceInstanceId: "anomaly-1",
      transportId: "anomaly-transport-1",
      transportRole: "internal",
      transportOrigin: "http://anomaly-detector:3004",
      transportProtocols: ["rest"],
      reportedAt: "2026-03-26T20:00:00.000Z",
      state: "healthy",
      acceptingWork: true,
      numberOfRunningGraphs: 1,
      isActive: true,
      isNonResponsive: false,
      isBlocked: false,
      health: {},
    });

    Cadenza.emit("meta.service_registry.runtime_status.monitor_tick", {});

    await new Promise((resolve) => setTimeout(resolve, 25));

    expect(requestedCtx).toBeNull();
  });

  it("skips runtime status fallback when a dependee already has a ready socket transport", async () => {
    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "OrdersService";
    registry.serviceInstanceId = "orders-1";
    registry.instances.set("AnomalyDetectorService", [
      {
        uuid: "anomaly-1",
        serviceName: "AnomalyDetectorService",
        numberOfRunningGraphs: 0,
        isPrimary: false,
        isActive: true,
        isNonResponsive: false,
        isBlocked: false,
        runtimeState: "healthy",
        acceptingWork: true,
        reportedAt: "2026-03-26T20:00:00.000Z",
        health: {},
        isFrontend: false,
        isDatabase: false,
        clientReadyTransportIds: ["anomaly-socket-1"],
        transports: [
          {
            uuid: "anomaly-socket-1",
            serviceInstanceId: "anomaly-1",
            role: "internal",
            origin: "ws://anomaly-detector:3004",
            protocols: ["socket"],
            securityProfile: null,
            authStrategy: null,
            deleted: false,
            clientCreated: true,
          },
        ],
      },
    ]);
    registry.dependeesByService.set("AnomalyDetectorService", new Set(["anomaly-1"]));
    registry.dependeeByInstance.set("anomaly-1", "AnomalyDetectorService");
    registry.lastHeartbeatAtByInstance.set("anomaly-1", 0);
    registry.missedHeartbeatsByInstance.set("anomaly-1", 2);
    registry.runtimeStatusFallbackInFlightByInstance.add("anomaly-1");

    let requestedCtx: Record<string, unknown> | null = null;
    Cadenza.createEphemeralMetaTask(
      "Capture socket-ready fallback request",
      (ctx) => {
        requestedCtx = { ...ctx };
        return ctx;
      },
      "",
      { register: false },
    ).doOn("meta.service_registry.runtime_status_fallback_requested");

    Cadenza.emit("meta.service_registry.runtime_status.monitor_tick", {});

    await new Promise((resolve) => setTimeout(resolve, 25));

    expect(requestedCtx).toBeNull();
    expect(registry.missedHeartbeatsByInstance.get("anomaly-1")).toBe(0);
    expect(registry.lastHeartbeatAtByInstance.get("anomaly-1")).toBeGreaterThan(0);
    expect(registry.runtimeStatusFallbackInFlightByInstance.has("anomaly-1")).toBe(
      false,
    );
  });

  it("treats fresher authoritative service_instance updates as heartbeat freshness", async () => {
    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "OrdersService";
    registry.serviceInstanceId = "orders-local";
    registry.useSocket = false;
    registry.instances.set("OrdersService", [
      {
        uuid: "orders-local",
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
        reportedAt: "2026-03-26T09:00:00.000Z",
        health: {},
        isFrontend: false,
        isDatabase: false,
        transports: [
          {
            uuid: "telemetry-transport-1",
            serviceInstanceId: "telemetry-1",
            role: "internal",
            origin: "http://telemetry-collector:3003",
            protocols: ["rest"],
            securityProfile: null,
            authStrategy: null,
            deleted: false,
            clientCreated: true,
          },
        ],
      },
    ]);
    registry.dependeesByService.set("TelemetryCollectorService", new Set(["telemetry-1"]));
    registry.dependeeByInstance.set("telemetry-1", "TelemetryCollectorService");
    registry.lastHeartbeatAtByInstance.set("telemetry-1", 0);
    registry.missedHeartbeatsByInstance.set("telemetry-1", 3);
    registry.runtimeStatusFallbackInFlightByInstance.add("telemetry-1");

    let requestedCtx: Record<string, unknown> | null = null;
    Cadenza.createEphemeralMetaTask(
      "Capture fallback request after fresh authority instance update",
      (ctx) => {
        requestedCtx = { ...ctx };
        return ctx;
      },
      "",
      { register: false },
    ).doOn("meta.service_registry.runtime_status_fallback_requested");

    Cadenza.emit("global.meta.service_instance.updated", {
      data: {
        uuid: "telemetry-1",
        service_name: "TelemetryCollectorService",
        is_active: true,
        is_non_responsive: false,
        is_blocked: false,
        number_of_running_graphs: 8,
        reported_at: "2026-03-26T10:05:00.000Z",
        health: {
          runtimeStatus: {
            state: "healthy",
            acceptingWork: true,
            reportedAt: "2026-03-26T10:05:00.000Z",
          },
        },
        is_frontend: false,
        is_database: false,
        deleted: false,
      },
    });

    await new Promise((resolve) => setTimeout(resolve, 25));

    expect(registry.missedHeartbeatsByInstance.get("telemetry-1")).toBe(0);
    expect(registry.lastHeartbeatAtByInstance.get("telemetry-1")).toBeGreaterThan(0);
    expect(
      registry.runtimeStatusFallbackInFlightByInstance.has("telemetry-1"),
    ).toBe(false);

    Cadenza.emit("meta.service_registry.runtime_status.monitor_tick", {});
    await new Promise((resolve) => setTimeout(resolve, 25));

    expect(requestedCtx).toBeNull();
  });

  it("emits a service_instance update when runtime fallback marks an instance non-responsive", async () => {
    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "OrdersService";
    registry.serviceInstanceId = "orders-1";
    registry.instances.set("OrdersService", [
      {
        uuid: "orders-1",
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
    registry.instances.set("AnomalyDetectorService", [
      {
        uuid: "anomaly-1",
        serviceName: "AnomalyDetectorService",
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

    let updatedCtx: Record<string, unknown> | null = null;
    Cadenza.createEphemeralMetaTask(
      "Capture service instance update from runtime fallback",
      (ctx) => {
        updatedCtx = { ...ctx };
        return ctx;
      },
      "",
      { register: false },
    ).doOn("global.meta.service_instance.updated");

    Cadenza.emit("meta.service_registry.runtime_status_unreachable", {
      serviceName: "AnomalyDetectorService",
      serviceInstanceId: "anomaly-1",
    });

    await new Promise((resolve) => setTimeout(resolve, 25));

    expect(updatedCtx).toMatchObject({
      data: {
        is_active: false,
        is_non_responsive: true,
        deleted: false,
      },
      filter: {
        uuid: "anomaly-1",
      },
    });
  });

  it("marks an instance inactive after repeated runtime status misses", async () => {
    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "OrdersService";
    registry.serviceInstanceId = "orders-1";
    registry.instances.set("OrdersService", [
      {
        uuid: "orders-1",
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
    registry.instances.set("AnomalyDetectorService", [
      {
        uuid: "anomaly-1",
        serviceName: "AnomalyDetectorService",
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
    registry.missedHeartbeatsByInstance.set("anomaly-1", 6);

    let updatedCtx: Record<string, unknown> | null = null;
    let durableUpdateCtx: Record<string, unknown> | null = null;
    let durableTransportUpdateCtx: Record<string, unknown> | null = null;
    Cadenza.createEphemeralMetaTask(
      "Capture inactive service instance update from runtime fallback",
      (ctx) => {
        updatedCtx = { ...ctx };
        return ctx;
      },
      "",
      { register: false },
    ).doOn("global.meta.service_instance.updated");
    Cadenza.createEphemeralMetaTask(
      "Capture unexpected durable inactive instance persistence",
      (ctx) => {
        durableUpdateCtx = { ...ctx };
        return ctx;
      },
      "",
      { register: false },
    ).doOn("meta.service_registry.instance_update_requested");
    Cadenza.createEphemeralMetaTask(
      "Capture unexpected durable inactive transport persistence",
      (ctx) => {
        durableTransportUpdateCtx = { ...ctx };
        return ctx;
      },
      "",
      { register: false },
    ).doOn("meta.service_registry.transport_update_requested");

    Cadenza.emit("meta.service_registry.runtime_status_unreachable", {
      serviceName: "AnomalyDetectorService",
      serviceInstanceId: "anomaly-1",
    });

    await new Promise((resolve) => setTimeout(resolve, 25));

    expect(updatedCtx).toMatchObject({
      data: {
        is_active: false,
        is_non_responsive: false,
        deleted: false,
      },
      filter: {
        uuid: "anomaly-1",
      },
    });
    expect(durableUpdateCtx).toBeNull();
    expect(durableTransportUpdateCtx).toBeNull();
  });

  it("does not demote CadenzaDB through generic runtime status unreachable handling", async () => {
    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "OrdersService";
    registry.serviceInstanceId = "orders-1";
    registry.instances.set("OrdersService", [
      {
        uuid: "orders-1",
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
    registry.instances.set("CadenzaDB", [
      {
        uuid: "cadenza-db",
        serviceName: "CadenzaDB",
        numberOfRunningGraphs: 0,
        isPrimary: false,
        isActive: true,
        isNonResponsive: false,
        isBlocked: false,
        runtimeState: "healthy",
        acceptingWork: true,
        health: {},
        isFrontend: false,
        isDatabase: true,
        transports: [],
      },
    ]);

    let updatedCtx: Record<string, unknown> | null = null;
    Cadenza.createEphemeralMetaTask(
      "Capture skipped CadenzaDB runtime status demotion",
      (ctx) => {
        updatedCtx = { ...ctx };
        return ctx;
      },
      "",
      { register: false },
    ).doOn("global.meta.service_instance.updated");

    Cadenza.emit("meta.service_registry.runtime_status_unreachable", {
      serviceName: "CadenzaDB",
      serviceInstanceId: "cadenza-db",
    });

    await new Promise((resolve) => setTimeout(resolve, 25));

    expect(updatedCtx).toBeNull();
    expect(registry.instances.get("CadenzaDB")).toEqual([
      expect.objectContaining({
        uuid: "cadenza-db",
        isActive: true,
        isNonResponsive: false,
      }),
    ]);
  });

  it("emits a service_instance update when handshake marks an instance active again", async () => {
    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "OrdersService";
    registry.serviceInstanceId = "orders-1";
    registry.instances.set("OrdersService", [
      {
        uuid: "orders-1",
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
    registry.instances.set("CadenzaDB", [
      {
        uuid: "cadenza-db",
        serviceName: "CadenzaDB",
        numberOfRunningGraphs: 0,
        isPrimary: false,
        isActive: false,
        isNonResponsive: true,
        isBlocked: false,
        runtimeState: "unavailable",
        acceptingWork: false,
        health: {},
        isFrontend: false,
        isDatabase: true,
        transports: [],
      },
    ]);

    let updatedCtx: Record<string, unknown> | null = null;
    Cadenza.createEphemeralMetaTask(
      "Capture service instance update from handshake",
      (ctx) => {
        updatedCtx = { ...ctx };
        return ctx;
      },
      "",
      { register: false },
    ).doOn("global.meta.service_instance.updated");

    Cadenza.emit("meta.fetch.handshake_complete", {
      serviceName: "CadenzaDB",
      serviceInstanceId: "cadenza-db",
    });

    await new Promise((resolve) => setTimeout(resolve, 25));

    expect(updatedCtx).toMatchObject({
      data: {
        is_active: true,
        is_non_responsive: false,
      },
      filter: {
        uuid: "cadenza-db",
      },
    });
  });

  it("refreshes the local instance when inbound activity is observed after a non-responsive state", async () => {
    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "OrdersService";
    registry.serviceInstanceId = "orders-1";
    registry.runtimeStatusHeartbeatIntervalMs = 10_000;
    registry.instances.set("OrdersService", [
      {
        uuid: "orders-1",
        serviceName: "OrdersService",
        numberOfRunningGraphs: 0,
        isPrimary: false,
        isActive: false,
        isNonResponsive: true,
        isBlocked: false,
        runtimeState: "unavailable",
        acceptingWork: false,
        reportedAt: "2026-03-25T21:00:00.000Z",
        health: {},
        isFrontend: false,
        isDatabase: false,
        transports: [],
      },
    ]);

    let updatedCtx: Record<string, unknown> | null = null;
    Cadenza.createEphemeralMetaTask(
      "Capture local activity instance update",
      (ctx) => {
        updatedCtx = { ...ctx };
        return ctx;
      },
      "",
      { register: false },
    ).doOn("global.meta.service_instance.updated");

    Cadenza.emit("meta.service_registry.instance_activity_observed", {
      serviceName: "OrdersService",
      serviceInstanceId: "orders-1",
      activityAt: "2026-03-26T10:00:00.000Z",
      source: "rest-delegation",
    });

    await new Promise((resolve) => setTimeout(resolve, 25));

    expect(updatedCtx).toBeNull();
    expect(registry.instances.get("OrdersService")).toEqual([
      expect.objectContaining({
        uuid: "orders-1",
        isActive: true,
        isNonResponsive: false,
      }),
    ]);
    expect(
      Date.parse(registry.instances.get("OrdersService")?.[0]?.reportedAt ?? ""),
    ).toBeGreaterThan(0);
  });

  it("refreshes a tracked remote instance when successful remote activity is observed", async () => {
    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "SchedulerService";
    registry.serviceInstanceId = "scheduler-1";
    registry.instances.set("OrdersService", [
      {
        uuid: "orders-1",
        serviceName: "OrdersService",
        numberOfRunningGraphs: 0,
        isPrimary: false,
        isActive: false,
        isNonResponsive: true,
        isBlocked: false,
        runtimeState: "unavailable",
        acceptingWork: false,
        reportedAt: "2026-03-26T09:00:00.000Z",
        health: {},
        isFrontend: false,
        isDatabase: false,
        transports: [
          {
            uuid: "transport-1",
            serviceInstanceId: "orders-1",
            role: "internal",
            origin: "http://orders:3001",
            protocols: ["rest"],
            securityProfile: null,
            authStrategy: null,
            communicationTypes: [],
          },
        ],
      },
    ]);
    registry.lastHeartbeatAtByInstance.set("orders-1", 0);
    registry.missedHeartbeatsByInstance.set("orders-1", 4);
    registry.runtimeStatusFallbackInFlightByInstance.add("orders-1");

    Cadenza.emit("meta.service_registry.remote_activity_observed", {
      serviceName: "OrdersService",
      serviceInstanceId: "orders-1",
      serviceTransportId: "transport-1",
      activityAt: "2026-03-26T10:05:00.000Z",
      source: "rest-delegation-success",
    });

    await new Promise((resolve) => setTimeout(resolve, 25));

    expect(registry.instances.get("OrdersService")).toEqual([
      expect.objectContaining({
        uuid: "orders-1",
        isActive: true,
        isNonResponsive: false,
        reportedAt: "2026-03-26T10:05:00.000Z",
        clientReadyTransportIds: expect.arrayContaining([
          buildRouteKey("OrdersService", "internal", "http://orders:3001"),
        ]),
      }),
    ]);
    expect(registry.missedHeartbeatsByInstance.get("orders-1")).toBe(0);
    expect(registry.lastHeartbeatAtByInstance.get("orders-1")).toBeGreaterThan(0);
    expect(registry.runtimeStatusFallbackInFlightByInstance.has("orders-1")).toBe(
      false,
    );
  });

  it("ignores stale disconnects for an obsolete same-origin transport generation", async () => {
    const registry = ServiceRegistry.instance as any;

    registry.instances.set("IotDbService", [
      {
        uuid: "iot-db-new",
        serviceName: "IotDbService",
        numberOfRunningGraphs: 0,
        isPrimary: false,
        isActive: true,
        isNonResponsive: false,
        isBlocked: false,
        runtimeState: "healthy",
        acceptingWork: true,
        reportedAt: "2026-03-29T10:05:00.000Z",
        health: {},
        isFrontend: false,
        isDatabase: true,
        transports: [
          {
            uuid: "iot-db-new-internal",
            serviceInstanceId: "iot-db-new",
            role: "internal",
            origin: "http://iot-db-service:3001",
            protocols: ["rest", "socket"],
            securityProfile: null,
            authStrategy: null,
          },
        ],
        clientCreatedTransportIds: [],
        clientPendingTransportIds: [],
        clientReadyTransportIds: [],
      },
    ]);

    registry.markTransportClientReady(registry.instances.get("IotDbService")[0], {
      uuid: "iot-db-new-internal",
      serviceInstanceId: "iot-db-new",
      role: "internal",
      origin: "http://iot-db-service:3001",
    });

    const routeKey = buildRouteKey(
      "IotDbService",
      "internal",
      "http://iot-db-service:3001",
    );

    Cadenza.emit("meta.socket_client.disconnected", {
      serviceName: "IotDbService",
      serviceInstanceId: "iot-db-old",
      serviceTransportId: "iot-db-old-internal",
      serviceOrigin: "http://iot-db-service:3001",
      fetchId: buildHandleKey(routeKey, "socket"),
      routeKey,
    });

    await new Promise((resolve) => setTimeout(resolve, 25));

    expect(registry.instances.get("IotDbService")).toEqual([
      expect.objectContaining({
        uuid: "iot-db-new",
        isActive: true,
        isNonResponsive: false,
        clientReadyTransportIds: expect.arrayContaining([routeKey]),
      }),
    ]);
  });

  it("does not mark a route unavailable when socket disconnects but rest remains ready", async () => {
    const registry = ServiceRegistry.instance as any;
    const routeKey = buildRouteKey(
      "CadenzaDB",
      "internal",
      "http://cadenza-db-service:8080",
    );

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
            uuid: "cadenza-db-transport-1",
            serviceInstanceId: "cadenza-db-1",
            role: "internal",
            origin: "http://cadenza-db-service:8080",
            protocols: ["rest", "socket"],
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
      serviceName: "CadenzaDB",
      role: "internal",
      origin: "http://cadenza-db-service:8080",
      protocols: ["rest", "socket"],
      serviceInstanceId: "cadenza-db-1",
      serviceTransportId: "cadenza-db-transport-1",
      generation: "cadenza-db-1|cadenza-db-transport-1",
      protocolState: {
        rest: {
          clientCreated: true,
          clientPending: false,
          clientReady: true,
        },
        socket: {
          clientCreated: true,
          clientPending: false,
          clientReady: true,
        },
      },
      lastUpdatedAt: Date.now(),
    });

    Cadenza.emit("meta.socket_client.disconnected", {
      serviceName: "CadenzaDB",
      serviceInstanceId: "cadenza-db-1",
      serviceTransportId: "cadenza-db-transport-1",
      serviceOrigin: "http://cadenza-db-service:8080",
      fetchId: buildHandleKey(routeKey, "socket"),
      routeKey,
      transportProtocol: "socket",
    });

    await new Promise((resolve) => setTimeout(resolve, 25));

    expect(registry.instances.get("CadenzaDB")).toEqual([
      expect.objectContaining({
        uuid: "cadenza-db-1",
        isActive: true,
        isNonResponsive: false,
        clientReadyTransportIds: expect.arrayContaining([routeKey]),
      }),
    ]);
  });

  it("ignores stale transport-handle events when the route has already moved to a newer transport", () => {
    const registry = ServiceRegistry.instance as any;
    const routeKey = buildRouteKey(
      "CadenzaDB",
      "internal",
      "http://cadenza-db-service:8080",
    );

    registry.remoteRoutesByKey.set(routeKey, {
      key: routeKey,
      serviceName: "CadenzaDB",
      role: "internal",
      origin: "http://cadenza-db-service:8080",
      protocols: ["rest", "socket"],
      serviceInstanceId: "cadenza-db-new",
      serviceTransportId: "cadenza-db-transport-new",
      generation: "cadenza-db-new|cadenza-db-transport-new",
      protocolState: {
        rest: {
          clientCreated: true,
          clientPending: false,
          clientReady: true,
        },
        socket: {
          clientCreated: true,
          clientPending: false,
          clientReady: true,
        },
      },
      lastUpdatedAt: Date.now(),
    });

    expect(
      registry.shouldProcessRemoteRouteEvent({
        routeKey,
        fetchId: buildHandleKey(routeKey, "rest"),
        serviceTransportId: "cadenza-db-transport-old",
      }),
    ).toBe(false);

    expect(
      registry.shouldProcessRemoteRouteEvent({
        routeKey,
        fetchId: buildHandleKey(routeKey, "rest"),
        serviceTransportId: "cadenza-db-transport-new",
      }),
    ).toBe(true);
  });

  it("does not mark the local authority instance inactive from self-route disconnect handling", async () => {
    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "CadenzaDB";
    registry.serviceInstanceId = "cadenza-db-self";

    const persistedUpdates: Array<Record<string, unknown>> = [];
    Cadenza.createEphemeralMetaTask(
      "Capture local authority inactive persistence",
      (ctx) => {
        persistedUpdates.push(ctx);
        return true;
      },
    ).doOn("global.meta.service_instance.updated");

    registry.instances.set("CadenzaDB", [
      {
        uuid: "cadenza-db-self",
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
            uuid: "cadenza-db-self-transport",
            serviceInstanceId: "cadenza-db-self",
            role: "internal",
            origin: "http://cadenza-db-service:8080",
            protocols: ["rest", "socket"],
            securityProfile: null,
            authStrategy: null,
            deleted: false,
          },
        ],
        clientCreatedTransportIds: [buildRouteKey("CadenzaDB", "internal", "http://cadenza-db-service:8080")],
        clientPendingTransportIds: [],
        clientReadyTransportIds: [buildRouteKey("CadenzaDB", "internal", "http://cadenza-db-service:8080")],
      },
    ]);

    Cadenza.emit("meta.socket_client.disconnected", {
      serviceName: "CadenzaDB",
      serviceInstanceId: "cadenza-db-self",
      serviceTransportId: "cadenza-db-self-transport",
      serviceOrigin: "http://cadenza-db-service:8080",
      fetchId: buildHandleKey(
        buildRouteKey("CadenzaDB", "internal", "http://cadenza-db-service:8080"),
        "socket",
      ),
      routeKey: buildRouteKey(
        "CadenzaDB",
        "internal",
        "http://cadenza-db-service:8080",
      ),
      transportProtocol: "socket",
    });

    await new Promise((resolve) => setTimeout(resolve, 25));

    expect(registry.instances.get("CadenzaDB")).toEqual([
      expect.objectContaining({
        uuid: "cadenza-db-self",
        isActive: true,
        isNonResponsive: false,
      }),
    ]);
    expect(
      persistedUpdates.some(
        (ctx) =>
          ctx.filter?.uuid === "cadenza-db-self" &&
          ctx.data?.is_active === false,
      ),
    ).toBe(false);
  });

  it("marks the local instance inactive and disables transports on graceful shutdown", async () => {
    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "OrdersService";
    registry.serviceInstanceId = "orders-1";
    registry.connectsToCadenzaDB = true;
    registry.instances.set("OrdersService", [
      {
        uuid: "orders-1",
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
            uuid: "11111111-1111-4111-8111-111111111111",
            serviceInstanceId: "99999999-9999-4999-8999-999999999999",
            role: "internal",
            origin: "http://orders:5000",
            protocols: ["rest", "socket"],
            securityProfile: null,
            authStrategy: null,
            deleted: false,
          },
        ],
      },
    ]);

    let instanceUpdate: Record<string, unknown> | null = null;
    let transportUpdate: Record<string, unknown> | null = null;

    Cadenza.createEphemeralMetaTask(
      "Capture local graceful shutdown instance update",
      (ctx) => {
        instanceUpdate = { ...ctx };
        return ctx;
      },
      "",
      { register: false },
    ).doOn("meta.service_registry.instance_update_requested");

    Cadenza.createEphemeralMetaTask(
      "Capture local graceful shutdown transport update",
      (ctx) => {
        transportUpdate = { ...ctx };
        return ctx;
      },
      "",
      { register: false },
    ).doOn("meta.service_registry.transport_update_requested");

    Cadenza.emit("meta.service_registry.instance_shutdown_reported", {
      serviceInstanceId: "orders-1",
      reason: "SIGTERM",
      graceful: true,
    });

    await new Promise((resolve) => setTimeout(resolve, 50));

    expect(instanceUpdate).toMatchObject({
      data: {
        is_active: false,
        is_non_responsive: false,
        deleted: false,
      },
      queryData: {
        filter: {
          uuid: "orders-1",
        },
      },
    });
    expect(transportUpdate).toBeNull();

    await new Promise((resolve) => setTimeout(resolve, 800));

    expect(transportUpdate).toMatchObject({
      data: {
        deleted: true,
      },
      queryData: {
        filter: {
          uuid: "11111111-1111-4111-8111-111111111111",
        },
      },
    });
  });

  it("ignores stale graceful shutdown signals for an older local instance id", async () => {
    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "CadenzaDB";
    registry.serviceInstanceId = "cadenza-db-new";
    registry.instances.set("CadenzaDB", [
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
        health: {},
        isFrontend: false,
        isDatabase: true,
        transports: [
          {
            uuid: "cadenza-db-new-transport",
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

    let instanceUpdate: Record<string, unknown> | null = null;
    let transportUpdate: Record<string, unknown> | null = null;

    Cadenza.createEphemeralMetaTask(
      "Capture stale graceful shutdown instance update",
      (ctx) => {
        instanceUpdate = { ...ctx };
        return ctx;
      },
      "",
      { register: false },
    ).doOn("meta.service_registry.instance_update_requested");

    Cadenza.createEphemeralMetaTask(
      "Capture stale graceful shutdown transport update",
      (ctx) => {
        transportUpdate = { ...ctx };
        return ctx;
      },
      "",
      { register: false },
    ).doOn("meta.service_registry.transport_update_requested");

    Cadenza.emit("meta.service_registry.instance_shutdown_reported", {
      serviceName: "CadenzaDB",
      serviceInstanceId: "cadenza-db-old",
      reason: "SIGTERM",
      graceful: true,
    });

    await new Promise((resolve) => setTimeout(resolve, 100));

    expect(instanceUpdate).toBeNull();
    expect(transportUpdate).toBeNull();
    expect(registry.getLocalInstance()).toMatchObject({
      uuid: "cadenza-db-new",
      isActive: true,
    });
    expect(
      registry.getLocalInstance().transports.find(
        (transport: any) => transport.uuid === "cadenza-db-new-transport",
      ),
    ).toMatchObject({
      deleted: false,
    });
  });

  it("attaches diagnostics when direct status and inquiry fallback miss the target", async () => {
    const fetchMock = vi.fn(async () => ({
      ok: true,
      json: async () => ({
        __status: "ok",
        serviceName: "WrongService",
        serviceInstanceId: "wrong-1",
        numberOfRunningGraphs: 0,
        health: {},
        isActive: true,
        isNonResponsive: false,
        isBlocked: false,
        state: "healthy",
        acceptingWork: true,
        reportedAt: "2026-03-14T12:32:04.000Z",
      }),
    }));

    globalThis.fetch = fetchMock as typeof fetch;

    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "OrdersService";
    registry.serviceInstanceId = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
    registry.useSocket = true;
    registry.instances.set("OrdersService", [
      {
        uuid: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
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
    registry.instances.set("CadenzaDB", [
      {
        uuid: "cadenza-db",
        serviceName: "CadenzaDB",
        numberOfRunningGraphs: 0,
        isPrimary: false,
        isActive: true,
        isNonResponsive: false,
        isBlocked: false,
        runtimeState: "healthy",
        acceptingWork: true,
        health: {},
        isFrontend: false,
        isDatabase: true,
        transports: [
          {
            uuid: "cadenza-db-internal-bootstrap",
            serviceInstanceId: "cadenza-db",
            role: "internal",
            origin: "http://bootstrap.example:5000",
            protocols: ["rest", "socket"],
            securityProfile: null,
            authStrategy: null,
            deleted: false,
            clientCreated: true,
          },
          {
            uuid: "cadenza-db-transport-1",
            serviceInstanceId: "cadenza-db",
            role: "internal",
            origin: "http://cadenza-db:5000",
            protocols: ["rest", "socket"],
            securityProfile: null,
            authStrategy: null,
            deleted: false,
            clientCreated: true,
          },
        ],
      },
    ]);

    vi.spyOn(Cadenza, "inquire").mockResolvedValue({
      runtimeStatusReports: [
        {
          serviceName: "BillingService",
          serviceInstanceId: "billing-1",
          numberOfRunningGraphs: 0,
          health: {},
          isActive: true,
          isNonResponsive: false,
          isBlocked: false,
          state: "healthy",
          acceptingWork: true,
          reportedAt: "2026-03-14T12:32:04.100Z",
        },
      ],
      __inquiryMeta: {
        inquiry: "meta-runtime-status",
        responded: 1,
        failed: 0,
        timedOut: 0,
        pending: 0,
      },
    } as any);

    await expect(
      (
        registry as {
          resolveRuntimeStatusFallbackInquiry: (
            serviceName: string,
            serviceInstanceId: string,
          ) => Promise<{
            report: Record<string, unknown>;
            inquiryMeta: Record<string, unknown>;
          }>;
        }
      ).resolveRuntimeStatusFallbackInquiry("CadenzaDB", "cadenza-db"),
    ).rejects.toMatchObject({
      message: "No runtime status report for CadenzaDB/cadenza-db",
      runtimeStatusFallback: expect.objectContaining({
        target: {
          serviceName: "CadenzaDB",
          serviceInstanceId: "cadenza-db",
        },
        instance: expect.objectContaining({
          exists: true,
          isDatabase: true,
          transports: expect.arrayContaining([
            expect.objectContaining({
              uuid: "cadenza-db-transport-1",
              origin: "http://cadenza-db:5000",
            }),
          ]),
        }),
        directStatusCheck: expect.objectContaining({
          attempted: true,
          outcome: "identity_mismatch",
          payloadServiceName: "WrongService",
          payloadServiceInstanceId: "wrong-1",
        }),
        inquiry: expect.objectContaining({
          meta: expect.objectContaining({
            responded: 1,
          }),
          reportTargets: expect.arrayContaining([
            expect.objectContaining({
              serviceName: "BillingService",
              serviceInstanceId: "billing-1",
            }),
          ]),
        }),
      }),
    });
  });

  it("reconciles bootstrap placeholders to the discovered remote instance id", () => {
    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "OrdersService";
    registry.serviceInstanceId = "orders-1";
    registry.instances.set("OrdersService", [
      {
        uuid: "orders-1",
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
    registry.instances.set("CadenzaDB", [
      {
        uuid: "cadenza-db",
        serviceName: "CadenzaDB",
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
        isBootstrapPlaceholder: true,
        transports: [
          {
            uuid: "cadenza-db-internal-bootstrap",
            serviceInstanceId: "cadenza-db",
            role: "internal",
            origin: "http://bootstrap.example:5000",
            protocols: ["rest", "socket"],
            securityProfile: null,
            authStrategy: null,
          },
        ],
      },
      {
        uuid: "c23a4dfd-d280-4baa-95e3-13c4e80851fb",
        serviceName: "CadenzaDB",
        numberOfRunningGraphs: 0,
        isPrimary: false,
        isActive: true,
        isNonResponsive: false,
        isBlocked: false,
        runtimeState: "healthy",
        acceptingWork: true,
        health: {},
        isFrontend: false,
        isDatabase: true,
        isBootstrapPlaceholder: false,
        transports: [
          {
            uuid: "c23a4dfd-d280-4baa-95e3-13c4e80851fb-internal",
            serviceInstanceId: "c23a4dfd-d280-4baa-95e3-13c4e80851fb",
            role: "internal",
            origin: "http://cadenza-db:5000",
            protocols: ["rest", "socket"],
            securityProfile: null,
            authStrategy: null,
          },
        ],
      },
    ]);

    (registry as { registerDependee: Function }).registerDependee(
      "CadenzaDB",
      "cadenza-db",
      {
        requiredForReadiness: true,
      },
    );

    (registry as {
      reconcileBootstrapPlaceholderInstance: (
        serviceName: string,
        resolvedInstanceId: string,
        emit: (signalName: string, ctx: Record<string, unknown>) => void,
      ) => void;
    }).reconcileBootstrapPlaceholderInstance(
      "CadenzaDB",
      "c23a4dfd-d280-4baa-95e3-13c4e80851fb",
      () => {},
    );

    const instances = registry.instances.get("CadenzaDB");
    expect(instances).toEqual([
      expect.objectContaining({
        uuid: "c23a4dfd-d280-4baa-95e3-13c4e80851fb",
        isDatabase: true,
        isBootstrapPlaceholder: false,
      }),
    ]);
    expect(registry.dependeeByInstance.has("cadenza-db")).toBe(false);
    expect(
      registry.dependeeByInstance.get("c23a4dfd-d280-4baa-95e3-13c4e80851fb"),
    ).toBe("CadenzaDB");
    expect(
      registry.readinessDependeeByInstance.has(
        "c23a4dfd-d280-4baa-95e3-13c4e80851fb",
      ),
    ).toBe(true);
  });

  it("preserves public bootstrap transport for frontend runtimes when the resolved instance has only internal transports", () => {
    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "DemoFrontend";
    registry.serviceInstanceId = "frontend-1";
    registry.isFrontend = true;
    registry.instances.set("CadenzaDB", [
      {
        uuid: "cadenza-db",
        serviceName: "CadenzaDB",
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
        isBootstrapPlaceholder: true,
        transports: [
          {
            uuid: "cadenza-db-public-bootstrap",
            serviceInstanceId: "cadenza-db",
            role: "public",
            origin: "http://cadenza-db.localhost",
            protocols: ["rest", "socket"],
            securityProfile: null,
            authStrategy: null,
          },
        ],
      },
      {
        uuid: "resolved-cadenza-db",
        serviceName: "CadenzaDB",
        numberOfRunningGraphs: 0,
        isPrimary: false,
        isActive: true,
        isNonResponsive: false,
        isBlocked: false,
        runtimeState: "healthy",
        acceptingWork: true,
        health: {},
        isFrontend: false,
        isDatabase: true,
        isBootstrapPlaceholder: false,
        transports: [
          {
            uuid: "resolved-cadenza-db-internal",
            serviceInstanceId: "resolved-cadenza-db",
            role: "internal",
            origin: "http://cadenza-db-service:8080",
            protocols: ["rest", "socket"],
            securityProfile: null,
            authStrategy: null,
          },
        ],
      },
    ]);

    (registry as {
      reconcileBootstrapPlaceholderInstance: (
        serviceName: string,
        resolvedInstanceId: string,
        emit: (signalName: string, ctx: Record<string, unknown>) => void,
      ) => void;
    }).reconcileBootstrapPlaceholderInstance(
      "CadenzaDB",
      "resolved-cadenza-db",
      () => {},
    );

    const instances = registry.instances.get("CadenzaDB");
    expect(instances).toEqual([
      expect.objectContaining({
        uuid: "resolved-cadenza-db",
        transports: expect.arrayContaining([
          expect.objectContaining({
            uuid: "resolved-cadenza-db-internal",
            role: "internal",
            origin: "http://cadenza-db-service:8080",
          }),
          expect.objectContaining({
            uuid: "cadenza-db-public-bootstrap",
            serviceInstanceId: "resolved-cadenza-db",
            role: "public",
            origin: "http://cadenza-db.localhost",
          }),
        ]),
      }),
    ]);
  });

  it("adopts the handshake-reported instance id for bootstrap placeholders", async () => {
    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "OrdersService";
    registry.serviceInstanceId = "orders-1";
    registry.instances.set("OrdersService", [
      {
        uuid: "orders-1",
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
    registry.instances.set("CadenzaDB", [
      {
        uuid: "cadenza-db",
        serviceName: "CadenzaDB",
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
        isBootstrapPlaceholder: true,
        transports: [
          {
            uuid: "cadenza-db-internal-bootstrap",
            serviceInstanceId: "cadenza-db",
            role: "internal",
            origin: "http://bootstrap.example:5000",
            protocols: ["rest", "socket"],
            securityProfile: null,
            authStrategy: null,
          },
        ],
      },
    ]);

    (registry as { registerDependee: Function }).registerDependee(
      "CadenzaDB",
      "cadenza-db",
      {
        requiredForReadiness: true,
      },
    );

    Cadenza.emit("meta.fetch.handshake_complete", {
      serviceName: "CadenzaDB",
      serviceInstanceId: "0541fea7-23e6-4ebf-aab7-06cbe0a8d52f",
      serviceTransportId: "cadenza-db-internal-bootstrap",
      serviceOrigin: "http://bootstrap.example:5000",
      transportProtocols: ["rest", "socket"],
    });

    await new Promise((resolve) => setTimeout(resolve, 50));

    const instances = registry.instances.get("CadenzaDB");
    expect(instances).toEqual([
      expect.objectContaining({
        uuid: "0541fea7-23e6-4ebf-aab7-06cbe0a8d52f",
        isBootstrapPlaceholder: false,
        transports: [
          expect.objectContaining({
            uuid: "cadenza-db-internal-bootstrap",
            serviceInstanceId: "0541fea7-23e6-4ebf-aab7-06cbe0a8d52f",
          }),
        ],
      }),
    ]);
    expect(
      registry.dependeeByInstance.get("0541fea7-23e6-4ebf-aab7-06cbe0a8d52f"),
    ).toBe("CadenzaDB");
  });

  it("retires older remote instances that advertise the same routeable endpoint after a successful handshake", async () => {
    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "OrdersService";
    registry.serviceInstanceId = "orders-1";

    const instanceUpdates: Array<Record<string, unknown>> = [];
    const transportUpdates: Array<Record<string, unknown>> = [];

    Cadenza.createEphemeralMetaTask("Capture superseded instance update", (ctx) => {
      instanceUpdates.push(ctx);
      return true;
    }).doOn("global.meta.service_instance.updated");

    Cadenza.createEphemeralMetaTask("Capture superseded transport update", (ctx) => {
      transportUpdates.push(ctx);
      return true;
    }).doOn("global.meta.service_instance_transport.updated");

    registry.instances.set("OrdersService", [
      {
        uuid: "orders-1",
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
        health: {},
        isFrontend: false,
        isDatabase: true,
        transports: [
          {
            uuid: "55555555-5555-4555-8555-555555555555",
            serviceInstanceId: "cadenza-db-old",
            role: "internal",
            origin: "http://cadenza-db:5000",
            protocols: ["rest", "socket"],
            securityProfile: null,
            authStrategy: null,
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
        health: {},
        isFrontend: false,
        isDatabase: true,
        transports: [
          {
            uuid: "66666666-6666-4666-8666-666666666666",
            serviceInstanceId: "cadenza-db-new",
            role: "internal",
            origin: "http://cadenza-db:5000",
            protocols: ["rest", "socket"],
            securityProfile: null,
            authStrategy: null,
          },
        ],
      },
    ]);

    Cadenza.emit("meta.fetch.handshake_complete", {
      serviceName: "CadenzaDB",
      serviceInstanceId: "cadenza-db-new",
      serviceTransportId: "66666666-6666-4666-8666-666666666666",
      serviceOrigin: "http://cadenza-db:5000",
      transportProtocols: ["rest", "socket"],
    });

    await new Promise((resolve) => setTimeout(resolve, 50));

    expect(registry.instances.get("CadenzaDB")).toEqual([
      expect.objectContaining({
        uuid: "cadenza-db-new",
      }),
    ]);
    expect(instanceUpdates).toEqual(
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
    expect(transportUpdates).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          data: expect.objectContaining({
            deleted: true,
          }),
          filter: {
            uuid: "55555555-5555-4555-8555-555555555555",
          },
        }),
      ]),
    );
  });
  it("persists graceful shutdown directly to authority via delegation", async () => {
    const fetchMock = vi.fn(async (_url: string, _init?: RequestInit) => ({
      ok: true,
      json: async () => ({
        __status: "success",
      }),
    }));

    globalThis.fetch = fetchMock as typeof fetch;

    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "OrdersService";
    registry.serviceInstanceId = "99999999-9999-4999-8999-999999999999";
    registry.connectsToCadenzaDB = true;
    registry.instances.set("OrdersService", [
      {
        uuid: "99999999-9999-4999-8999-999999999999",
        serviceName: "OrdersService",
        numberOfRunningGraphs: 0,
        isPrimary: true,
        isActive: true,
        isNonResponsive: false,
        isBlocked: false,
        runtimeState: "healthy",
        acceptingWork: true,
        reportedAt: "2026-03-24T23:00:00.000Z",
        health: {},
        isFrontend: false,
        isDatabase: false,
        transports: [
          {
            uuid: "77777777-7777-4777-8777-777777777777",
            serviceInstanceId: "99999999-9999-4999-8999-999999999999",
            role: "internal",
            origin: "http://orders:3000",
            protocols: ["rest", "socket"],
            securityProfile: null,
            authStrategy: null,
            deleted: false,
          },
        ],
      },
    ]);
    registry.instances.set("CadenzaDB", [
      {
        uuid: "cadenza-db",
        serviceName: "CadenzaDB",
        numberOfRunningGraphs: 0,
        isPrimary: true,
        isActive: true,
        isNonResponsive: false,
        isBlocked: false,
        runtimeState: "healthy",
        acceptingWork: true,
        health: {},
        isFrontend: false,
        isDatabase: true,
        transports: [
          {
            uuid: "88888888-8888-4888-8888-888888888888",
            serviceInstanceId: "cadenza-db",
            role: "internal",
            origin: "http://cadenza-db:5000",
            protocols: ["rest", "socket"],
            securityProfile: null,
            authStrategy: null,
            deleted: false,
            clientCreated: true,
          },
        ],
      },
    ]);

    const persisted = await registry.reportLocalShutdownToAuthority(
      "SIGTERM",
      3_000,
    );

    expect(persisted).toBe(true);
    expect(fetchMock).toHaveBeenCalledTimes(3);

    const firstRequestBody = JSON.parse(
      String(fetchMock.mock.calls[0]?.[1]?.body ?? "{}"),
    );
    const secondRequestBody = JSON.parse(
      String(fetchMock.mock.calls[1]?.[1]?.body ?? "{}"),
    );
    const thirdRequestBody = JSON.parse(
      String(fetchMock.mock.calls[2]?.[1]?.body ?? "{}"),
    );

    expect(fetchMock.mock.calls[0]?.[0]).toBe(
      "http://cadenza-db:5000/delegation",
    );
    expect(firstRequestBody).toMatchObject({
      __remoteRoutineName: "Update service_instance",
      __serviceName: "CadenzaDB",
      queryData: {
        filter: {
          uuid: "99999999-9999-4999-8999-999999999999",
        },
      },
      data: {
        is_active: false,
        is_non_responsive: false,
        deleted: false,
      },
    });
    expect(secondRequestBody).toMatchObject({
      __remoteRoutineName: "Update service_instance_lease",
      __serviceName: "CadenzaDB",
      queryData: {
        filter: {
          service_instance_id: "99999999-9999-4999-8999-999999999999",
        },
      },
      data: {
        status: "inactive",
        is_ready: false,
        readiness_reason: "graceful_shutdown",
      },
    });
    expect(thirdRequestBody).toMatchObject({
      __remoteRoutineName: "Update service_instance_transport",
      __serviceName: "CadenzaDB",
      queryData: {
        filter: {
          uuid: "77777777-7777-4777-8777-777777777777",
        },
      },
      data: {
        deleted: true,
      },
    });
    expect(registry.instances.get("OrdersService")?.[0]).toMatchObject({
      isActive: false,
      isNonResponsive: false,
      transports: [
        expect.objectContaining({
          uuid: "77777777-7777-4777-8777-777777777777",
          deleted: true,
        }),
      ],
    });
  });

  it("returns false when graceful shutdown cannot reach authority directly", async () => {
    const registry = ServiceRegistry.instance as any;
    registry.serviceName = "OrdersService";
    registry.serviceInstanceId = "orders-1";
    registry.connectsToCadenzaDB = true;
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
        health: {},
        isFrontend: false,
        isDatabase: false,
        transports: [],
      },
    ]);

    await expect(
      registry.reportLocalShutdownToAuthority("SIGTERM", 3_000),
    ).resolves.toBe(false);
  });
});
