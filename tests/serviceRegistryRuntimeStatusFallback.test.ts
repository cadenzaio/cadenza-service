import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import Cadenza from "../src/Cadenza";
import DatabaseController from "@service-database-controller";
import GraphMetadataController from "../src/graph/controllers/GraphMetadataController";
import GraphSyncController from "../src/graph/controllers/GraphSyncController";
import RestController from "../src/network/RestController";
import ServiceRegistry from "../src/registry/ServiceRegistry";
import SignalController from "../src/signals/SignalController";
import SocketController from "../src/network/SocketController";
import { selectTransportForRole } from "../src/utils/transport";

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
    registry.serviceInstanceId = "orders-1";
    registry.useSocket = true;
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
    registry.serviceInstanceId = "orders-1";
    registry.useSocket = true;
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

    await new Promise((resolve) => setTimeout(resolve, 25));

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
    expect(registry.dependeeByInstance.has("cadenza-db")).toBe(false);
    expect(
      registry.dependeeByInstance.get("0541fea7-23e6-4ebf-aab7-06cbe0a8d52f"),
    ).toBe("CadenzaDB");
  });
});
