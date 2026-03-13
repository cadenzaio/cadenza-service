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
});
