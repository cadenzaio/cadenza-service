import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import Cadenza from "../src/Cadenza";
import RestController from "../src/network/RestController";
import type { AnyObject } from "@cadenza.io/core";

function resetRuntimeState() {
  try {
    Cadenza.emit("meta.server_shutdown_requested", {});
  } catch {
    // Ignore shutdown attempts before bootstrap.
  }

  try {
    Cadenza.reset();
  } catch {
    // Ignore first-run reset errors before bootstrap.
  }

  (RestController as any)._instance = undefined;
}

describe("RestController delegation resolution", () => {
  let consoleLogSpy: ReturnType<typeof vi.spyOn>;
  let consoleWarnSpy: ReturnType<typeof vi.spyOn>;
  let consoleErrorSpy: ReturnType<typeof vi.spyOn>;
  let originalNodeEnv: string | undefined;

  beforeEach(() => {
    consoleLogSpy = vi.spyOn(console, "log").mockImplementation(() => {});
    consoleWarnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
    consoleErrorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    originalNodeEnv = process.env.NODE_ENV;
    process.env.NODE_ENV = "development";
    resetRuntimeState();
    Cadenza.bootstrap();
    Cadenza.setMode("production");
    RestController.instance;
  });

  afterEach(() => {
    resetRuntimeState();
    process.env.NODE_ENV = originalNodeEnv;
    consoleLogSpy.mockRestore();
    consoleWarnSpy.mockRestore();
    consoleErrorSpy.mockRestore();
  });

  it("registers the service instance insert deputy with the snake_case target", () => {
    const serviceInstanceInsertTask = (Cadenza as any).serviceRegistry
      .insertServiceInstanceTask as any;

    expect(serviceInstanceInsertTask).toBeDefined();
    expect(serviceInstanceInsertTask?.name).toBe(
      "Insert service_instance in CadenzaDB",
    );
    expect(serviceInstanceInsertTask?.remoteRoutineName).toBe(
      "Insert service_instance",
    );
  });

  it("returns an error response when delegation target lookup fails", async () => {
    const networkConfiguredPromise = new Promise<AnyObject>((resolve) => {
      Cadenza.createEphemeralMetaTask(
        "Observe rest network configured",
        (ctx) => {
          resolve(ctx);
          return true;
        },
        "Observes REST network configuration during tests",
        { register: false },
      ).doOn("global.meta.rest.network_configured");
    });

    Cadenza.emit("meta.service_registry.service_inserted", {
      __isDatabase: false,
      __networkMode: "dev",
      __port: 0,
      __securityProfile: "low",
      __serviceInstanceId: "rest-delegation-resolution-test",
      __serviceName: "RestDelegationResolutionTest",
    });

    const networkContext = await networkConfiguredPromise;
    const port = networkContext.httpServer?.address()?.port;

    const deputyExecId = "delegation-target-not-found";
    const response = await fetch(`http://localhost:${port}/delegation`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        __remoteRoutineName: "Insert serviceInstance",
        __metadata: {
          __deputyExecId: deputyExecId,
        },
      }),
    });

    const failureContext = await response.json();

    expect(response.ok).toBe(true);
    expect(failureContext.__status).toBe("error");
    expect(failureContext.errored).toBe(true);
    expect(failureContext.__error).toBe(
      "No task or routine registered for delegation target Insert serviceInstance.",
    );
  }, 10_000);

  it("synthesizes delegation metadata for direct REST requests", async () => {
    const networkConfiguredPromise = new Promise<AnyObject>((resolve) => {
      Cadenza.createEphemeralMetaTask(
        "Observe rest network configured for synthetic delegation metadata",
        (ctx) => {
          resolve(ctx);
          return true;
        },
        "Observes REST network configuration during metadata synthesis tests",
        { register: false },
      ).doOn("global.meta.rest.network_configured");
    });

    Cadenza.emit("meta.service_registry.service_inserted", {
      __isDatabase: false,
      __networkMode: "dev",
      __port: 0,
      __securityProfile: "low",
      __serviceInstanceId: "rest-delegation-metadata-test",
      __serviceName: "RestDelegationMetadataTest",
    });

    const networkContext = await networkConfiguredPromise;
    const port = networkContext.httpServer?.address()?.port;

    const response = await fetch(`http://localhost:${port}/delegation`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        __remoteRoutineName: "Insert serviceInstance",
      }),
    });

    const failureContext = await response.json();

    expect(response.ok).toBe(true);
    expect(failureContext.__status).toBe("error");
    expect(failureContext.errored).toBe(true);
    expect(failureContext.__deputyExecId).toEqual(expect.any(String));
    expect(failureContext.__deputyExecId.length).toBeGreaterThan(0);
  }, 10_000);

  it("serves status checks without requiring a request body", async () => {
    (Cadenza.serviceRegistry as any).serviceName = "RestStatusRouteTest";
    (Cadenza.serviceRegistry as any).serviceInstanceId =
      "rest-status-route-test";

    const networkConfiguredPromise = new Promise<AnyObject>((resolve) => {
      Cadenza.createEphemeralMetaTask(
        "Observe rest network configured for status test",
        (ctx) => {
          resolve(ctx);
          return true;
        },
        "Observes REST network configuration during status route tests",
        { register: false },
      ).doOn("global.meta.rest.network_configured");
    });

    Cadenza.emit("meta.service_registry.service_inserted", {
      __isDatabase: false,
      __networkMode: "dev",
      __port: 0,
      __securityProfile: "low",
      __serviceInstanceId: "rest-status-route-test",
      __serviceName: "RestStatusRouteTest",
    });

    const networkContext = await networkConfiguredPromise;
    const port = networkContext.httpServer?.address()?.port;
    (Cadenza.serviceRegistry as any).instances.set("RestStatusRouteTest", [
      {
        uuid: "rest-status-route-test",
        serviceName: "RestStatusRouteTest",
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
            uuid: "rest-status-route-test-internal",
            serviceInstanceId: "rest-status-route-test",
            role: "internal",
            origin: `http://localhost:${port}`,
            protocols: ["rest"],
            securityProfile: null,
            authStrategy: null,
          },
        ],
      },
    ]);

    const response = await fetch(`http://localhost:${port}/status`, {
      method: "GET",
      signal: AbortSignal.timeout(2_000),
    });

    const statusContext = await response.json();

    expect(response.ok).toBe(true);
    expect(statusContext).toEqual(
      expect.objectContaining({
        __status: "ok",
        __serviceName: "RestStatusRouteTest",
        __serviceInstanceId: "rest-status-route-test",
        serviceName: "RestStatusRouteTest",
        serviceInstanceId: "rest-status-route-test",
        isActive: true,
        state: "healthy",
        health: expect.objectContaining({
          runtimeStatus: expect.objectContaining({
            state: "healthy",
            acceptingWork: true,
          }),
        }),
      }),
    );
  }, 10_000);
});
