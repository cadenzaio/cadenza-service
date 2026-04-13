import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import Cadenza from "../src/Cadenza";
import RestController from "../src/network/RestController";
import type { AnyObject } from "@cadenza.io/core";
import { buildTransportHandleKey as buildHandleKey } from "../src/utils/transport";

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

async function waitForCondition(
  predicate: () => boolean,
  timeoutMs = 1_000,
): Promise<void> {
  const startedAt = Date.now();
  while (Date.now() - startedAt < timeoutMs) {
    if (predicate()) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  throw new Error("Timed out waiting for condition");
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

  it("registers the service instance insert resolver for authority routing", () => {
    const serviceInstanceInsertTask = (Cadenza as any).serviceRegistry
      .insertServiceInstanceTask as any;

    expect(serviceInstanceInsertTask).toBeDefined();
    expect(serviceInstanceInsertTask?.name).toBe(
      "Resolve service registry insert for service_instance",
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

  it("resolves direct task delegations without requiring a matching routine lookup", async () => {
    Cadenza.createMetaTask(
      "Resolve rest delegation test task",
      (ctx) => ({
        ok: true,
        echoed: ctx.payload ?? null,
      }),
      "Resolves a direct REST delegation task during tests.",
    );

    const networkConfiguredPromise = new Promise<AnyObject>((resolve) => {
      Cadenza.createEphemeralMetaTask(
        "Observe rest network configured for direct task delegation",
        (ctx) => {
          resolve(ctx);
          return true;
        },
        "Observes REST network configuration during direct task delegation tests",
        { register: false },
      ).doOn("global.meta.rest.network_configured");
    });

    Cadenza.emit("meta.service_registry.service_inserted", {
      __isDatabase: false,
      __networkMode: "dev",
      __port: 0,
      __securityProfile: "low",
      __serviceInstanceId: "rest-direct-task-delegation-test",
      __serviceName: "RestDirectTaskDelegationTest",
    });

    const networkContext = await networkConfiguredPromise;
    const port = networkContext.httpServer?.address()?.port;

    const response = await fetch(`http://localhost:${port}/delegation`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        __remoteRoutineName: "Resolve rest delegation test task",
        payload: { deviceId: "device-7" },
        __metadata: {
          __deputyExecId: "rest-direct-task-delegation",
        },
      }),
    });

    const resultContext = await response.json();

    expect(response.ok).toBe(true);
    expect(resultContext.__status).toBe("success");
    expect(resultContext.ok).toBe(true);
    expect(resultContext.echoed).toEqual({ deviceId: "device-7" });
    expect(
      Cadenza.signalBroker
        .listObservedSignals()
        .filter(
          (signal) =>
            signal ===
            "meta.rest.delegation_target_not_found:rest-direct-task-delegation",
        ),
    ).toEqual([]);
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

  it("hoists inquiry lineage from delegation metadata onto direct REST requests", async () => {
    const networkConfiguredPromise = new Promise<AnyObject>((resolve) => {
      Cadenza.createEphemeralMetaTask(
        "Observe rest network configured for inquiry lineage hoist",
        (ctx) => {
          resolve(ctx);
          return true;
        },
        "Observes REST network configuration during delegation inquiry lineage tests",
        { register: false },
      ).doOn("global.meta.rest.network_configured");
    });

    Cadenza.emit("meta.service_registry.service_inserted", {
      __isDatabase: false,
      __networkMode: "dev",
      __port: 0,
      __securityProfile: "low",
      __serviceInstanceId: "rest-delegation-inquiry-lineage-test",
      __serviceName: "RestDelegationInquiryLineageTest",
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
        __metadata: {
          __inquiryId: "inquiry-1",
          __executionTraceId: "trace-1",
          __inquirySourceTaskExecutionId: "task-exec-1",
        },
      }),
    });

    const failureContext = await response.json();

    expect(response.ok).toBe(true);
    expect(failureContext.__status).toBe("error");
    expect(failureContext.__inquiryId).toBe("inquiry-1");
    expect(failureContext.__executionTraceId).toBe("trace-1");
    expect(failureContext.__inquirySourceTaskExecutionId).toBe("task-exec-1");
  }, 10_000);

  it("accepts delegation payloads larger than the default body-parser limit", async () => {
    const networkConfiguredPromise = new Promise<AnyObject>((resolve) => {
      Cadenza.createEphemeralMetaTask(
        "Observe rest network configured for large delegation payload test",
        (ctx) => {
          resolve(ctx);
          return true;
        },
        "Observes REST network configuration during large payload delegation tests",
        { register: false },
      ).doOn("global.meta.rest.network_configured");
    });

    Cadenza.emit("meta.service_registry.service_inserted", {
      __isDatabase: false,
      __networkMode: "dev",
      __port: 0,
      __securityProfile: "low",
      __serviceInstanceId: "rest-delegation-large-payload-test",
      __serviceName: "RestDelegationLargePayloadTest",
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
        payloadPadding: "x".repeat(200_000),
      }),
    });

    const failureContext = await response.json();

    expect(response.ok).toBe(true);
    expect(response.headers.get("content-type")).toContain("application/json");
    expect(failureContext.__status).toBe("error");
    expect(failureContext.errored).toBe(true);
    expect(failureContext.__error).toBe(
      "No task or routine registered for delegation target Insert serviceInstance.",
    );
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

  it("extends delegation timeout budgets for sync traffic", () => {
    const controller = RestController.instance as any;

    expect(controller.resolveDelegationTimeoutMs({})).toBe(30_000);
    expect(controller.resolveDelegationTimeoutMs({ __timeout: 120_000 })).toBe(
      120_000,
    );
    expect(
      controller.resolveDelegationTimeoutMs({
        __metadata: { __timeout: 45_000 },
      }),
    ).toBe(45_000);
    expect(
      controller.resolveDelegationTimeoutMs({
        joinedContexts: [{ __metadata: { __timeout: 90_000 } }],
      }),
    ).toBe(90_000);
    expect(controller.resolveDelegationTimeoutMs({ __syncing: true })).toBe(
      120_000,
    );
    expect(
      controller.resolveDelegationTimeoutMs({
        __metadata: { __syncing: true },
      }),
    ).toBe(120_000);
    expect(
      controller.resolveDelegationTimeoutMs({
        joinedContexts: [{ __metadata: { __syncing: true } }],
      }),
    ).toBe(120_000);
  });

  it("uses longer authority budgets for signal transmission", () => {
    const controller = RestController.instance as any;

    expect(
      controller.resolveSignalTransmissionTimeoutMs("OrdersService", {}),
    ).toBe(5_000);
    expect(
      controller.resolveSignalTransmissionTimeoutMs("CadenzaDB", {}),
    ).toBe(30_000);
    expect(
      controller.resolveSignalTransmissionTimeoutMs("CadenzaDB", {
        __metadata: { __syncing: true },
      }),
    ).toBe(120_000);
    expect(
      controller.resolveSignalTransmissionConcurrency("OrdersService"),
    ).toBe(0);
    expect(
      controller.resolveSignalTransmissionConcurrency("CadenzaDB"),
    ).toBe(10);
  });

  it("creates a fresh fetch delegation client when the same URL is re-registered with a new transport id", async () => {
    const sharedRegistration = {
      serviceName: "OrdersService",
      serviceInstanceId: "orders-1",
      serviceOrigin: "http://orders-service:8080",
      communicationTypes: ["rest"],
      transportProtocols: ["rest"],
    };

    Cadenza.emit("meta.service_registry.dependee_registered", {
      ...sharedRegistration,
      serviceTransportId: "orders-public-1",
    });

    Cadenza.emit("meta.service_registry.dependee_registered", {
      ...sharedRegistration,
      serviceTransportId: "orders-public-2",
    });

    await waitForCondition(
      () =>
        Boolean(
          Cadenza.get(
            `Delegate flow to REST server http://orders-service:8080 (${buildHandleKey("orders-public-2", "rest")})`,
          ),
        ),
      1_000,
    );

    expect(
      Cadenza.get(
        `Send Handshake to http://orders-service:8080 (${buildHandleKey("orders-public-1", "rest")})`,
      ),
    ).toBeDefined();
    expect(
      Cadenza.get(
        `Send Handshake to http://orders-service:8080 (${buildHandleKey("orders-public-2", "rest")})`,
      ),
    ).toBeDefined();
    expect(
      Cadenza.get(
        `Delegate flow to REST server http://orders-service:8080 (${buildHandleKey("orders-public-1", "rest")})`,
      ),
    ).toBeDefined();
    expect(
      Cadenza.get(
        `Delegate flow to REST server http://orders-service:8080 (${buildHandleKey("orders-public-2", "rest")})`,
      ),
    );
  });

  it("keeps current delegation payloads when an embedded snapshot is stale", async () => {
    const restController = RestController.instance as any;
    const fetchSpy = vi
      .spyOn(restController, "fetchDataWithTimeout")
      .mockResolvedValue({ __status: "success" });

    const setupFetchClientTask = Cadenza.get("Setup fetch client") as any;
    expect(setupFetchClientTask).toBeDefined();

    setupFetchClientTask.taskFunction({
      serviceName: "CadenzaDB",
      serviceOrigin: "http://cadenza-db-service:8080",
      serviceTransportId: "fetch-service-instance-test",
    });

    const delegateTask = Cadenza.get(
      `Delegate flow to REST server http://cadenza-db-service:8080 (${buildHandleKey("fetch-service-instance-test", "rest")})`,
    ) as any;
    expect(delegateTask).toBeDefined();

    await delegateTask.taskFunction(
      {
        __remoteRoutineName: "Insert service_instance",
        __metadata: {
          __deputyExecId: "service-instance-delegation-test",
        },
        data: {
          uuid: "orders-1",
          process_pid: 123,
          service_name: "OrdersService",
          is_active: true,
        },
        queryData: {
          data: {
            uuid: "orders-1",
            process_pid: 123,
            service_name: "OrdersService",
            is_active: true,
          },
          onConflict: {
            target: ["uuid"],
          },
        },
        onConflict: {
          target: ["name"],
        },
        __delegationRequestContext: {
          __remoteRoutineName: "Insert service",
          data: {
            name: "OrdersService",
            description: "Orders",
          },
          queryData: {
            data: {
              name: "OrdersService",
              description: "Orders",
            },
            onConflict: {
              target: ["name"],
            },
          },
        },
      },
      Cadenza.emit.bind(Cadenza),
    );

    expect(fetchSpy).toHaveBeenCalled();
    const [, options] = fetchSpy.mock.calls.at(-1) as [string, RequestInit];
    const requestBody = JSON.parse(String(options.body));

    expect(requestBody.data).toMatchObject({
      uuid: "orders-1",
      process_pid: 123,
      service_name: "OrdersService",
      is_active: true,
    });
    expect(requestBody.data).not.toMatchObject({
      name: "OrdersService",
      description: "Orders",
    });
    expect(requestBody.queryData).toMatchObject({
      data: {
        uuid: "orders-1",
        process_pid: 123,
        service_name: "OrdersService",
        is_active: true,
      },
      onConflict: {
        target: ["uuid"],
      },
    });
    expect(requestBody.queryData.data).not.toMatchObject({
      name: "OrdersService",
      description: "Orders",
    });
  });

  it("strips transport selection routing context before delegating over REST", async () => {
    const restController = RestController.instance as any;
    const fetchSpy = vi
      .spyOn(restController, "fetchDataWithTimeout")
      .mockResolvedValue({ __status: "success" });

    const setupFetchClientTask = Cadenza.get("Setup fetch client") as any;
    expect(setupFetchClientTask).toBeDefined();

    setupFetchClientTask.taskFunction({
      serviceName: "CadenzaDB",
      serviceOrigin: "http://cadenza-db-service:8080",
      serviceTransportId: "fetch-routing-strip-test",
    });

    const delegateTask = Cadenza.get(
      `Delegate flow to REST server http://cadenza-db-service:8080 (${buildHandleKey("fetch-routing-strip-test", "rest")})`,
    ) as any;
    expect(delegateTask).toBeDefined();

    await delegateTask.taskFunction(
      {
        __remoteRoutineName: "Insert execution_trace",
        __signalEmission: {
          fullSignalName:
            `meta.service_registry.selected_instance_for_fetch:${buildHandleKey("fetch-routing-strip-test", "rest")}`,
          taskName: "Get balanced instance",
          taskExecutionId: "transport-task-exec-1",
        },
        __signalEmissionId: "transport-signal-emission-1",
        __routineExecId: "transport-routine-exec-1",
        __localRoutineExecId: "original-routine-exec-1",
        __metadata: {
          __deputyExecId: "routing-strip-deputy-1",
          __routineExecId: "original-routine-exec-1",
          __executionTraceId: "trace-1",
        },
        data: {
          uuid: "trace-1",
        },
        queryData: {
          data: {
            uuid: "trace-1",
          },
        },
      },
      Cadenza.emit.bind(Cadenza),
    );

    expect(fetchSpy).toHaveBeenCalled();
    const [, options] = fetchSpy.mock.calls.at(-1) as [string, RequestInit];
    const requestBody = JSON.parse(String(options.body));

    expect(requestBody.__signalEmission).toBeUndefined();
    expect(requestBody.__signalEmissionId).toBeUndefined();
    expect(requestBody.__routineExecId).toBeUndefined();
    expect(requestBody.__localRoutineExecId).toBe("original-routine-exec-1");
    expect(requestBody.__metadata).toMatchObject({
      __deputyExecId: "routing-strip-deputy-1",
      __routineExecId: "original-routine-exec-1",
      __executionTraceId: "trace-1",
    });
  });

  it("emits delegate_failed when a REST delegation throws a transport error", async () => {
    const restController = RestController.instance as any;
    const fetchSpy = vi
      .spyOn(restController, "fetchDataWithTimeout")
      .mockRejectedValue(
        new Error(
          "FetchError: request to http://predictor:3005/delegation failed, reason: connect ECONNREFUSED 127.0.0.1:3005",
        ),
      );

    let failedCtx: AnyObject | null = null;
    Cadenza.createEphemeralMetaTask(
      "Capture REST delegation failure signal",
      (ctx) => {
        failedCtx = ctx;
        return true;
      },
      "",
      { register: false },
    ).doOn("meta.fetch.delegate_failed");

    const setupFetchClientTask = Cadenza.get("Setup fetch client") as any;
    expect(setupFetchClientTask).toBeDefined();

    setupFetchClientTask.taskFunction({
      serviceName: "PredictorService",
      serviceOrigin: "http://predictor:3005",
      serviceTransportId: "predictor-fetch-failure-test",
    });

    const delegateTask = Cadenza.get(
      `Delegate flow to REST server http://predictor:3005 (${buildHandleKey("predictor-fetch-failure-test", "rest")})`,
    ) as any;
    expect(delegateTask).toBeDefined();

    const result = await delegateTask.taskFunction(
      {
        __serviceName: "PredictorService",
        __instance: "predictor-1",
        __transportId: "predictor-fetch-failure-test",
        __transportOrigin: "http://predictor:3005",
        __transportProtocols: ["rest"],
        __fetchId: buildHandleKey("predictor-fetch-failure-test", "rest"),
        __remoteRoutineName: "Normalize prediction compute input",
        __metadata: {
          __deputyExecId: "predictor-fetch-failure-deputy",
        },
      },
      Cadenza.emit.bind(Cadenza),
    );

    expect(fetchSpy).toHaveBeenCalled();
    await waitForCondition(() => failedCtx !== null, 1_500);

    expect(result).toMatchObject({
      errored: true,
      __serviceName: "PredictorService",
      __instance: "predictor-1",
      __transportId: "predictor-fetch-failure-test",
    });
    expect(failedCtx).toMatchObject({
      errored: true,
      __signalName: "meta.fetch.delegate_failed",
      __serviceName: "PredictorService",
      __instance: "predictor-1",
      __transportId: "predictor-fetch-failure-test",
    });
    expect(String(failedCtx?.__error ?? "")).toContain("ECONNREFUSED");
  });

  it("does not re-schedule a handshake when a same-route fetch client is already healthy", () => {
    const debounceSpy = vi
      .spyOn(Cadenza, "debounce")
      .mockImplementation(() => true as any);

    const setupFetchClientTask = Cadenza.get("Setup fetch client") as any;
    expect(setupFetchClientTask).toBeDefined();

    const ctx = {
      serviceName: "PredictorService",
      serviceOrigin: "http://predictor-b:3005",
      serviceTransportId: "predictor-b-transport-1",
      routeKey: "PredictorService|internal|http://predictor-b:3005",
      fetchId: buildHandleKey(
        "PredictorService|internal|http://predictor-b:3005",
        "rest",
      ),
      communicationTypes: ["intent"],
      handshakeData: {
        instanceId: "telemetry-1",
        serviceName: "TelemetryCollectorService",
      },
    };

    setupFetchClientTask.taskFunction(ctx);
    const diagnostics = (RestController.instance as any).fetchClientDiagnostics.get(
      ctx.fetchId,
    );
    diagnostics.connected = true;
    diagnostics.destroyed = false;
    diagnostics.lastHandshakeError = null;
    debounceSpy.mockClear();

    const result = setupFetchClientTask.taskFunction(ctx);

    expect(result).toBe(true);
    expect(debounceSpy).not.toHaveBeenCalled();
  });

  it("re-schedules a handshake when a same-route fetch client already exists but is unhealthy", () => {
    const debounceSpy = vi
      .spyOn(Cadenza, "debounce")
      .mockImplementation(() => true as any);

    const setupFetchClientTask = Cadenza.get("Setup fetch client") as any;
    expect(setupFetchClientTask).toBeDefined();

    const ctx = {
      serviceName: "PredictorService",
      serviceOrigin: "http://predictor-b:3005",
      serviceTransportId: "predictor-b-transport-1",
      routeKey: "PredictorService|internal|http://predictor-b:3005",
      fetchId: buildHandleKey(
        "PredictorService|internal|http://predictor-b:3005",
        "rest",
      ),
      communicationTypes: ["intent"],
      handshakeData: {
        instanceId: "telemetry-1",
        serviceName: "TelemetryCollectorService",
      },
    };

    setupFetchClientTask.taskFunction(ctx);
    const diagnostics = (RestController.instance as any).fetchClientDiagnostics.get(
      ctx.fetchId,
    );
    diagnostics.connected = false;
    diagnostics.handshakeInFlight = false;
    diagnostics.destroyed = false;
    diagnostics.lastHandshakeError = "ECONNRESET";
    debounceSpy.mockClear();

    const result = setupFetchClientTask.taskFunction(ctx);

    expect(result).toBe(true);
    expect(debounceSpy).toHaveBeenCalledWith(
      `meta.fetch.handshake_requested:${ctx.fetchId}`,
      expect.objectContaining({
        serviceName: "PredictorService",
        serviceTransportId: "predictor-b-transport-1",
        fetchId: ctx.fetchId,
        routeKey: ctx.routeKey,
        transportProtocol: "rest",
      }),
      50,
    );
  });

  it("does not queue another handshake when one is already in flight for the same route", () => {
    const debounceSpy = vi
      .spyOn(Cadenza, "debounce")
      .mockImplementation(() => true as any);

    const setupFetchClientTask = Cadenza.get("Setup fetch client") as any;
    expect(setupFetchClientTask).toBeDefined();

    const ctx = {
      serviceName: "PredictorService",
      serviceOrigin: "http://predictor-b:3005",
      serviceTransportId: "predictor-b-transport-1",
      routeKey: "PredictorService|internal|http://predictor-b:3005",
      fetchId: buildHandleKey(
        "PredictorService|internal|http://predictor-b:3005",
        "rest",
      ),
      communicationTypes: ["intent"],
      handshakeData: {
        instanceId: "telemetry-1",
        serviceName: "TelemetryCollectorService",
      },
    };

    setupFetchClientTask.taskFunction(ctx);
    const diagnostics = (RestController.instance as any).fetchClientDiagnostics.get(
      ctx.fetchId,
    );
    diagnostics.connected = false;
    diagnostics.handshakeInFlight = true;
    diagnostics.destroyed = false;
    diagnostics.lastHandshakeError = "ECONNRESET";
    debounceSpy.mockClear();

    const result = setupFetchClientTask.taskFunction(ctx);

    expect(result).toBe(true);
    expect(debounceSpy).not.toHaveBeenCalled();
  });
});
