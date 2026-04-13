import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

vi.mock("socket.io-client", async () => {
  const { EventEmitter } = await import("node:events");
  let lastSocket: MockSocket | null = null;

  class MockSocket extends EventEmitter {
    connected = false;
    id = "mock-socket";
    ackHandlers: Record<
      string,
      (
        data: unknown,
        callback?: (err: unknown, response: unknown) => void,
      ) => void
    > = {};

    connect() {
      queueMicrotask(() => {
        this.connected = true;
        this.emit("connect");
      });
      return this;
    }

    close() {
      this.removeAllListeners();
      return this;
    }

    timeout(_ms: number) {
      return {
        emit: (
          event: string,
          data: unknown,
          callback?: (err: unknown, response: unknown) => void,
        ) => {
          if (this.ackHandlers[event]) {
            this.ackHandlers[event](data, callback);
            return;
          }
          callback?.(
            null,
            event === "handshake" ? { status: "success" } : { ok: true },
          );
        },
      };
    }
  }

  return {
    io: () => {
      lastSocket = new MockSocket();
      return lastSocket;
    },
    Socket: MockSocket,
    __getLastSocket: () => lastSocket,
  };
});

import Cadenza from "../src/Cadenza";
import SocketController from "../src/network/SocketController";
import { __getLastSocket } from "socket.io-client";
import { buildTransportHandleKey } from "../src/utils/transport";

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

  (SocketController as any)._instance = undefined;
}

describe("SocketController delegation cleanup", () => {
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
  });

  it("keeps socket setup as signal-driven actor tasks", () => {
    const setupSocketServerTask = Cadenza.get("Setup SocketServer");
    const connectSocketClientTask = Cadenza.get("Connect to socket server");

    expect(setupSocketServerTask).toBeDefined();
    expect(connectSocketClientTask).toBeDefined();
    expect(
      setupSocketServerTask?.observedSignals.has(
        "global.meta.rest.network_configured",
      ),
    ).toBe(true);
    expect(
      connectSocketClientTask?.observedSignals.has("meta.fetch.handshake_complete"),
    ).toBe(true);
  });

  it("does not initialize a socket client for REST-only handshakes", async () => {
    SocketController.instance;

    Cadenza.emit("meta.fetch.handshake_complete", {
      serviceInstanceId: "remote-rest-only",
      communicationTypes: ["signal"],
      serviceName: "RestOnlyService",
      serviceTransportId: "rest-only-transport",
      serviceOrigin: "http://rest-only.localhost:3001",
      transportProtocols: ["rest"],
    });

    await new Promise((resolve) => setTimeout(resolve, 50));

    expect(__getLastSocket()).toBeNull();
  });

  it("does not accumulate pending delegation/timer counters across failed retries", async () => {
    const controller = SocketController.instance as any;

    const serviceOrigin = "http://127.0.0.1:65531";
    const serviceName = "RetryLeakTestService";
    const fetchId = "retry-transport-1";
    const socketHandle = buildTransportHandleKey(fetchId, "socket");
    const delegationSignal = `meta.service_registry.selected_instance_for_socket:${socketHandle}`;

    Cadenza.emit("meta.fetch.handshake_complete", {
      serviceInstanceId: "remote-instance",
      communicationTypes: ["socket"],
      serviceName,
      serviceTransportId: fetchId,
      serviceOrigin,
      transportProtocols: ["rest", "socket"],
    });

    await waitForCondition(
      async () =>
        Boolean(await controller.getSocketClientDiagnosticsEntry(socketHandle)),
      1_000,
    );

    __getLastSocket()!.ackHandlers.delegation = (_data, callback) => {
      callback?.({ message: "mock delegation failure" }, undefined);
    };

    for (let i = 0; i < 15; i++) {
      const previousErrorCount =
        (await controller.getSocketClientDiagnosticsEntry(socketHandle))
          ?.errorHistory.length ?? 0;

      Cadenza.emit(delegationSignal, {
        __remoteRoutineName: "remote-task",
        __metadata: {
          __deputyExecId: `delegation-${i}`,
        },
        __timeout: 10,
      });

      await waitForCondition(async () => {
        const state = await controller.getSocketClientDiagnosticsEntry(socketHandle);
        return Boolean(
          state &&
            state.errorHistory.length > previousErrorCount &&
            state.pendingDelegations === 0 &&
            state.pendingTimers === 0,
        );
      }, 1_000);
    }

    const finalState = await controller.getSocketClientDiagnosticsEntry(socketHandle);
    expect(finalState).toBeDefined();
    expect(finalState.pendingDelegations).toBe(0);
    expect(finalState.pendingTimers).toBe(0);
    expect(finalState.errorHistory.length).toBeGreaterThan(0);
  });

  it("strips transport selection routing context before delegating over sockets", async () => {
    SocketController.instance;

    const fetchId = "socket-routing-strip-1";
    const socketHandle = buildTransportHandleKey(fetchId, "socket");
    Cadenza.emit("meta.fetch.handshake_complete", {
      serviceInstanceId: "remote-cadenza-db",
      communicationTypes: ["socket"],
      serviceName: "CadenzaDB",
      serviceTransportId: fetchId,
      serviceOrigin: "http://cadenza-db.localhost",
      transportProtocols: ["socket"],
    });

    await waitForCondition(() => Boolean(__getLastSocket()?.connected), 1_000);
    await waitForCondition(
      async () =>
        Boolean(
          await (SocketController.instance as any).getSocketClientDiagnosticsEntry(
            socketHandle,
          ),
        ),
      1_000,
    );

    let capturedPayload: any = null;
    __getLastSocket()!.ackHandlers.delegation = (data, callback) => {
      capturedPayload = data;
      callback?.(null, { __status: "success" });
    };

    Cadenza.emit(`meta.service_registry.selected_instance_for_socket:${socketHandle}`, {
      __remoteRoutineName: "Insert execution_trace",
      __signalEmission: {
        fullSignalName:
          `meta.service_registry.selected_instance_for_socket:${socketHandle}`,
        taskName: "Get balanced instance",
        taskExecutionId: "transport-task-exec-1",
      },
      __signalEmissionId: "transport-signal-emission-1",
      __routineExecId: "transport-routine-exec-1",
      __localRoutineExecId: "original-routine-exec-1",
      __traceCreatedByRunner: true,
      __routineCreatedByRunner: true,
      __routineName: "Transport insert routine",
      __routineVersion: 1,
      __routineCreatedAt: "2026-03-24T00:00:00.000Z",
      __routineIsMeta: true,
      __metadata: {
        __deputyExecId: "socket-routing-strip-deputy-1",
        __routineExecId: "original-routine-exec-1",
        __executionTraceId: "trace-1",
        __traceCreatedByRunner: true,
        __routineCreatedByRunner: true,
        __routineName: "Transport insert routine",
      },
      data: {
        uuid: "trace-1",
      },
      queryData: {
        data: {
          uuid: "trace-1",
        },
      },
    });

    await waitForCondition(() => capturedPayload !== null, 1_000);

    expect(capturedPayload.__signalEmission).toBeUndefined();
    expect(capturedPayload.__signalEmissionId).toBeUndefined();
    expect(capturedPayload.__routineExecId).toBeUndefined();
    expect(capturedPayload.__traceCreatedByRunner).toBeUndefined();
    expect(capturedPayload.__routineCreatedByRunner).toBeUndefined();
    expect(capturedPayload.__routineName).toBeUndefined();
    expect(capturedPayload.__localRoutineExecId).toBe("original-routine-exec-1");
    expect(capturedPayload.__metadata).toMatchObject({
      __deputyExecId: "socket-routing-strip-deputy-1",
      __routineExecId: "original-routine-exec-1",
      __executionTraceId: "trace-1",
    });
    expect(capturedPayload.__metadata.__traceCreatedByRunner).toBeUndefined();
    expect(capturedPayload.__metadata.__routineCreatedByRunner).toBeUndefined();
    expect(capturedPayload.__metadata.__routineName).toBeUndefined();
  });

  it("returns delegation lookup failures through the frontend socket client receiver", async () => {
    SocketController.instance;
    Cadenza.serviceRegistry.serviceName = "DemoFrontend";
    Cadenza.serviceRegistry.serviceInstanceId = "frontend-1";
    Cadenza.serviceRegistry.isFrontend = true;

    Cadenza.emit("meta.fetch.handshake_complete", {
      serviceInstanceId: "remote-cadenza-db",
      communicationTypes: ["socket"],
      serviceName: "CadenzaDB",
      serviceTransportId: "cadenza-db-public-1",
      serviceOrigin: "http://cadenza-db.localhost",
      transportProtocols: ["socket"],
    });

    await waitForCondition(() => Boolean(__getLastSocket()?.connected), 1_000);
    await waitForCondition(
      () => (__getLastSocket() as any)?.listenerCount?.("delegation") > 0,
      1_000,
    );

    const response = await new Promise<any>((resolve) => {
      __getLastSocket()?.emit(
        "delegation",
        {
          __remoteRoutineName: "Missing frontend delegation target",
          payload: {
            deviceId: "device-7",
          },
          __metadata: {
            __deputyExecId: "frontend-delegation-1",
          },
        },
        resolve,
      );
    });

    expect(response).toMatchObject({
      errored: true,
      __error:
        "No task or routine registered for delegation target Missing frontend delegation target.",
    });
  }, 10_000);

  it("resolves direct task delegations through the frontend socket client receiver", async () => {
    SocketController.instance;
    Cadenza.serviceRegistry.serviceName = "DemoFrontend";
    Cadenza.serviceRegistry.serviceInstanceId = "frontend-1";
    Cadenza.serviceRegistry.isFrontend = true;

    Cadenza.createMetaTask(
      "Resolve socket delegation test task",
      (ctx: any) => ({
        ok: true,
        echoed: ctx.payload ?? null,
      }),
      "Resolves a direct socket delegation task during tests.",
    );

    Cadenza.emit("meta.fetch.handshake_complete", {
      serviceInstanceId: "remote-cadenza-db",
      communicationTypes: ["socket"],
      serviceName: "CadenzaDB",
      serviceTransportId: "cadenza-db-public-1",
      serviceOrigin: "http://cadenza-db.localhost",
      transportProtocols: ["socket"],
    });

    await waitForCondition(() => Boolean(__getLastSocket()?.connected), 1_000);
    await waitForCondition(
      () => (__getLastSocket() as any)?.listenerCount?.("delegation") > 0,
      1_000,
    );

    const response = await new Promise<any>((resolve) => {
      __getLastSocket()?.emit(
        "delegation",
        {
          __remoteRoutineName: "Resolve socket delegation test task",
          payload: {
            deviceId: "device-9",
          },
          __metadata: {
            __deputyExecId: "frontend-direct-task-delegation",
          },
        },
        resolve,
      );
    });

    expect(response).toMatchObject({
      ok: true,
      echoed: { deviceId: "device-9" },
    });
  }, 10_000);

  it("ignores non-function trailing signal event arguments on the frontend socket client", async () => {
    SocketController.instance;
    Cadenza.serviceRegistry.serviceName = "DemoFrontend";
    Cadenza.serviceRegistry.serviceInstanceId = "frontend-1";
    Cadenza.serviceRegistry.isFrontend = true;

    let receivedSignalCtx: any = null;
    Cadenza.createTask(
      "Observe frontend socket signal",
      (ctx: any) => {
        receivedSignalCtx = ctx;
        return ctx;
      },
      "Captures server-pushed frontend signal payloads during socket tests.",
    ).doOn("demo.frontend.signal");

    Cadenza.emit("meta.fetch.handshake_complete", {
      serviceInstanceId: "remote-cadenza-db",
      communicationTypes: ["socket"],
      serviceName: "CadenzaDB",
      serviceTransportId: "cadenza-db-public-1",
      serviceOrigin: "http://cadenza-db.localhost",
      transportProtocols: ["socket"],
    });

    await waitForCondition(() => Boolean(__getLastSocket()?.connected), 1_000);
    await waitForCondition(
      () => (__getLastSocket() as any)?.listenerCount?.("signal") > 0,
      1_000,
    );

    expect(() =>
      __getLastSocket()?.emit(
        "signal",
        {
          __signalName: "demo.frontend.signal",
          payload: {
            deviceId: "device-7",
          },
        },
        {
          not: "an-ack-callback",
        },
      ),
    ).not.toThrow();

    await waitForCondition(() => Boolean(receivedSignalCtx), 1_000);
    expect(receivedSignalCtx).toMatchObject({
      payload: {
        deviceId: "device-7",
      },
    });
  });
});
