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
    const delegationSignal = `meta.service_registry.selected_instance_for_socket:${fetchId}`;

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
        Boolean(await controller.getSocketClientDiagnosticsEntry(fetchId)),
      1_000,
    );

    __getLastSocket()!.ackHandlers.delegation = (_data, callback) => {
      callback?.({ message: "mock delegation failure" }, undefined);
    };

    for (let i = 0; i < 15; i++) {
      const previousErrorCount =
        (await controller.getSocketClientDiagnosticsEntry(fetchId))
          ?.errorHistory.length ?? 0;

      Cadenza.emit(delegationSignal, {
        __remoteRoutineName: "remote-task",
        __metadata: {
          __deputyExecId: `delegation-${i}`,
        },
        __timeout: 10,
      });

      await waitForCondition(async () => {
        const state = await controller.getSocketClientDiagnosticsEntry(fetchId);
        return Boolean(
          state &&
            state.errorHistory.length > previousErrorCount &&
            state.pendingDelegations === 0 &&
            state.pendingTimers === 0,
        );
      }, 1_000);
    }

    const finalState = await controller.getSocketClientDiagnosticsEntry(fetchId);
    expect(finalState).toBeDefined();
    expect(finalState.pendingDelegations).toBe(0);
    expect(finalState.pendingTimers).toBe(0);
    expect(finalState.errorHistory.length).toBeGreaterThan(0);
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
