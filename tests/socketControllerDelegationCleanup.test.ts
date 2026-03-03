import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

vi.mock("socket.io-client", async () => {
  const { EventEmitter } = await import("node:events");

  class MockSocket extends EventEmitter {
    connected = false;
    id = "mock-socket";

    connect() {
      return this;
    }

    close() {
      this.removeAllListeners();
      return this;
    }

    timeout(_ms: number) {
      return {
        emit: (
          _event: string,
          _data: unknown,
          callback?: (err: unknown, response: unknown) => void,
        ) => {
          callback?.(null, { ok: true });
        },
      };
    }
  }

  return {
    io: () => new MockSocket(),
    Socket: MockSocket,
  };
});

import Cadenza from "../src/Cadenza";
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

  it("does not accumulate pending delegation/timer counters across failed retries", async () => {
    const controller = SocketController.instance as any;

    const serviceAddress = "127.0.0.1";
    const servicePort = 65531;
    const serviceName = "RetryLeakTestService";
    const fetchId = `${serviceAddress}_${servicePort}`;
    const delegationSignal = `meta.service_registry.selected_instance_for_socket:${fetchId}`;

    Cadenza.emit("meta.fetch.handshake_complete", {
      serviceInstanceId: "remote-instance",
      communicationTypes: ["socket"],
      serviceName,
      serviceAddress,
      servicePort,
      protocol: "http",
    });

    await waitForCondition(
      async () =>
        Boolean(await controller.getSocketClientDiagnosticsEntry(fetchId)),
      1_000,
    );

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
});
