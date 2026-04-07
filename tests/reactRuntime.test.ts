import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { act, create, type ReactTestRenderer } from "react-test-renderer";
import { createElement } from "react";

import Cadenza from "../src/Cadenza";
import type { CadenzaReactRuntime } from "../src/react";
import {
  CadenzaRuntimeProvider,
  createCadenzaReactRuntime,
  useCadenzaProjectionSelector,
  useCadenzaProjectionState,
  useCadenzaRuntime,
  useCadenzaRuntimeReady,
} from "../src/react";

type ProjectionState = {
  flags: {
    maintenanceMode: boolean;
  };
  metrics: {
    alertCount: number;
  };
  devicesById: Record<string, { online: boolean }>;
  liveFeed: string[];
};

const originalConsoleError = console.error;

function resetRuntimeState() {
  try {
    Cadenza.reset();
  } catch {
    // Ignore first-run reset errors before bootstrap.
  }
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

describe("React browser runtime wrapper", () => {
  let renderer: ReactTestRenderer | undefined;
  let consoleErrorSpy: ReturnType<typeof vi.spyOn> | undefined;

  beforeEach(() => {
    (globalThis as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;
    consoleErrorSpy = vi
      .spyOn(console, "error")
      .mockImplementation((message?: unknown, ...args: unknown[]) => {
        if (
          typeof message === "string" &&
          message.includes("react-test-renderer is deprecated")
        ) {
          return;
        }

        originalConsoleError(message, ...args);
      });
    resetRuntimeState();
  });

  afterEach(() => {
    if (renderer) {
      act(() => {
        renderer?.unmount();
      });
    }
    renderer = undefined;
    consoleErrorSpy?.mockRestore();
    consoleErrorSpy = undefined;
    delete (globalThis as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT;
    resetRuntimeState();
  });

  it("creates the browser runtime actor with the expected options", () => {
    const fakeHandle = {
      actor: {} as any,
      actorKey: "browser-runtime",
      waitUntilReady: async () => {},
      inquire: vi.fn(),
      getRuntimeState: () => ({
        ready: false,
        projectionState: {
          count: 0,
        },
        lastReadyAt: null,
        lastSyncRequestedAt: null,
      }),
      subscribe: () => () => {},
    };
    const cadenza = {
      createBrowserRuntimeActor: vi.fn(() => fakeHandle),
    } as any;

    const runtime = createCadenzaReactRuntime({
      cadenza,
      actorName: "BrowserDashboardRuntimeActor",
      service: {
        name: "BrowserApp",
        description: "Frontend app",
        useSocket: false,
      },
      bootstrapUrl: () => "http://cadenza-db.example:7000",
      hydration: () => ({
        initialInquiryResults: {
          "dashboard.seed": {
            ok: true,
          },
        },
      }),
      initialProjectionState: {
        count: 0,
      },
      commands: () => ({
        refresh: () => "ok",
      }),
    });

    expect(cadenza.createBrowserRuntimeActor).toHaveBeenCalledWith({
      actorName: "BrowserDashboardRuntimeActor",
      initialProjectionState: {
        count: 0,
      },
      service: {
        name: "BrowserApp",
        description: "Frontend app",
        useSocket: false,
        bootstrap: {
          url: "http://cadenza-db.example:7000",
        },
        hydration: {
          initialInquiryResults: {
            "dashboard.seed": {
              ok: true,
            },
          },
        },
      },
    });
    expect(runtime.getSnapshot()).toEqual({
      ready: false,
      projectionState: {
        count: 0,
      },
    });
    expect(runtime.commands.refresh()).toBe("ok");
  });

  it("projects signals into structured state, exposes commands, and rerenders selector consumers only for selected slices", async () => {
    Cadenza.createTask("Respond to wrapped react inquiry", () => ({
      ok: true,
    })).respondsTo("wrapped-react-runtime-intent");

    const runtime = createCadenzaReactRuntime<
      ProjectionState,
      {
        sendIntent: () => Promise<any>;
      }
    >({
      cadenza: Cadenza,
      actorName: "BrowserDashboardRuntimeActor",
      service: {
        name: "BrowserApp",
        description: "Frontend app",
        useSocket: false,
        cadenzaDB: {
          connect: false,
        },
      },
      bootstrapUrl: "http://cadenza-db.example:7000",
      initialProjectionState: {
        flags: {
          maintenanceMode: false,
        },
        metrics: {
          alertCount: 0,
        },
        devicesById: {},
        liveFeed: [],
      },
      signalBindings: [
        {
          signal: "global.demo.maintenance.enabled",
          reduce: (current) => ({
            ...current,
            flags: {
              ...current.flags,
              maintenanceMode: true,
            },
          }),
        },
        {
          signal: "global.demo.alert.raised",
          reduce: (current, payload) => ({
            ...current,
            metrics: {
              ...current.metrics,
              alertCount: current.metrics.alertCount + 1,
            },
            liveFeed: [
              String(payload.message ?? "alert"),
              ...current.liveFeed,
            ],
          }),
        },
        {
          signal: "global.demo.device.online",
          reduce: (current, payload) => ({
            ...current,
            devicesById: {
              ...current.devicesById,
              [String(payload.deviceId)]: {
                online: true,
              },
            },
          }),
        },
      ],
      commands: ({ cadenza, runtime }) => ({
        sendIntent: async () => {
          await runtime.waitUntilReady();
          return cadenza.inquire(
            "wrapped-react-runtime-intent",
            {},
            {
              overallTimeoutMs: 500,
            },
          );
        },
      }),
    });

    const renderCounts = {
      fullState: 0,
      maintenance: 0,
      alertCount: 0,
      device: 0,
      liveFeed: 0,
      ready: 0,
    };
    let latestProjectionState:
      | ReturnType<typeof useCadenzaProjectionState<ProjectionState>>
      | undefined;
    let latestRuntime:
      | CadenzaReactRuntime<
          ProjectionState,
          {
            sendIntent: () => Promise<any>;
          }
        >
      | undefined;
    let latestDeviceOnline = false;

    function FullStateConsumer() {
      renderCounts.fullState += 1;
      latestProjectionState = useCadenzaProjectionState<ProjectionState>();
      return createElement("div");
    }

    function MaintenanceConsumer() {
      renderCounts.maintenance += 1;
      useCadenzaProjectionSelector<ProjectionState, boolean>(
        (state) => state.flags.maintenanceMode,
      );
      return createElement("div");
    }

    function AlertCountConsumer() {
      renderCounts.alertCount += 1;
      useCadenzaProjectionSelector<ProjectionState, number>(
        (state) => state.metrics.alertCount,
      );
      return createElement("div");
    }

    function DeviceConsumer() {
      renderCounts.device += 1;
      latestDeviceOnline = useCadenzaProjectionSelector<ProjectionState, boolean>(
        (state) => state.devicesById["device-7"]?.online ?? false,
      );
      return createElement("div");
    }

    function LiveFeedConsumer() {
      renderCounts.liveFeed += 1;
      useCadenzaProjectionSelector<ProjectionState, number>(
        (state) => state.liveFeed.length,
      );
      return createElement("div");
    }

    function ReadyConsumer() {
      renderCounts.ready += 1;
      useCadenzaRuntimeReady();
      return createElement("div");
    }

    function RuntimeConsumer() {
      latestRuntime = useCadenzaRuntime<
        CadenzaReactRuntime<
          ProjectionState,
          {
            sendIntent: () => Promise<any>;
          }
        >
      >();
      return createElement("div");
    }

    await act(async () => {
      renderer = create(
        createElement(
          CadenzaRuntimeProvider,
          { runtime },
          createElement(FullStateConsumer),
          createElement(MaintenanceConsumer),
          createElement(AlertCountConsumer),
          createElement(DeviceConsumer),
          createElement(LiveFeedConsumer),
          createElement(ReadyConsumer),
          createElement(RuntimeConsumer),
        ),
      );
    });

    expect(latestProjectionState).toEqual({
      ready: false,
      projectionState: {
        flags: {
          maintenanceMode: false,
        },
        metrics: {
          alertCount: 0,
        },
        devicesById: {},
        liveFeed: [],
      },
    });
    const baselineRenderCounts = { ...renderCounts };

    await act(async () => {
      Cadenza.emit("global.demo.device.online", {
        deviceId: "device-7",
      });
    });

    await waitForCondition(() => latestDeviceOnline === true);
    expect(renderCounts.maintenance).toBe(baselineRenderCounts.maintenance);
    expect(renderCounts.alertCount).toBe(baselineRenderCounts.alertCount);
    expect(renderCounts.device).toBe(baselineRenderCounts.device + 1);
    expect(renderCounts.liveFeed).toBe(baselineRenderCounts.liveFeed);
    expect(renderCounts.fullState).toBe(baselineRenderCounts.fullState + 1);

    await act(async () => {
      Cadenza.emit("global.demo.alert.raised", {
        message: "Alert raised",
      });
    });

    await waitForCondition(
      () =>
        latestProjectionState?.projectionState.metrics.alertCount === 1 &&
        latestProjectionState?.projectionState.liveFeed[0] === "Alert raised",
    );
    expect(renderCounts.maintenance).toBe(baselineRenderCounts.maintenance);
    expect(renderCounts.alertCount).toBe(baselineRenderCounts.alertCount + 1);
    expect(renderCounts.device).toBe(baselineRenderCounts.device + 1);
    expect(renderCounts.liveFeed).toBe(baselineRenderCounts.liveFeed + 1);
    expect(renderCounts.fullState).toBe(baselineRenderCounts.fullState + 2);

    await act(async () => {
      Cadenza.emit("global.demo.maintenance.enabled", {});
    });

    await waitForCondition(
      () =>
        latestProjectionState?.projectionState.flags.maintenanceMode === true,
    );
    expect(renderCounts.maintenance).toBe(baselineRenderCounts.maintenance + 1);
    expect(renderCounts.alertCount).toBe(baselineRenderCounts.alertCount + 1);
    expect(renderCounts.device).toBe(baselineRenderCounts.device + 1);
    expect(renderCounts.liveFeed).toBe(baselineRenderCounts.liveFeed + 1);
    expect(renderCounts.fullState).toBe(baselineRenderCounts.fullState + 3);

    await act(async () => {
      Cadenza.emit("meta.service_registry.initial_sync_complete", {});
      await runtime.waitUntilReady();
    });

    await waitForCondition(() => latestProjectionState?.ready === true);
    expect(renderCounts.ready).toBe(baselineRenderCounts.ready + 1);
    await expect(latestRuntime?.commands.sendIntent()).resolves.toMatchObject({
      ok: true,
    });
  });

  it("throws a clear error when hooks are used outside the provider", () => {
    function OrphanConsumer() {
      useCadenzaRuntime();
      return createElement("div");
    }

    expect(() =>
      act(() => {
        create(createElement(OrphanConsumer));
      }),
    ).toThrow(
      /CadenzaRuntimeProvider is required/i,
    );
  });

  it("keeps multiple runtime providers isolated", async () => {
    const runtimeA = createCadenzaReactRuntime({
      cadenza: Cadenza,
      actorName: "BrowserDashboardRuntimeActorA",
      service: {
        name: "BrowserAppA",
        useSocket: false,
        cadenzaDB: {
          connect: false,
        },
      },
      bootstrapUrl: "http://cadenza-db.example:7000",
      initialProjectionState: {
        count: 0,
      },
      signalBindings: [
        {
          signal: "global.demo.runtime-a",
          reduce: (current) => ({
            count: current.count + 1,
          }),
        },
      ],
    });
    const runtimeB = createCadenzaReactRuntime({
      cadenza: Cadenza,
      actorName: "BrowserDashboardRuntimeActorB",
      service: {
        name: "BrowserAppB",
        useSocket: false,
        cadenzaDB: {
          connect: false,
        },
      },
      bootstrapUrl: "http://cadenza-db.example:7000",
      initialProjectionState: {
        count: 0,
      },
      signalBindings: [
        {
          signal: "global.demo.runtime-b",
          reduce: (current) => ({
            count: current.count + 1,
          }),
        },
      ],
    });

    let countA = 0;
    let countB = 0;

    function CounterA() {
      countA = useCadenzaProjectionSelector<{ count: number }, number>(
        (state) => state.count,
      );
      return createElement("div");
    }

    function CounterB() {
      countB = useCadenzaProjectionSelector<{ count: number }, number>(
        (state) => state.count,
      );
      return createElement("div");
    }

    await act(async () => {
      renderer = create(
        createElement(
          "div",
          undefined,
          createElement(
            CadenzaRuntimeProvider,
            { runtime: runtimeA },
            createElement(CounterA),
          ),
          createElement(
            CadenzaRuntimeProvider,
            { runtime: runtimeB },
            createElement(CounterB),
          ),
        ),
      );
    });

    expect(countA).toBe(0);
    expect(countB).toBe(0);

    await act(async () => {
      Cadenza.emit("global.demo.runtime-a", {});
    });

    await waitForCondition(() => countA === 1);
    expect(countA).toBe(1);
    expect(countB).toBe(0);

    await act(async () => {
      Cadenza.emit("global.demo.runtime-b", {});
    });

    await waitForCondition(() => countB === 1);
    expect(countA).toBe(1);
    expect(countB).toBe(1);
  });
});
