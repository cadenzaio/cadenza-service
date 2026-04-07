import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { createApp, effectScope, watch, type App, type EffectScope } from "vue";

import Cadenza from "../src/Cadenza";
import type { CadenzaVueRuntime } from "../src/vue";
import {
  createCadenzaVueRuntime,
  useCadenzaProjectionSelector,
  useCadenzaProjectionState,
  useCadenzaRuntime,
  useCadenzaRuntimeReady,
  useCadenzaRuntimeState,
} from "../src/vue";

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

function runInAppContext<T>(app: App, callback: () => T): T {
  return app.runWithContext(callback);
}

describe("Vue browser runtime wrapper", () => {
  let scope: EffectScope | undefined;
  let consoleWarnSpy: ReturnType<typeof vi.spyOn> | undefined;

  beforeEach(() => {
    consoleWarnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
    resetRuntimeState();
  });

  afterEach(() => {
    scope?.stop();
    scope = undefined;
    consoleWarnSpy?.mockRestore();
    consoleWarnSpy = undefined;
    resetRuntimeState();
  });

  it("creates the browser runtime actor with the expected options and disposes cleanly", () => {
    const unsubscribe = vi.fn();
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
      subscribe: vi.fn(() => unsubscribe),
    };
    const cadenza = {
      createBrowserRuntimeActor: vi.fn(() => fakeHandle),
    } as any;

    const runtime = createCadenzaVueRuntime({
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
    expect(runtime.snapshot.value).toEqual({
      ready: false,
      projectionState: {
        count: 0,
      },
    });
    expect(runtime.projectionState.value).toEqual({
      count: 0,
    });
    expect(runtime.ready.value).toBe(false);
    expect(runtime.commands.refresh()).toBe("ok");

    runtime.dispose();
    runtime.dispose();

    expect(unsubscribe).toHaveBeenCalledTimes(1);
  });

  it("installs into a Vue app, exposes reactive state and commands, and limits selector churn to selected slices", async () => {
    Cadenza.createTask("Respond to wrapped vue inquiry", () => ({
      ok: true,
    })).respondsTo("wrapped-vue-runtime-intent");

    const runtime = createCadenzaVueRuntime<
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
            "wrapped-vue-runtime-intent",
            {},
            {
              overallTimeoutMs: 500,
            },
          );
        },
      }),
    });

    const app = createApp({});
    app.use(runtime);

    expect(app.config.globalProperties.$cadenzaRuntime).toBe(runtime);

    let runtimeFromComposable:
      | CadenzaVueRuntime<
          ProjectionState,
          {
            sendIntent: () => Promise<any>;
          }
        >
      | undefined;
    let snapshotRef:
      | ReturnType<typeof useCadenzaRuntimeState<ProjectionState>>
      | undefined;
    let projectionStateRef:
      | ReturnType<typeof useCadenzaProjectionState<ProjectionState>>
      | undefined;
    let readyRef: ReturnType<typeof useCadenzaRuntimeReady> | undefined;
    let maintenanceRef:
      | ReturnType<typeof useCadenzaProjectionSelector<ProjectionState, boolean>>
      | undefined;
    let alertCountRef:
      | ReturnType<typeof useCadenzaProjectionSelector<ProjectionState, number>>
      | undefined;
    const selectorUpdates = {
      maintenance: 0,
      alertCount: 0,
      ready: 0,
    };

    scope = effectScope();
    scope.run(() => {
      runInAppContext(app, () => {
        runtimeFromComposable = useCadenzaRuntime<
          CadenzaVueRuntime<
            ProjectionState,
            {
              sendIntent: () => Promise<any>;
            }
          >
        >();
        snapshotRef = useCadenzaRuntimeState<ProjectionState>();
        projectionStateRef = useCadenzaProjectionState<ProjectionState>();
        readyRef = useCadenzaRuntimeReady();
        maintenanceRef = useCadenzaProjectionSelector<ProjectionState, boolean>(
          (state) => state.flags.maintenanceMode,
        );
        alertCountRef = useCadenzaProjectionSelector<ProjectionState, number>(
          (state) => state.metrics.alertCount,
        );

        watch(maintenanceRef!, () => {
          selectorUpdates.maintenance += 1;
        });
        watch(alertCountRef!, () => {
          selectorUpdates.alertCount += 1;
        });
        watch(readyRef!, () => {
          selectorUpdates.ready += 1;
        });
      });
    });

    expect(runtimeFromComposable).toBe(runtime);
    expect(snapshotRef?.value).toEqual({
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
    expect(projectionStateRef?.value.metrics.alertCount).toBe(0);
    expect(readyRef?.value).toBe(false);

    Cadenza.emit("global.demo.alert.raised", {
      message: "Alert raised",
    });
    Cadenza.emit("global.demo.device.online", {
      deviceId: "device-7",
    });

    await waitForCondition(
      () =>
        snapshotRef?.value.projectionState.metrics.alertCount === 1 &&
        snapshotRef?.value.projectionState.devicesById["device-7"]?.online ===
          true &&
        snapshotRef?.value.projectionState.liveFeed[0] === "Alert raised",
    );
    expect(selectorUpdates.alertCount).toBe(1);
    expect(selectorUpdates.maintenance).toBe(0);

    Cadenza.emit("global.demo.maintenance.enabled", {});
    await waitForCondition(
      () => snapshotRef?.value.projectionState.flags.maintenanceMode === true,
    );
    expect(selectorUpdates.maintenance).toBe(1);
    expect(selectorUpdates.alertCount).toBe(1);

    Cadenza.emit("meta.service_registry.initial_sync_complete", {});
    await runtime.waitUntilReady();

    await waitForCondition(() => readyRef?.value === true);
    expect(selectorUpdates.ready).toBe(1);
    await expect(runtime.commands.sendIntent()).resolves.toMatchObject({
      ok: true,
    });
  });

  it("throws a clear error when composables are used without installing the runtime", () => {
    expect(() => {
      const orphanScope = effectScope();
      orphanScope.run(() => {
        useCadenzaRuntime();
      });
      orphanScope.stop();
    }).toThrow(/app\.use\(runtime\)/i);
  });

  it("keeps separate Vue apps isolated when they use different runtimes", async () => {
    const runtimeA = createCadenzaVueRuntime({
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
          signal: "global.demo.alpha",
          reduce: (current) => ({
            count: current.count + 1,
          }),
        },
      ],
    });
    const runtimeB = createCadenzaVueRuntime({
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
          signal: "global.demo.beta",
          reduce: (current) => ({
            count: current.count + 1,
          }),
        },
      ],
    });

    const appA = createApp({});
    const appB = createApp({});
    appA.use(runtimeA);
    appB.use(runtimeB);

    let countA:
      | ReturnType<typeof useCadenzaProjectionSelector<{ count: number }, number>>
      | undefined;
    let countB:
      | ReturnType<typeof useCadenzaProjectionSelector<{ count: number }, number>>
      | undefined;
    const isolationScope = effectScope();
    isolationScope.run(() => {
      runInAppContext(appA, () => {
        countA = useCadenzaProjectionSelector<{ count: number }, number>(
          (state) => state.count,
        );
      });
      runInAppContext(appB, () => {
        countB = useCadenzaProjectionSelector<{ count: number }, number>(
          (state) => state.count,
        );
      });
    });

    Cadenza.emit("global.demo.alpha", {});
    await waitForCondition(() => countA?.value === 1);
    expect(countB?.value).toBe(0);

    Cadenza.emit("global.demo.beta", {});
    await waitForCondition(() => countB?.value === 1);
    expect(countA?.value).toBe(1);

    isolationScope.stop();
    runtimeA.dispose();
    runtimeB.dispose();
  });
});
