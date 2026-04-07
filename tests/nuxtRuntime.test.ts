import { afterEach, beforeEach, describe, expect, it } from "vitest";

import Cadenza from "../src/Cadenza";
import type { CadenzaNuxtRuntime } from "../src/nuxt";
import {
  defineCadenzaNuxtRuntimePlugin,
  useCadenzaProjectionState,
  useCadenzaRuntime,
  useCadenzaRuntimeReady,
} from "../src/nuxt";

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

const nuxtState = new Map<string, { value: unknown }>();
let providedRuntime: CadenzaNuxtRuntime<any, any> | undefined;

function resetRuntimeState() {
  try {
    Cadenza.reset();
  } catch {
    // Ignore first-run reset errors before bootstrap.
  }

  nuxtState.clear();
  providedRuntime = undefined;
  delete (globalThis as Record<string, unknown>).__CADENZA_NUXT_RUNTIME_HOLDER__;
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

(globalThis as Record<string, unknown>).defineNuxtPlugin = (
  setup: () => { provide?: Record<string, unknown> } | void,
) => setup;
(globalThis as Record<string, unknown>).useState = <T>(
  key: string,
  init: () => T,
) => {
  if (!nuxtState.has(key)) {
    nuxtState.set(key, { value: init() });
  }

  return nuxtState.get(key) as { value: T };
};
(globalThis as Record<string, unknown>).useRuntimeConfig = () => ({
  public: {
    cadenzaBootstrapUrl: "http://cadenza-db.example:7000",
  },
});
(globalThis as Record<string, unknown>).useNuxtApp = () => ({
  $cadenzaRuntime: providedRuntime,
});
(globalThis as Record<string, unknown>).computed = <T>(getter: () => T) => ({
  get value() {
    return getter();
  },
});

describe("Nuxt browser runtime wrapper", () => {
  beforeEach(() => {
    resetRuntimeState();
  });

  afterEach(() => {
    resetRuntimeState();
  });

  it("creates one runtime and projects different signals into structured state slices", async () => {
    const plugin = defineCadenzaNuxtRuntimePlugin<
      ProjectionState,
      Record<string, never>
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
      bootstrapUrl: (config) => String(config.public.cadenzaBootstrapUrl),
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
    });

    const result = plugin();
    providedRuntime = result?.provide?.cadenzaRuntime as CadenzaNuxtRuntime<
      ProjectionState,
      Record<string, never>
    >;

    const projectionState = useCadenzaProjectionState<ProjectionState>();

    expect(projectionState.value).toEqual({
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

    Cadenza.emit("global.demo.maintenance.enabled", {});
    Cadenza.emit("global.demo.alert.raised", {
      message: "Alert raised",
    });
    Cadenza.emit("global.demo.device.online", {
      deviceId: "device-7",
    });

    await waitForCondition(
      () =>
        projectionState.value.projectionState.flags.maintenanceMode &&
        projectionState.value.projectionState.metrics.alertCount === 1 &&
        projectionState.value.projectionState.devicesById["device-7"]?.online ===
          true &&
        projectionState.value.projectionState.liveFeed[0] === "Alert raised",
    );
  });

  it("exposes runtime methods, commands, and ready state through composables", async () => {
    Cadenza.createTask("Respond to wrapped inquiry", () => ({
      ok: true,
    })).respondsTo("wrapped-runtime-intent");

    const plugin = defineCadenzaNuxtRuntimePlugin<
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
      commands: ({ cadenza, runtime }) => ({
        sendIntent: async () => {
          await runtime.waitUntilReady();
          return cadenza.inquire(
            "wrapped-runtime-intent",
            {},
            {
              overallTimeoutMs: 500,
            },
          );
        },
      }),
    });

    const result = plugin();
    providedRuntime = result?.provide?.cadenzaRuntime as CadenzaNuxtRuntime<
      ProjectionState,
      {
        sendIntent: () => Promise<any>;
      }
    >;

    const runtime = useCadenzaRuntime<
      CadenzaNuxtRuntime<
        ProjectionState,
        {
          sendIntent: () => Promise<any>;
        }
      >
    >();
    const readyState = useCadenzaRuntimeReady();

    expect(readyState.value).toBe(false);

    Cadenza.emit("meta.service_registry.initial_sync_complete", {});
    await runtime.waitUntilReady();

    expect(readyState.value).toBe(true);
    await expect(runtime.commands.sendIntent()).resolves.toMatchObject({
      ok: true,
    });
  });
});
