import { afterEach, beforeEach, describe, expect, it } from "vitest";

import Cadenza from "../src/Cadenza";

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
    // Ignore first-run resets before bootstrap.
  }
}

describe("browser runtime actor", () => {
  beforeEach(() => {
    resetRuntimeState();
  });

  afterEach(() => {
    resetRuntimeState();
  });

  it("projects browser signals into actor runtime state and exposes subscription updates", async () => {
    const snapshots: Array<{
      ready: boolean;
      events: string[];
    }> = [];

    const handle = Cadenza.createBrowserRuntimeActor({
      actorName: "BrowserRuntimeActor",
      service: {
        name: "BrowserApp",
        description: "Frontend app",
        bootstrap: {
          url: "http://cadenza-db.example:7000",
        },
        useSocket: false,
        cadenzaDB: {
          connect: false,
        },
        customServiceId: "browser-runtime-actor-1",
      },
      initialProjectionState: {
        events: [] as string[],
      },
      signalBindings: [
        {
          signal: "global.browser.demo.event",
          reduce: (current, payload) => ({
            ...current,
            events: [...current.events, String(payload.id ?? "unknown")],
          }),
        },
      ],
    });

    const unsubscribe = handle.subscribe((state) => {
      snapshots.push({
        ready: state.ready,
        events: [...state.projectionState.events],
      });
    });

    expect(handle.getRuntimeState()).toEqual({
      ready: false,
      projectionState: {
        events: [],
      },
      lastReadyAt: null,
      lastSyncRequestedAt: null,
    });

    Cadenza.emit("global.browser.demo.event", {
      id: "evt-1",
    });

    await waitForCondition(
      () => handle.getRuntimeState().projectionState.events.length === 1,
    );

    expect(handle.getRuntimeState().projectionState.events).toEqual(["evt-1"]);

    Cadenza.emit("meta.fetch.handshake_complete", {
      source: "browser-runtime-test",
    });

    await waitForCondition(
      () => handle.getRuntimeState().lastSyncRequestedAt !== null,
    );

    Cadenza.emit("meta.service_registry.initial_sync_complete", {});
    await handle.waitUntilReady();

    expect(handle.getRuntimeState().ready).toBe(true);
    expect(Cadenza.serviceRegistry.isFrontend).toBe(true);
    expect(Cadenza.serviceRegistry.serviceName).toBe("BrowserApp");
    expect(
      snapshots.some((snapshot) =>
        snapshot.events.includes("evt-1") && snapshot.ready === false
      ),
    ).toBe(true);
    expect(snapshots.at(-1)?.ready).toBe(true);

    unsubscribe();
  });

  it("reuses the same actor and generated tasks across repeated initialization", () => {
    const options = {
      actorName: "BrowserRuntimeActor",
      service: {
        name: "BrowserApp",
        description: "Frontend app",
        bootstrap: {
          url: "http://cadenza-db.example:7000",
        },
        useSocket: false,
        cadenzaDB: {
          connect: false,
        },
      },
      initialProjectionState: {
        events: [] as string[],
      },
      signalBindings: [
        {
          signal: "global.browser.demo.event",
          reduce: (current: { events: string[] }, payload: Record<string, any>) => ({
            ...current,
            events: [...current.events, String(payload.id ?? "unknown")],
          }),
        },
      ],
    };

    const first = Cadenza.createBrowserRuntimeActor(options);
    const second = Cadenza.createBrowserRuntimeActor(options);

    expect(first.actor).toBe(second.actor);
    expect(Cadenza.get("BrowserRuntimeActor.MarkReady")).toBeDefined();
    expect(Cadenza.get("BrowserRuntimeActor.RequestInitialSync")).toBeDefined();
    expect(
      Cadenza.get("BrowserRuntimeActor.Project global.browser.demo.event"),
    ).toBeDefined();
  });

  it("waits for readiness before forwarding inquiries", async () => {
    Cadenza.createTask("Respond to browser inquiry", () => {
      return {
        ok: true,
      };
    }).respondsTo("browser-runtime-intent");

    const handle = Cadenza.createBrowserRuntimeActor({
      actorName: "BrowserRuntimeActor",
      service: {
        name: "BrowserApp",
        description: "Frontend app",
        bootstrap: {
          url: "http://cadenza-db.example:7000",
        },
        useSocket: false,
        cadenzaDB: {
          connect: false,
        },
      },
      initialProjectionState: {
        events: [] as string[],
      },
    });

    let settled = false;
    const inquiryPromise = handle
      .inquire("browser-runtime-intent", {}, { overallTimeoutMs: 500 })
      .then((result) => {
        settled = true;
        return result;
      });

    await new Promise((resolve) => setTimeout(resolve, 25));
    expect(settled).toBe(false);

    Cadenza.emit("meta.service_registry.initial_sync_complete", {});

    await expect(inquiryPromise).resolves.toMatchObject({
      ok: true,
    });
  });
});
