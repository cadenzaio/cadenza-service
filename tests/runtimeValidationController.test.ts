import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import Cadenza from "../src/Cadenza";
import DatabaseController from "@service-database-controller";
import GraphMetadataController from "../src/graph/controllers/GraphMetadataController";
import GraphSyncController from "../src/graph/controllers/GraphSyncController";
import RestController from "../src/network/RestController";
import ServiceRegistry from "../src/registry/ServiceRegistry";
import SignalController from "../src/signals/SignalController";
import SocketController from "../src/network/SocketController";
import RuntimeValidationController, {
  RUNTIME_VALIDATION_INTENTS,
  RUNTIME_VALIDATION_SIGNALS,
} from "../src/runtime/RuntimeValidationController";

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

  (DatabaseController as any)._instance = undefined;
  (GraphMetadataController as any)._instance = undefined;
  (GraphSyncController as any)._instance = undefined;
  (RestController as any)._instance = undefined;
  (RuntimeValidationController as any)._instance = undefined;
  (ServiceRegistry as any)._instance = undefined;
  (SignalController as any)._instance = undefined;
  (SocketController as any)._instance = undefined;
  delete (globalThis as any).__CADENZA_RUNTIME__;
}

describe("Runtime validation controller", () => {
  let consoleLogSpy: ReturnType<typeof vi.spyOn>;
  let consoleWarnSpy: ReturnType<typeof vi.spyOn>;
  let consoleErrorSpy: ReturnType<typeof vi.spyOn>;

  beforeEach(() => {
    consoleLogSpy = vi.spyOn(console, "log").mockImplementation(() => {});
    consoleWarnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
    consoleErrorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    resetRuntimeState();
  });

  afterEach(() => {
    resetRuntimeState();
    consoleLogSpy.mockRestore();
    consoleWarnSpy.mockRestore();
    consoleErrorSpy.mockRestore();
    vi.restoreAllMocks();
  });

  it("updates runtime validation policy through meta inquiry intents", async () => {
    const setResult = await Cadenza.inquire(
      RUNTIME_VALIDATION_INTENTS.setPolicy,
      {
        policy: {
          businessInput: "enforce",
          warnOnMissingBusinessInputSchema: true,
        },
      },
      { requireComplete: true },
    );

    expect(setResult.policy).toMatchObject({
      businessInput: "enforce",
      warnOnMissingBusinessInputSchema: true,
    });

    const getResult = await Cadenza.inquire(
      RUNTIME_VALIDATION_INTENTS.getPolicy,
      {},
      { requireComplete: true },
    );

    expect(getResult.policy).toMatchObject({
      businessInput: "enforce",
      warnOnMissingBusinessInputSchema: true,
    });
  });

  it("updates runtime validation policy through meta signals", async () => {
    const updates: any[] = [];

    Cadenza.createMetaTask("Capture runtime validation update", (ctx) => {
      updates.push(ctx);
      return true;
    }).doOn(RUNTIME_VALIDATION_SIGNALS.policyUpdated);

    Cadenza.emit(RUNTIME_VALIDATION_SIGNALS.setPolicyRequested, {
      policy: {
        metaInput: "warn",
      },
    });

    await waitForCondition(() => updates.length === 1);

    expect(updates[0]?.policy).toMatchObject({
      metaInput: "warn",
    });
    expect(Cadenza.getRuntimeValidationPolicy()).toMatchObject({
      metaInput: "warn",
    });
  });

  it("manages runtime validation scopes at runtime", async () => {
    const scopeId = "debug-predictor-flow";

    const upsertResult = await Cadenza.inquire(
      RUNTIME_VALIDATION_INTENTS.upsertScope,
      {
        scope: {
          id: scopeId,
          startTaskNames: ["Predict health metric"],
          policy: {
            businessInput: "enforce",
          },
        },
      },
      { requireComplete: true },
    );

    expect(upsertResult.scope).toMatchObject({
      id: scopeId,
      startTaskNames: ["Predict health metric"],
      policy: {
        businessInput: "enforce",
      },
    });

    const listResult = await Cadenza.inquire(
      RUNTIME_VALIDATION_INTENTS.listScopes,
      {},
      { requireComplete: true },
    );

    expect(listResult.scopes).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          id: scopeId,
        }),
      ]),
    );

    const removeResult = await Cadenza.inquire(
      RUNTIME_VALIDATION_INTENTS.removeScope,
      { id: scopeId },
      { requireComplete: true },
    );

    expect(removeResult.scopes).toEqual(
      expect.not.arrayContaining([
        expect.objectContaining({
          id: scopeId,
        }),
      ]),
    );
  });
});
