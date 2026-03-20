import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import Cadenza from "../src/Cadenza";
import RestControllerBrowser from "../src/network/RestController.browser";

function resetRuntimeState() {
  try {
    Cadenza.reset();
  } catch {
    // Ignore first-run resets before bootstrap.
  }

  (RestControllerBrowser as any)._instance = undefined;
}

describe("RestController browser registration", () => {
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
    RestControllerBrowser.instance;
  });

  afterEach(() => {
    resetRuntimeState();
    consoleLogSpy.mockRestore();
    consoleWarnSpy.mockRestore();
    consoleErrorSpy.mockRestore();
  });

  it("preserves frontend instance payload when declaring browser network", async () => {
    const registrationPromise = new Promise<any>((resolve) => {
      Cadenza.createEphemeralMetaTask(
        "Capture browser instance registration",
        (ctx) => {
          resolve(ctx);
          return true;
        },
        "Captures browser instance registration events during tests.",
        { register: false },
      ).doOn("meta.service_registry.instance_registration_requested");
    });

    Cadenza.emit("meta.service_registry.service_inserted", {
      data: {
        name: "DemoFrontend",
        description: "Nuxt dashboard runtime for the Cadenza Demo 2 system.",
        displayName: "",
        isMeta: false,
      },
      __serviceName: "DemoFrontend",
      __serviceInstanceId: "demo-frontend-1",
      __isFrontend: true,
      __declaredTransports: [
        {
          uuid: "demo-frontend-public",
          role: "public",
          origin: "http://frontend.localhost",
          protocols: ["rest"],
        },
      ],
    });

    const registrationContext = await registrationPromise;

    expect(registrationContext.data).toEqual(
      expect.objectContaining({
        uuid: "demo-frontend-1",
        service_name: "DemoFrontend",
        is_frontend: true,
      }),
    );
    expect(registrationContext.data.name).toBeUndefined();
    expect(registrationContext.__registrationData).toEqual(
      expect.objectContaining({
        uuid: "demo-frontend-1",
        service_name: "DemoFrontend",
      }),
    );
    expect(registrationContext.__transportData).toEqual([
      expect.objectContaining({
        uuid: "demo-frontend-public",
        service_instance_id: "demo-frontend-1",
        role: "public",
        origin: "http://frontend.localhost",
        protocols: ["rest"],
      }),
    ]);
  });
});
