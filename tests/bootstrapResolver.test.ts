import { afterEach, describe, expect, it } from "vitest";

import { resolveBootstrapEndpoint } from "../src/utils/bootstrap";

const ORIGINAL_ENV = { ...process.env };

afterEach(() => {
  process.env = { ...ORIGINAL_ENV };
  delete (globalThis as any).__CADENZA_RUNTIME__;
});

describe("bootstrap endpoint resolution", () => {
  it("resolves CADENZA_DB_ADDRESS with an embedded port", () => {
    process.env.CADENZA_DB_ADDRESS = "https://cadenza.example:9443";
    delete process.env.CADENZA_DB_PORT;

    const resolved = resolveBootstrapEndpoint({
      runtime: "server",
    });

    expect(resolved).toEqual(
      expect.objectContaining({
        url: "https://cadenza.example:9443",
        protocol: "https",
        address: "cadenza.example",
        port: 9443,
        exposed: true,
      }),
    );
  });

  it("resolves a bare CADENZA_DB_ADDRESS together with CADENZA_DB_PORT", () => {
    process.env.CADENZA_DB_ADDRESS = "localhost";
    process.env.CADENZA_DB_PORT = "5000";

    const resolved = resolveBootstrapEndpoint({
      runtime: "server",
    });

    expect(resolved).toEqual(
      expect.objectContaining({
        url: "http://localhost:5000",
        protocol: "http",
        address: "localhost",
        port: 5000,
        exposed: false,
      }),
    );
  });

  it("rejects a bare bootstrap address without a port", () => {
    process.env.CADENZA_DB_ADDRESS = "localhost";
    delete process.env.CADENZA_DB_PORT;

    expect(() =>
      resolveBootstrapEndpoint({
        runtime: "server",
      }),
    ).toThrow(/port/i);
  });

  it("prefers explicit bootstrap url over injected browser config", () => {
    (globalThis as any).__CADENZA_RUNTIME__ = {
      bootstrapUrl: "http://injected.example:7000",
    };

    const resolved = resolveBootstrapEndpoint({
      runtime: "browser",
      bootstrap: {
        url: "https://explicit.example:9000",
      },
    });

    expect(resolved).toEqual(
      expect.objectContaining({
        url: "https://explicit.example:9000",
        address: "explicit.example",
        port: 9000,
      }),
    );
  });
});
