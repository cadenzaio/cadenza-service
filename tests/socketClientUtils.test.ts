import { EventEmitter } from "node:events";
import { describe, expect, it } from "vitest";
import { waitForSocketConnection } from "../src/network/socketClientUtils";

class MockSocket extends EventEmitter {
  connected: boolean = false;
}

describe("waitForSocketConnection", () => {
  it("resolves immediately when already connected", async () => {
    const socket = new MockSocket();
    socket.connected = true;

    const result = await waitForSocketConnection(
      socket,
      50,
      (reason) => `error:${reason}`,
    );

    expect(result).toEqual({ ok: true });
    expect(socket.listenerCount("connect")).toBe(0);
    expect(socket.listenerCount("connect_error")).toBe(0);
    expect(socket.listenerCount("disconnect")).toBe(0);
  });

  it("resolves with timeout error and cleans up listeners", async () => {
    const socket = new MockSocket();
    const result = await waitForSocketConnection(
      socket,
      5,
      (reason) => `error:${reason}`,
    );

    expect(result).toEqual({
      ok: false,
      error: "error:connect_timeout",
    });
    expect(socket.listenerCount("connect")).toBe(0);
    expect(socket.listenerCount("connect_error")).toBe(0);
    expect(socket.listenerCount("disconnect")).toBe(0);
  });

  it("resolves with connect error and cleans up listeners", async () => {
    const socket = new MockSocket();
    const pending = waitForSocketConnection(
      socket,
      50,
      (reason, error) =>
        `error:${reason}:${error instanceof Error ? error.message : String(error)}`,
    );

    socket.emit("connect_error", new Error("boom"));
    const result = await pending;

    expect(result).toEqual({
      ok: false,
      error: "error:connect_error:boom",
    });
    expect(socket.listenerCount("connect")).toBe(0);
    expect(socket.listenerCount("connect_error")).toBe(0);
    expect(socket.listenerCount("disconnect")).toBe(0);
  });
});
