import { describe, expect, it, vi } from "vitest";
import { SignalBroker } from "@cadenza.io/core";

describe("signal tag matching", () => {
  it("does not match tagged signals against untagged listeners", () => {
    const broker = new SignalBroker();
    const run = vi.fn();
    const runner = { run } as any;
    broker.bootstrap(runner, runner);

    const exactTask = { id: "exact" } as any;
    const wildcardTask = { id: "wildcard" } as any;
    broker.observe("meta.fetch.handshake_failed", exactTask);
    broker.observe("meta.fetch.handshake_failed.*", wildcardTask);

    broker.emit("meta.fetch.handshake_failed:instance-1", {});

    expect(run).toHaveBeenCalledTimes(1);
    const [tasks] = run.mock.calls[0];
    expect(tasks).toHaveLength(1);
    expect(tasks[0]).toBe(wildcardTask);
  });
});
