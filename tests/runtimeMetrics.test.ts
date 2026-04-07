import { describe, expect, it } from "vitest";

import { RuntimeMetricsSampler } from "../src/utils/runtimeMetrics";

describe("runtime metrics sampler", () => {
  it("computes normalized CPU, memory, and event-loop lag snapshots", () => {
    const nowMs = [1_000, 6_000];
    const cpuUsage = [
      { user: 0, system: 0 },
      { user: 2_500_000, system: 500_000 },
    ];
    let percentileCalls = 0;

    const sampler = new RuntimeMetricsSampler({
      nowMs: () => nowMs.shift() ?? 6_000,
      cpuUsage: () => cpuUsage.shift() ?? { user: 2_500_000, system: 500_000 },
      memoryUsage: () =>
        ({
          rss: 200,
          heapUsed: 120,
          heapTotal: 160,
          external: 0,
          arrayBuffers: 0,
        }) as NodeJS.MemoryUsage,
      effectiveCpuCapacity: () => 2,
      effectiveMemoryLimitBytes: () => 400,
      createEventLoopMonitor: () => ({
        enable() {},
        disable() {},
        percentile() {
          percentileCalls += 1;
          return 12_340_000;
        },
      }),
    });

    expect(sampler.sample()).toMatchObject({
      cpuUsage: null,
      memoryUsage: 0.5,
      eventLoopLag: 12.34,
      rssBytes: 200,
      heapUsedBytes: 120,
      heapTotalBytes: 160,
      memoryLimitBytes: 400,
    });

    expect(sampler.sample()).toMatchObject({
      cpuUsage: 0.3,
      memoryUsage: 0.5,
      eventLoopLag: 12.34,
      rssBytes: 200,
      heapUsedBytes: 120,
      heapTotalBytes: 160,
      memoryLimitBytes: 400,
    });
    expect(percentileCalls).toBeGreaterThan(0);
  });
});
