import { describe, expect, it } from "vitest";

import {
  buildAuthorityRuntimeStatusSignature,
  type AuthorityRuntimeStatusReport,
} from "../src/registry/runtimeStatusContract";

function buildBaseReport(): AuthorityRuntimeStatusReport {
  return {
    serviceName: "OrdersService",
    serviceInstanceId: "orders-1",
    transportId: "transport-1",
    transportRole: "internal",
    transportOrigin: "http://orders:3000",
    transportProtocols: ["rest"],
    isFrontend: false,
    reportedAt: "2026-04-12T18:00:00.000Z",
    state: "healthy",
    acceptingWork: true,
    numberOfRunningGraphs: 4,
    cpuUsage: 0.2,
    memoryUsage: 0.3,
    eventLoopLag: 12,
    isActive: true,
    isNonResponsive: false,
    isBlocked: false,
    health: {
      runtimeMetrics: {
        sampledAt: "2026-04-12T18:00:00.000Z",
        rssBytes: 2_000,
        heapUsedBytes: 1_000,
      },
    },
  };
}

describe("runtime status contract", () => {
  it("ignores volatile runtime metrics when coalescing authority lease reports", () => {
    const base = buildBaseReport();

    const baseline = buildAuthorityRuntimeStatusSignature(base);
    const withMetricDelta = buildAuthorityRuntimeStatusSignature({
      ...base,
      numberOfRunningGraphs: 99,
      cpuUsage: 0.91,
      memoryUsage: 0.82,
      eventLoopLag: 48,
      health: {
        runtimeMetrics: {
          sampledAt: "2026-04-12T18:00:30.000Z",
          rssBytes: 4_000,
          heapUsedBytes: 2_200,
        },
      },
    });

    expect(withMetricDelta).toBe(baseline);
  });

  it("changes the authority signature when lifecycle-relevant fields change", () => {
    const base = buildBaseReport();

    expect(
      buildAuthorityRuntimeStatusSignature({
        ...base,
        acceptingWork: false,
      }),
    ).not.toBe(buildAuthorityRuntimeStatusSignature(base));

    expect(
      buildAuthorityRuntimeStatusSignature({
        ...base,
        state: "overloaded",
      }),
    ).not.toBe(buildAuthorityRuntimeStatusSignature(base));
  });
});
