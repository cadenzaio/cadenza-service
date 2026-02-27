import { describe, expect, it } from "vitest";
import {
  evaluateDependencyReadiness,
  resolveServiceReadinessState,
  summarizeDependencyReadiness,
} from "../src/utils/readiness";

describe("readiness utils", () => {
  it("marks stale dependency as degraded before timeout threshold", () => {
    const evaluation = evaluateDependencyReadiness({
      exists: true,
      runtimeState: "healthy",
      acceptingWork: true,
      missedHeartbeats: 1,
      missThreshold: 3,
    });

    expect(evaluation).toMatchObject({
      state: "degraded",
      stale: true,
      blocked: false,
      reason: "heartbeat-stale",
    });
  });

  it("marks dependency unavailable when heartbeat timeout threshold is reached", () => {
    const evaluation = evaluateDependencyReadiness({
      exists: true,
      runtimeState: "healthy",
      acceptingWork: true,
      missedHeartbeats: 3,
      missThreshold: 3,
    });

    expect(evaluation).toMatchObject({
      state: "unavailable",
      stale: true,
      blocked: true,
      reason: "heartbeat-timeout",
    });
  });

  it("summarizes dependency state counters", () => {
    const summary = summarizeDependencyReadiness([
      evaluateDependencyReadiness({
        exists: true,
        runtimeState: "healthy",
        acceptingWork: true,
        missedHeartbeats: 0,
        missThreshold: 3,
      }),
      evaluateDependencyReadiness({
        exists: true,
        runtimeState: "overloaded",
        acceptingWork: true,
        missedHeartbeats: 0,
        missThreshold: 3,
      }),
      evaluateDependencyReadiness({
        exists: true,
        runtimeState: "healthy",
        acceptingWork: true,
        missedHeartbeats: 1,
        missThreshold: 3,
      }),
      evaluateDependencyReadiness({
        exists: true,
        runtimeState: "unavailable",
        acceptingWork: false,
        missedHeartbeats: 0,
        missThreshold: 3,
      }),
    ]);

    expect(summary).toEqual({
      total: 4,
      ready: 1,
      degraded: 1,
      overloaded: 1,
      unavailable: 1,
      stale: 1,
    });
  });

  it("resolves service readiness as blocked when dependencies are unavailable", () => {
    const state = resolveServiceReadinessState(
      "healthy",
      true,
      {
        total: 2,
        ready: 1,
        degraded: 0,
        overloaded: 0,
        unavailable: 1,
        stale: 1,
      },
    );

    expect(state).toBe("blocked");
  });

  it("resolves service readiness as degraded when dependencies are stale but available", () => {
    const state = resolveServiceReadinessState(
      "healthy",
      true,
      {
        total: 2,
        ready: 1,
        degraded: 1,
        overloaded: 0,
        unavailable: 0,
        stale: 1,
      },
    );

    expect(state).toBe("degraded");
  });
});
