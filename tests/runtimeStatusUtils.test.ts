import { describe, expect, it } from "vitest";
import {
  hasSignificantRuntimeStatusChange,
  resolveRuntimeStatus,
  runtimeStatusPriority,
} from "../src/utils/runtimeStatus";

describe("runtime status utils", () => {
  it("marks inactive or blocked instances as unavailable", () => {
    const status = resolveRuntimeStatus({
      numberOfRunningGraphs: 1,
      isActive: false,
      isNonResponsive: false,
      isBlocked: false,
      degradedGraphThreshold: 10,
      overloadedGraphThreshold: 20,
    });

    expect(status).toMatchObject({
      state: "unavailable",
      acceptingWork: false,
    });
  });

  it("marks high load as overloaded while still accepting work", () => {
    const status = resolveRuntimeStatus({
      numberOfRunningGraphs: 22,
      isActive: true,
      isNonResponsive: false,
      isBlocked: false,
      degradedGraphThreshold: 10,
      overloadedGraphThreshold: 20,
    });

    expect(status).toMatchObject({
      state: "overloaded",
      acceptingWork: true,
    });
  });

  it("detects significant status changes from healthy to overloaded", () => {
    const previous = resolveRuntimeStatus({
      numberOfRunningGraphs: 2,
      isActive: true,
      isNonResponsive: false,
      isBlocked: false,
      degradedGraphThreshold: 10,
      overloadedGraphThreshold: 20,
    });
    const next = resolveRuntimeStatus({
      numberOfRunningGraphs: 21,
      isActive: true,
      isNonResponsive: false,
      isBlocked: false,
      degradedGraphThreshold: 10,
      overloadedGraphThreshold: 20,
    });

    expect(hasSignificantRuntimeStatusChange(previous, next)).toBe(true);
  });

  it("does not treat pure count change in same state as significant", () => {
    const previous = resolveRuntimeStatus({
      numberOfRunningGraphs: 2,
      isActive: true,
      isNonResponsive: false,
      isBlocked: false,
      degradedGraphThreshold: 10,
      overloadedGraphThreshold: 20,
    });
    const next = resolveRuntimeStatus({
      numberOfRunningGraphs: 3,
      isActive: true,
      isNonResponsive: false,
      isBlocked: false,
      degradedGraphThreshold: 10,
      overloadedGraphThreshold: 20,
    });

    expect(hasSignificantRuntimeStatusChange(previous, next)).toBe(false);
  });

  it("orders statuses by balancing preference", () => {
    expect(runtimeStatusPriority("healthy")).toBeLessThan(
      runtimeStatusPriority("degraded"),
    );
    expect(runtimeStatusPriority("degraded")).toBeLessThan(
      runtimeStatusPriority("overloaded"),
    );
    expect(runtimeStatusPriority("overloaded")).toBeLessThan(
      runtimeStatusPriority("unavailable"),
    );
  });
});
