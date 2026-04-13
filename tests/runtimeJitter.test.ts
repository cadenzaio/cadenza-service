import { describe, expect, it } from "vitest";

import {
  buildDeterministicJitteredDelayMs,
  buildDeterministicJitterOffsetMs,
  hashKeyToUnitInterval,
  normalizeJitterRatio,
} from "../src/registry/runtimeJitter";

describe("runtimeJitter", () => {
  it("hashes keys deterministically into the unit interval", () => {
    const first = hashKeyToUnitInterval("service-a:instance-1:heartbeat");
    const second = hashKeyToUnitInterval("service-a:instance-1:heartbeat");

    expect(first).toBe(second);
    expect(first).toBeGreaterThanOrEqual(0);
    expect(first).toBeLessThanOrEqual(1);
  });

  it("clamps invalid jitter ratios", () => {
    expect(normalizeJitterRatio(-1)).toBe(0);
    expect(normalizeJitterRatio(Number.NaN)).toBe(0);
    expect(normalizeJitterRatio(0.25)).toBe(0.25);
    expect(normalizeJitterRatio(2)).toBe(1);
  });

  it("builds deterministic bounded offsets", () => {
    const offset = buildDeterministicJitterOffsetMs(
      10_000,
      0.2,
      "service-a:instance-1:heartbeat",
    );

    expect(offset).toBeGreaterThanOrEqual(0);
    expect(offset).toBeLessThanOrEqual(2_000);
    expect(offset).toBe(
      buildDeterministicJitterOffsetMs(
        10_000,
        0.2,
        "service-a:instance-1:heartbeat",
      ),
    );
  });

  it("adds jitter to delays without reducing the base delay", () => {
    const delay = buildDeterministicJitteredDelayMs(
      5_000,
      0.2,
      "service-a:instance-1:bootstrap",
    );

    expect(delay).toBeGreaterThanOrEqual(5_000);
    expect(delay).toBeLessThanOrEqual(6_000);
  });
});
