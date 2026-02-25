import { describe, expect, it } from "vitest";
import { decomposeSignalName, formatTimestamp } from "../src/utils/tools";

describe("tools", () => {
  it("formats timestamps as ISO strings", () => {
    expect(formatTimestamp(0)).toBe("1970-01-01T00:00:00.000Z");
  });

  it("decomposes regular business signals", () => {
    expect(decomposeSignalName("orders.created")).toEqual({
      isMeta: false,
      isGlobal: false,
      domain: "orders",
      action: "created",
    });
  });

  it("decomposes global business signals", () => {
    expect(decomposeSignalName("global.orders.created")).toEqual({
      isMeta: false,
      isGlobal: true,
      domain: "orders",
      action: "created",
    });
  });

  it("decomposes global meta signals", () => {
    expect(decomposeSignalName("global.meta.service.updated")).toEqual({
      isMeta: true,
      isGlobal: true,
      domain: "service",
      action: "updated",
    });
  });

  it("decomposes sub_meta signals", () => {
    expect(decomposeSignalName("sub_meta.signal_broker.emitting_signal")).toEqual(
      {
        isMeta: true,
        isGlobal: false,
        domain: "signal_broker",
        action: "emitting_signal",
      },
    );
  });
});
