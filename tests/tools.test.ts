import { describe, expect, it } from "vitest";
import { decomposeSignalName, formatTimestamp } from "../src/utils/tools";
import { createGlobal, createHelper, createTask } from "../src/index";
import Cadenza from "../src/Cadenza";

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

  it("re-exports helper and global authoring APIs from the service surface", () => {
    Cadenza.reset();

    const config = createGlobal("Service surface tool config", {
      prefix: "svc",
    });
    const normalize = createHelper("Service surface normalizer", (context) => ({
      ...context,
      value: String(context.value ?? "").trim(),
    }));

    const task = createTask(
      "Service surface tool task",
      (_context, _emit, _inquire, tools) => {
        const normalized = tools.helpers.normalize({
          value: "  ok  ",
        }) as { value: string };
        return {
          label: `${(tools.globals.config as { prefix: string }).prefix}:${normalized.value}`,
        };
      },
    )
      .usesHelpers({
        normalize,
      })
      .usesGlobals({
        config,
      });

    expect(task).toBeDefined();
  });
});
