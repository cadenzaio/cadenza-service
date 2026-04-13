import { describe, expect, it } from "vitest";
import {
  sanitizeExecutionPersistenceContext,
  sanitizeExecutionPersistenceResultPayload,
  splitRoutinePersistenceContext,
} from "../src/utils/routinePersistence";

describe("routine persistence sanitization", () => {
  it("replaces bulky execution persistence fields with bounded summaries", () => {
    const sanitized = sanitizeExecutionPersistenceContext({
      __traceCreatedByRunner: true,
      rows: [{ id: 1 }, { id: 2 }],
      joinedContexts: [{ a: 1 }, { b: 2 }, { c: 3 }],
      serviceInstances: [{ uuid: "a" }],
      queryData: {
        data: { foo: "bar", count: 1 },
        filter: { uuid: "task-1" },
        onConflict: {
          target: ["uuid"],
        },
      },
      kept: "value",
    });

    expect(sanitized).toMatchObject({
      rowsCount: 2,
      joinedContextsCount: 3,
      serviceInstancesCount: 1,
      queryData: {
        dataKeys: ["count", "foo"],
        filterKeys: ["uuid"],
        onConflictTarget: ["uuid"],
      },
      kept: "value",
    });
    expect(sanitized).not.toHaveProperty("rows");
    expect(sanitized).not.toHaveProperty("joinedContexts");
    expect(sanitized).not.toHaveProperty("serviceInstances");
    expect(sanitized).not.toHaveProperty("__traceCreatedByRunner");
  });

  it("splits sanitized execution persistence context into business and meta fields", () => {
    const { context, metaContext } = splitRoutinePersistenceContext({
      foo: "bar",
      rows: [{ id: 1 }],
      __executionTraceId: "trace-1",
      __metadata: {
        nested: true,
      },
    });

    expect(context).toMatchObject({
      foo: "bar",
      rowsCount: 1,
    });
    expect(context).not.toHaveProperty("rows");
    expect(metaContext).toMatchObject({
      __executionTraceId: "trace-1",
      __metadata: {
        nested: true,
      },
    });
  });

  it("sanitizes ended result contexts before execution-persistence updates", () => {
    const sanitized = sanitizeExecutionPersistenceResultPayload({
      resultContext: {
        rows: [{ id: 1 }, { id: 2 }],
        joinedContexts: [{ a: 1 }],
        kept: "result",
      },
      metaResultContext: {
        serviceInstances: [{ uuid: "service-1" }],
        __executionTraceId: "trace-1",
      },
      result_context: {
        tasks: [{ name: "Insert task" }],
        queryData: {
          data: { uuid: "task-1" },
        },
      },
      meta_result_context: {
        actors: [{ name: "ActorA" }],
      },
    });

    expect(sanitized.resultContext).toMatchObject({
      rowsCount: 2,
      joinedContextsCount: 1,
      kept: "result",
    });
    expect(sanitized.resultContext).not.toHaveProperty("rows");
    expect(sanitized.metaResultContext).toMatchObject({
      serviceInstancesCount: 1,
      __executionTraceId: "trace-1",
    });
    expect(sanitized.result_context).toMatchObject({
      tasksCount: 1,
      queryData: {
        dataKeys: ["uuid"],
      },
    });
    expect(sanitized.meta_result_context).toMatchObject({
      actorsCount: 1,
    });
  });
});
