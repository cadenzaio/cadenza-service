import { describe, expect, it } from "vitest";
import {
  compareResponderDescriptors,
  isMetaIntentName,
  mergeInquiryContexts,
  shouldExecuteInquiryResponder,
  summarizeResponderStatuses,
} from "../src/utils/inquiry";

describe("inquiry utils", () => {
  it("merges inquiry contexts deterministically with deep object merge", () => {
    const merged = mergeInquiryContexts([
      {
        alpha: 1,
        nested: {
          a: true,
          list: [1],
        },
      },
      {
        beta: 2,
        nested: {
          b: true,
          list: [2, 3],
        },
      },
    ]);

    expect(merged).toEqual({
      alpha: 1,
      beta: 2,
      nested: {
        a: true,
        b: true,
        list: [1, 2, 3],
      },
    });
  });

  it("sorts responders by service/task/version/localTaskName", () => {
    const responders = [
      {
        isRemote: true,
        serviceName: "BillingService",
        taskName: "Fetch invoices",
        taskVersion: 2,
        localTaskName: "A",
      },
      {
        isRemote: false,
        serviceName: "AccountService",
        taskName: "Fetch profile",
        taskVersion: 1,
        localTaskName: "B",
      },
      {
        isRemote: true,
        serviceName: "BillingService",
        taskName: "Fetch invoices",
        taskVersion: 1,
        localTaskName: "C",
      },
    ];

    responders.sort(compareResponderDescriptors);

    expect(responders.map((r) => r.localTaskName)).toEqual(["B", "C", "A"]);
  });

  it("summarizes responder statuses", () => {
    const summary = summarizeResponderStatuses([
      {
        isRemote: false,
        serviceName: "A",
        taskName: "t1",
        taskVersion: 1,
        localTaskName: "t1",
        status: "fulfilled",
        durationMs: 10,
      },
      {
        isRemote: true,
        serviceName: "B",
        taskName: "t2",
        taskVersion: 1,
        localTaskName: "t2",
        status: "failed",
        durationMs: 20,
      },
      {
        isRemote: true,
        serviceName: "C",
        taskName: "t3",
        taskVersion: 1,
        localTaskName: "t3",
        status: "timed_out",
        durationMs: 30,
      },
    ]);

    expect(summary).toEqual({
      responded: 1,
      failed: 1,
      timedOut: 1,
      pending: 0,
    });
  });

  it("detects meta intent names via dash prefix", () => {
    expect(isMetaIntentName("meta-runtime-transport-diagnostics")).toBe(true);
    expect(isMetaIntentName("query-cadenza-db-service")).toBe(false);
  });

  it("executes only meta responders for meta intents", () => {
    expect(
      shouldExecuteInquiryResponder(
        "meta-runtime-transport-diagnostics",
        true,
      ),
    ).toBe(true);
    expect(
      shouldExecuteInquiryResponder(
        "meta-runtime-transport-diagnostics",
        false,
      ),
    ).toBe(false);
    expect(shouldExecuteInquiryResponder("domain-customer-profile", true)).toBe(
      true,
    );
    expect(
      shouldExecuteInquiryResponder("domain-customer-profile", false),
    ).toBe(true);
  });
});
