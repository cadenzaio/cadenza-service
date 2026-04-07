import { describe, expect, it } from "vitest";

import {
  buildTransportHandleKey,
  parseTransportHandleKey,
} from "../src/utils/transport";

describe("transport handle keys", () => {
  it("round-trips the logical route key for rest handles", () => {
    const routeKey = "CadenzaDB|internal|http://cadenza-db-service:8080";
    const handleKey = buildTransportHandleKey(routeKey, "rest");

    expect(parseTransportHandleKey(handleKey)).toEqual({
      routeKey,
      protocol: "rest",
    });
  });

  it("round-trips the logical route key for socket handles", () => {
    const routeKey =
      "ScheduledRunnerService|internal|http://scheduled-runner:3002";
    const handleKey = buildTransportHandleKey(routeKey, "socket");

    expect(parseTransportHandleKey(handleKey)).toEqual({
      routeKey,
      protocol: "socket",
    });
  });
});
