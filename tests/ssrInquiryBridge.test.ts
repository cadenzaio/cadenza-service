import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { createSSRInquiryBridge } from "../src/ssr/createSSRInquiryBridge";

describe("SSR inquiry bridge", () => {
  const originalFetch = globalThis.fetch;

  beforeEach(() => {
    vi.restoreAllMocks();
  });

  afterEach(() => {
    globalThis.fetch = originalFetch;
    delete process.env.CADENZA_DB_ADDRESS;
    delete process.env.CADENZA_DB_PORT;
  });

  it("executes request-scoped inquiries and dehydrates initial results", async () => {
    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      const body = init?.body ? JSON.parse(String(init.body)) : {};
      const routineName = body.__remoteRoutineName;

      if (url === "http://cadenza-db:5000/delegation") {
        if (routineName === "Query intent_to_task_map") {
          return {
            json: async () => ({
              rows: [
                {
                  intent_name: "orders.lookup",
                  service_name: "OrdersService",
                  task_name: "LookupOrders",
                  task_version: 1,
                  deleted: false,
                },
              ],
            }),
          } as Response;
        }

        if (routineName === "Query service_instance") {
          return {
            json: async () => ({
              rows: [
                {
                  uuid: "orders-1",
                  service_name: "OrdersService",
                  is_active: true,
                  is_non_responsive: false,
                  is_blocked: false,
                  is_primary: true,
                },
              ],
            }),
          } as Response;
        }

        if (routineName === "Query service_instance_transport") {
          return {
            json: async () => ({
              rows: [
                {
                  uuid: "orders-internal-1",
                  service_instance_id: "orders-1",
                  role: "internal",
                  origin: "http://orders.example:7000",
                  protocols: ["rest", "socket"],
                  deleted: false,
                },
              ],
            }),
          } as Response;
        }
      }

      if (url === "http://orders.example:7000/delegation") {
        expect(routineName).toBe("LookupOrders");
        return {
          json: async () => ({
            orders: [{ id: "order-1" }],
          }),
        } as Response;
      }

      throw new Error(`Unexpected fetch call: ${url}`);
    });

    globalThis.fetch = fetchMock as typeof fetch;

    const bridge = createSSRInquiryBridge({
      bootstrap: {
        url: "http://cadenza-db:5000",
      },
    });

    const result = await bridge.inquire(
      "orders.lookup",
      {
        accountId: "acct-1",
      },
      {
        hydrationKey: "orders.lookup.initial",
      },
    );

    expect(result.orders).toEqual([{ id: "order-1" }]);
    expect(result.__inquiryMeta).toEqual(
      expect.objectContaining({
        inquiry: "orders.lookup",
        responded: 1,
        failed: 0,
      }),
    );

    expect(bridge.dehydrate()).toEqual({
      initialInquiryResults: {
        "orders.lookup.initial": expect.objectContaining({
          orders: [{ id: "order-1" }],
        }),
      },
    });

    expect(fetchMock).toHaveBeenCalledTimes(4);
  });

  it("accepts camel-cased plural query payloads from generated database tasks", async () => {
    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      const body = init?.body ? JSON.parse(String(init.body)) : {};
      const routineName = body.__remoteRoutineName;

      if (url === "http://cadenza-db:5000/delegation") {
        if (routineName === "Query intent_to_task_map") {
          return {
            json: async () => ({
              intentToTaskMaps: [
                {
                  intent_name: "orders.lookup",
                  service_name: "OrdersService",
                  task_name: "LookupOrders",
                  task_version: 1,
                  deleted: false,
                },
              ],
            }),
          } as Response;
        }

        if (routineName === "Query service_instance") {
          return {
            json: async () => ({
              serviceInstances: [
                {
                  uuid: "orders-1",
                  service_name: "OrdersService",
                  is_active: true,
                  is_non_responsive: false,
                  is_blocked: false,
                  is_primary: true,
                },
              ],
            }),
          } as Response;
        }

        if (routineName === "Query service_instance_transport") {
          return {
            json: async () => ({
              serviceInstanceTransports: [
                {
                  uuid: "orders-internal-1",
                  service_instance_id: "orders-1",
                  role: "internal",
                  origin: "http://orders.example:7000",
                  protocols: ["rest", "socket"],
                  deleted: false,
                },
              ],
            }),
          } as Response;
        }
      }

      if (url === "http://orders.example:7000/delegation") {
        expect(routineName).toBe("LookupOrders");
        return {
          json: async () => ({
            orders: [{ id: "order-1" }],
          }),
        } as Response;
      }

      throw new Error(`Unexpected fetch call: ${url}`);
    });

    globalThis.fetch = fetchMock as typeof fetch;

    const bridge = createSSRInquiryBridge({
      bootstrap: {
        url: "http://cadenza-db:5000",
      },
    });

    const result = await bridge.inquire("orders.lookup", {}, {});

    expect(result.orders).toEqual([{ id: "order-1" }]);
    expect(result.__inquiryMeta).toEqual(
      expect.objectContaining({
        inquiry: "orders.lookup",
        responded: 1,
        failed: 0,
      }),
    );
  });

  it("unwraps delegated query payloads nested under joined contexts", async () => {
    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      const body = init?.body ? JSON.parse(String(init.body)) : {};
      const routineName = body.__remoteRoutineName;

      if (url === "http://cadenza-db:5000/delegation") {
        if (routineName === "Query intent_to_task_map") {
          return {
            json: async () => ({
              rows: [
                {
                  intent_name: "orders.lookup",
                  service_name: "OrdersService",
                  task_name: "LookupOrders",
                  task_version: 1,
                  deleted: false,
                },
              ],
            }),
          } as Response;
        }

        if (routineName === "Query service_instance") {
          return {
            json: async () => ({
              joinedContexts: [
                {
                  serviceInstances: [
                    {
                      uuid: "orders-1",
                      service_name: "OrdersService",
                      is_active: true,
                      is_non_responsive: false,
                      is_blocked: false,
                      is_primary: true,
                    },
                  ],
                },
              ],
            }),
          } as Response;
        }

        if (routineName === "Query service_instance_transport") {
          return {
            json: async () => ({
              joinedContexts: [
                {
                  serviceInstanceTransports: [
                    {
                      uuid: "orders-internal-1",
                      service_instance_id: "orders-1",
                      role: "internal",
                      origin: "http://orders.example:7000",
                      protocols: ["rest"],
                      deleted: false,
                    },
                  ],
                },
              ],
            }),
          } as Response;
        }
      }

      if (url === "http://orders.example:7000/delegation") {
        expect(routineName).toBe("LookupOrders");
        return {
          json: async () => ({
            orders: [{ id: "order-1" }],
          }),
        } as Response;
      }

      throw new Error(`Unexpected fetch call: ${url}`);
    });

    globalThis.fetch = fetchMock as typeof fetch;

    const bridge = createSSRInquiryBridge({
      bootstrap: {
        url: "http://cadenza-db:5000",
      },
    });

    const result = await bridge.inquire("orders.lookup");

    expect(result.orders).toEqual([{ id: "order-1" }]);
    expect(result.__inquiryMeta).toEqual(
      expect.objectContaining({
        inquiry: "orders.lookup",
        responded: 1,
        failed: 0,
      }),
    );
  });

  it("skips stale active instances that lack an internal transport", async () => {
    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      const body = init?.body ? JSON.parse(String(init.body)) : {};
      const routineName = body.__remoteRoutineName;

      if (url === "http://cadenza-db:5000/delegation") {
        if (routineName === "Query intent_to_task_map") {
          return {
            json: async () => ({
              rows: [
                {
                  intent_name: "devices.count",
                  service_name: "IotDbService",
                  task_name: "CountDevices",
                  task_version: 1,
                  deleted: false,
                },
              ],
            }),
          } as Response;
        }

        if (routineName === "Query service_instance") {
          return {
            json: async () => ({
              rows: [
                {
                  uuid: "iot-stale",
                  service_name: "IotDbService",
                  is_active: true,
                  is_non_responsive: false,
                  is_blocked: false,
                  is_primary: true,
                },
                {
                  uuid: "iot-fresh",
                  service_name: "IotDbService",
                  is_active: true,
                  is_non_responsive: false,
                  is_blocked: false,
                  is_primary: true,
                },
              ],
            }),
          } as Response;
        }

        if (routineName === "Query service_instance_transport") {
          return {
            json: async () => ({
              rows: [
                {
                  uuid: "iot-stale-public",
                  service_instance_id: "iot-stale",
                  role: "public",
                  origin: "http://iot-db.localhost",
                  protocols: ["rest"],
                  deleted: false,
                },
                {
                  uuid: "iot-fresh-internal",
                  service_instance_id: "iot-fresh",
                  role: "internal",
                  origin: "http://iot-db-service:3001",
                  protocols: ["rest", "socket"],
                  deleted: false,
                },
              ],
            }),
          } as Response;
        }
      }

      if (url === "http://iot-db-service:3001/delegation") {
        expect(routineName).toBe("CountDevices");
        return {
          json: async () => ({
            count: 924,
          }),
        } as Response;
      }

      throw new Error(`Unexpected fetch call: ${url}`);
    });

    globalThis.fetch = fetchMock as typeof fetch;

    const bridge = createSSRInquiryBridge({
      bootstrap: {
        url: "http://cadenza-db:5000",
      },
    });

    const result = await bridge.inquire("devices.count");

    expect(result.count).toBe(924);
    expect(result.__inquiryMeta).toEqual(
      expect.objectContaining({
        inquiry: "devices.count",
        responded: 1,
        failed: 0,
      }),
    );
    expect(fetchMock).not.toHaveBeenCalledWith(
      "http://iot-db.localhost/delegation",
      expect.anything(),
    );
  });
});
