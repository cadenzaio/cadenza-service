import { beforeEach, describe, expect, it } from "vitest";
import { GraphContext } from "@cadenza.io/core";
import Cadenza from "../src/Cadenza";

function resetRuntimeState() {
  try {
    Cadenza.reset();
  } catch {
    // Ignore first-run reset errors before bootstrap.
  }
}

describe("SignalTransmissionTask", () => {
  beforeEach(() => {
    resetRuntimeState();
    Cadenza.bootstrap();
    Cadenza.serviceRegistry.serviceName = "OrdersService";
    Cadenza.serviceRegistry.serviceInstanceId = "orders-service-instance-1";
  });

  it("preserves the original signal emission payload for remote receivers", async () => {
    const task = Cadenza.createSignalTransmissionTask(
      "orders.created",
      "BillingService",
    );

    expect(task).toBeDefined();

    const result = await task!.execute(
      new GraphContext({
        orderId: "order-1",
        __executionTraceId: "trace-1",
        __routineExecId: "routine-1",
        __signalEmissionId: "signal-1",
        __signalEmission: {
          uuid: "signal-1",
          fullSignalName: "orders.created",
          signalName: "orders.created",
          signalTag: null,
          taskName: "Publish order created",
          taskVersion: 1,
          taskExecutionId: "task-exec-1",
          routineExecutionId: "routine-1",
          executionTraceId: "trace-1",
          emittedAt: "2026-03-25T00:00:00.000Z",
          consumed: false,
          consumedBy: null,
          isMeta: false,
        },
      }),
      () => undefined,
      async () => ({}),
      () => undefined,
    );

    expect(result).toMatchObject({
      __serviceName: "BillingService",
      __signalName: "orders.created",
      __signalEmissionId: "signal-1",
      __signalEmission: {
        uuid: "signal-1",
        signalName: "orders.created",
        executionTraceId: "trace-1",
        serviceName: "OrdersService",
        serviceInstanceId: "orders-service-instance-1",
        taskName: "Publish order created",
      },
    });
    expect(result.__broadcast).toBeUndefined();
    expect(result.__broadcastFilter).toBeUndefined();
  });

  it("applies broadcast delivery metadata to remote signal transmissions", async () => {
    Cadenza.createTask("Publish broadcast orders signal", () => true).attachSignal({
      name: "orders.broadcast",
      deliveryMode: "broadcast",
      broadcastFilter: {
        serviceNames: ["BillingService"],
        origins: ["http://billing.internal"],
      },
    });

    const task = Cadenza.createSignalTransmissionTask(
      "orders.broadcast",
      "BillingService",
    );

    const result = await task!.execute(
      new GraphContext({
        orderId: "order-2",
        __executionTraceId: "trace-2",
        __routineExecId: "routine-2",
      }),
      () => undefined,
      async () => ({}),
      () => undefined,
    );

    expect(result).toMatchObject({
      __serviceName: "BillingService",
      __signalName: "orders.broadcast",
      __broadcast: true,
      __broadcastFilter: {
        serviceNames: ["BillingService"],
        origins: ["http://billing.internal"],
      },
    });
  });
});
