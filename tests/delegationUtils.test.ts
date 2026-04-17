import { describe, expect, it } from "vitest";

import {
  attachDelegationRequestSnapshot,
  buildDelegationFailureContext,
  restoreDelegationRequestSnapshot,
} from "../src/utils/delegation";

describe("delegation snapshot helpers", () => {
  it("preserves Date values in delegation request snapshots", () => {
    const timestamp = new Date("2026-04-10T17:16:58.974Z");
    const predictedEta = new Date("2026-04-20T12:10:07.035Z");

    const attached = attachDelegationRequestSnapshot({
      __remoteRoutineName: "Query health_metric",
      __serviceName: "IotDbService",
      timestamp,
      nested: {
        predictedEta,
      },
    }) as Record<string, any>;

    const snapshot = attached.__delegationRequestContext;
    expect(snapshot.timestamp).toBeInstanceOf(Date);
    expect(snapshot.timestamp.toISOString()).toBe(timestamp.toISOString());
    expect(snapshot.nested.predictedEta).toBeInstanceOf(Date);
    expect(snapshot.nested.predictedEta.toISOString()).toBe(
      predictedEta.toISOString(),
    );
  });

  it("restores Date values from a delegation snapshot result", () => {
    const timestamp = new Date("2026-04-10T17:16:58.974Z");
    const predictedEta = new Date("2026-04-20T12:10:07.035Z");

    const restored = restoreDelegationRequestSnapshot({
      __status: "success",
      rowCount: 1,
      __delegationRequestContext: {
        __remoteRoutineName: "Query health_metric",
        __serviceName: "IotDbService",
        timestamp,
        predictedEta,
      },
    }) as Record<string, any>;

    expect(restored.timestamp).toBeInstanceOf(Date);
    expect(restored.timestamp.toISOString()).toBe(timestamp.toISOString());
    expect(restored.predictedEta).toBeInstanceOf(Date);
    expect(restored.predictedEta.toISOString()).toBe(
      predictedEta.toISOString(),
    );
  });

  it("restores a delegation snapshot stored under __metadata", () => {
    const restored = restoreDelegationRequestSnapshot({
      __status: "error",
      errored: true,
      __metadata: {
        __delegationRequestContext: {
          __remoteRoutineName: "Insert actor_session_state",
          __serviceName: "CadenzaDB",
          data: {
            actor_name: "TelemetrySessionActor",
          },
          queryData: {
            data: {
              actor_name: "TelemetrySessionActor",
            },
          },
        },
      },
    }) as Record<string, any>;

    expect(restored.__remoteRoutineName).toBe("Insert actor_session_state");
    expect(restored.data).toEqual({
      actor_name: "TelemetrySessionActor",
    });
    expect(restored.queryData).toEqual({
      data: {
        actor_name: "TelemetrySessionActor",
      },
    });
  });

  it("slims actor_session_state delegation snapshots to recovery-critical payload only", () => {
    const attached = attachDelegationRequestSnapshot({
      __remoteRoutineName: "Insert actor_session_state",
      __serviceName: "CadenzaDB",
      __localTaskName: "Persist telemetry ingest session state best effort",
      deviceId: "device-7",
      telemetryPayload: {
        deviceId: "device-7",
        timestamp: "2026-04-17T12:00:00.000Z",
        readings: {
          temperature: 23.5,
          humidity: 51.86,
          battery: 85.97,
        },
      },
      data: {
        actor_name: "TelemetrySessionActor",
        actor_key: "device-7",
        durable_state: {
          lastTelemetry: {
            deviceId: "device-7",
          },
        },
      },
      queryData: {
        data: {
          actor_name: "TelemetrySessionActor",
          actor_key: "device-7",
        },
        onConflict: {
          target: ["actor_name", "actor_key"],
        },
      },
    }) as Record<string, any>;

    expect(attached.__delegationRequestContext).toEqual({
      __remoteRoutineName: "Insert actor_session_state",
      __serviceName: "CadenzaDB",
      __localTaskName: "Persist telemetry ingest session state best effort",
      data: {
        actor_name: "TelemetrySessionActor",
        actor_key: "device-7",
        durable_state: {
          lastTelemetry: {
            deviceId: "device-7",
          },
        },
      },
      queryData: {
        data: {
          actor_name: "TelemetrySessionActor",
          actor_key: "device-7",
        },
        onConflict: {
          target: ["actor_name", "actor_key"],
        },
      },
    });
    expect(attached.__delegationRequestContext.deviceId).toBeUndefined();
    expect(attached.__delegationRequestContext.telemetryPayload).toBeUndefined();
  });

  it("restores a delegation snapshot from a failed inquiry envelope", () => {
    const restored = restoreDelegationRequestSnapshot({
      errored: true,
      __error: "Inquiry 'iot-telemetry-ingest' did not complete successfully",
      __inquiryMeta: {
        inquiry: "iot-telemetry-ingest",
        timedOut: 1,
      },
      __delegationRequestContext: {
        __remoteRoutineName: "Normalize telemetry ingest payload",
        __serviceName: "TelemetryCollectorService",
        deviceId: "device-7",
        timestamp: "2026-04-10T23:20:57.559Z",
        readings: {
          temperature: 23.5,
          humidity: 51.86,
          battery: 85.97,
        },
      },
    }) as Record<string, any>;

    expect(restored.__remoteRoutineName).toBe(
      "Normalize telemetry ingest payload",
    );
    expect(restored.deviceId).toBe("device-7");
    expect(restored.readings).toEqual({
      temperature: 23.5,
      humidity: 51.86,
      battery: 85.97,
    });
  });

  it("builds a slim delegation failure envelope while preserving the retry snapshot", () => {
    const failure = buildDelegationFailureContext(
      "meta.fetch.delegate_failed",
      {
        __remoteRoutineName: "Insert actor_session_state",
        __serviceName: "CadenzaDB",
        __fetchId: "CadenzaDB|rest",
        __routeKey: "CadenzaDB|internal|http://cadenza-db-service:8080",
        __retries: 2,
        __triedInstances: ["instance-1"],
        __metadata: {
          __deputyExecId: "deputy-1",
        },
        __delegationRequestContext: {
          __remoteRoutineName: "Insert actor_session_state",
          __serviceName: "CadenzaDB",
          actor_name: "TelemetrySessionActor",
          durable_state: {
            lastTelemetry: {
              deviceId: "device-1",
            },
          },
          queryData: {
            data: {
              actor_name: "TelemetrySessionActor",
            },
          },
        },
        data: {
          actor_name: "TelemetrySessionActor",
        },
        queryData: {
          data: {
            actor_name: "TelemetrySessionActor",
          },
        },
      },
      new Error("socket hang up"),
    ) as Record<string, any>;

    expect(failure.__signalName).toBe("meta.fetch.delegate_failed");
    expect(failure.__error).toBe("socket hang up");
    expect(failure.errored).toBe(true);
    expect(failure.__remoteRoutineName).toBe("Insert actor_session_state");
    expect(failure.__delegationRequestContext?.actor_name).toBe(
      "TelemetrySessionActor",
    );
    expect(failure.data).toBeUndefined();
    expect(failure.queryData).toBeUndefined();
  });

  it("hoists the retry snapshot from __metadata into the failure envelope", () => {
    const failure = buildDelegationFailureContext(
      "meta.fetch.delegate_failed",
      {
        __remoteRoutineName: "Insert actor_session_state",
        __serviceName: "CadenzaDB",
        __metadata: {
          __delegationRequestContext: {
            __remoteRoutineName: "Insert actor_session_state",
            __serviceName: "CadenzaDB",
            data: {
              actor_name: "TelemetrySessionActor",
            },
          },
        },
      },
      new Error("timeout"),
    ) as Record<string, any>;

    expect(failure.__delegationRequestContext).toEqual({
      __remoteRoutineName: "Insert actor_session_state",
      __serviceName: "CadenzaDB",
      data: {
        actor_name: "TelemetrySessionActor",
      },
    });
  });
});
