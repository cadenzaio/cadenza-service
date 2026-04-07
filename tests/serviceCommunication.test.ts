import { describe, expect, it } from "vitest";
import {
  buildServiceCommunicationEstablishedContext,
  buildServiceCommunicationRetryContext,
  resolveServiceCommunicationPersistenceDescriptor,
  META_SERVICE_COMMUNICATION_PERSIST_RETRY_SIGNAL,
  SERVICE_COMMUNICATION_MAP_CONFLICT_TARGET,
  SERVICE_COMMUNICATION_PERSIST_RETRY_DELAYS_MS,
} from "../src/utils/serviceCommunication";

describe("service communication helpers", () => {
  it("builds idempotent service communication inserts", () => {
    expect(
      buildServiceCommunicationEstablishedContext({
        serviceInstanceId: "instance-a",
        serviceInstanceClientId: "instance-b",
        communicationType: "signal",
      }),
    ).toEqual({
      data: {
        serviceInstanceId: "instance-a",
        serviceInstanceClientId: "instance-b",
        communicationType: "signal",
      },
      onConflict: {
        target: [...SERVICE_COMMUNICATION_MAP_CONFLICT_TARGET],
        action: {
          do: "nothing",
        },
      },
      queryData: {
        onConflict: {
          target: [...SERVICE_COMMUNICATION_MAP_CONFLICT_TARGET],
          action: {
            do: "nothing",
          },
        },
      },
    });
  });

  it("normalizes service communication persistence descriptors from retry contexts", () => {
    expect(
      resolveServiceCommunicationPersistenceDescriptor({
        ...buildServiceCommunicationRetryContext({
          serviceInstanceId: "instance-a",
          serviceInstanceClientId: "instance-b",
          communicationType: "signal",
          retryAttempt: 2,
        }),
      }),
    ).toEqual({
      serviceInstanceId: "instance-a",
      serviceInstanceClientId: "instance-b",
      communicationType: "signal",
      retryAttempt: 2,
    });
  });

  it("exposes the bounded local retry contract", () => {
    expect(META_SERVICE_COMMUNICATION_PERSIST_RETRY_SIGNAL).toBe(
      "meta.service_registry.service_communication_persist_retry_requested",
    );
    expect([...SERVICE_COMMUNICATION_PERSIST_RETRY_DELAYS_MS]).toEqual([
      250,
      750,
      1500,
      3000,
    ]);
  });
});
