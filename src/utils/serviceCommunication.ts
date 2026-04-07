import type { AnyObject } from "@cadenza.io/core";

export const SERVICE_COMMUNICATION_MAP_CONFLICT_TARGET = [
  "service_instance_id",
  "service_instance_client_id",
  "communication_type",
] as const;
export const META_SERVICE_COMMUNICATION_PERSIST_RETRY_SIGNAL =
  "meta.service_registry.service_communication_persist_retry_requested";
export const SERVICE_COMMUNICATION_PERSIST_RETRY_DELAYS_MS = [
  250,
  750,
  1500,
  3000,
] as const;

export interface ServiceCommunicationPersistenceDescriptor {
  serviceInstanceId: string;
  serviceInstanceClientId: string;
  communicationType: string;
  retryAttempt: number;
}

export function buildServiceCommunicationEstablishedContext(input: {
  serviceInstanceId: string;
  serviceInstanceClientId: string;
  communicationType: string;
}): AnyObject {
  const { serviceInstanceId, serviceInstanceClientId, communicationType } = input;

  return {
    data: {
      serviceInstanceId,
      serviceInstanceClientId,
      communicationType,
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
  };
}

export function resolveServiceCommunicationPersistenceDescriptor(
  ctx: AnyObject | null | undefined,
): ServiceCommunicationPersistenceDescriptor | null {
  const data =
    ctx?.data && typeof ctx.data === "object" && !Array.isArray(ctx.data)
      ? (ctx.data as AnyObject)
      : {};
  const queryData =
    ctx?.queryData && typeof ctx.queryData === "object" && !Array.isArray(ctx.queryData)
      ? (ctx.queryData as AnyObject)
      : {};
  const queryDataData =
    queryData.data &&
    typeof queryData.data === "object" &&
    !Array.isArray(queryData.data)
      ? (queryData.data as AnyObject)
      : {};

  const serviceInstanceId = String(
    data.serviceInstanceId ??
      data.service_instance_id ??
      queryDataData.serviceInstanceId ??
      queryDataData.service_instance_id ??
      "",
  ).trim();
  const serviceInstanceClientId = String(
    data.serviceInstanceClientId ??
      data.service_instance_client_id ??
      queryDataData.serviceInstanceClientId ??
      queryDataData.service_instance_client_id ??
      "",
  ).trim();
  const communicationType = String(
    data.communicationType ??
      data.communication_type ??
      queryDataData.communicationType ??
      queryDataData.communication_type ??
      "",
  ).trim();
  const retryAttempt = Number(
    ctx?.__serviceCommunicationRetryAttempt ??
      data.retryAttempt ??
      queryDataData.retryAttempt ??
      0,
  );

  if (!serviceInstanceId || !serviceInstanceClientId || !communicationType) {
    return null;
  }

  return {
    serviceInstanceId,
    serviceInstanceClientId,
    communicationType,
    retryAttempt:
      Number.isFinite(retryAttempt) && retryAttempt > 0
        ? Math.trunc(retryAttempt)
        : 0,
  };
}

export function buildServiceCommunicationRetryContext(
  descriptor: ServiceCommunicationPersistenceDescriptor,
): AnyObject {
  return {
    ...buildServiceCommunicationEstablishedContext(descriptor),
    __serviceCommunicationRetryAttempt: descriptor.retryAttempt,
  };
}
