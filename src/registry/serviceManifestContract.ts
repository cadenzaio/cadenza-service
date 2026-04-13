import type { ServiceManifestSnapshot } from "../types/serviceManifest";

export const AUTHORITY_SERVICE_MANIFEST_REPORT_INTENT =
  "meta-service-registry-authority-service-manifest-report";

export const AUTHORITY_SERVICE_MANIFEST_UPDATED_SIGNAL =
  "global.meta.cadenza_db.service_manifest_updated";

export function normalizeServiceManifestSnapshot(
  input: unknown,
): ServiceManifestSnapshot | null {
  if (!input || typeof input !== "object" || Array.isArray(input)) {
    return null;
  }

  const record = input as Record<string, unknown>;
  const serviceName = String(record.serviceName ?? "").trim();
  const serviceInstanceId = String(record.serviceInstanceId ?? "").trim();
  const revision = Math.max(1, Math.trunc(Number(record.revision ?? 0) || 0));
  const manifestHash = String(record.manifestHash ?? "").trim();
  const publishedAt = String(record.publishedAt ?? "").trim();

  if (!serviceName || !serviceInstanceId || !manifestHash || !publishedAt) {
    return null;
  }

  const normalizeArray = <T extends Record<string, unknown>>(
    value: unknown,
  ): T[] =>
    Array.isArray(value)
      ? value.filter(
          (entry): entry is T =>
            !!entry && typeof entry === "object" && !Array.isArray(entry),
        )
      : [];

  return {
    serviceName,
    serviceInstanceId,
    revision,
    manifestHash,
    publishedAt,
    publicationLayer:
      record.publicationLayer === "routing_capability" ||
      record.publicationLayer === "business_structural" ||
      record.publicationLayer === "local_meta_structural"
        ? record.publicationLayer
        : "business_structural",
    tasks: normalizeArray(record.tasks),
    signals: normalizeArray(record.signals),
    intents: normalizeArray(record.intents),
    actors: normalizeArray(record.actors),
    routines: normalizeArray(record.routines),
    helpers: normalizeArray(record.helpers),
    globals: normalizeArray(record.globals),
    directionalTaskMaps: normalizeArray(record.directionalTaskMaps),
    signalToTaskMaps: normalizeArray(record.signalToTaskMaps),
    intentToTaskMaps: normalizeArray(record.intentToTaskMaps),
    actorTaskMaps: normalizeArray(record.actorTaskMaps),
    taskToRoutineMaps: normalizeArray(record.taskToRoutineMaps),
    taskToHelperMaps: normalizeArray(record.taskToHelperMaps),
    helperToHelperMaps: normalizeArray(record.helperToHelperMaps),
    taskToGlobalMaps: normalizeArray(record.taskToGlobalMaps),
    helperToGlobalMaps: normalizeArray(record.helperToGlobalMaps),
  };
}

export function selectLatestServiceManifestSnapshots(
  snapshots: ServiceManifestSnapshot[],
): ServiceManifestSnapshot[] {
  const latestByInstance = new Map<string, ServiceManifestSnapshot>();

  for (const snapshot of snapshots) {
    const current = latestByInstance.get(snapshot.serviceInstanceId);
    if (!current) {
      latestByInstance.set(snapshot.serviceInstanceId, snapshot);
      continue;
    }

    if (snapshot.revision > current.revision) {
      latestByInstance.set(snapshot.serviceInstanceId, snapshot);
      continue;
    }

    if (
      snapshot.revision === current.revision &&
      snapshot.publishedAt.localeCompare(current.publishedAt) > 0
    ) {
      latestByInstance.set(snapshot.serviceInstanceId, snapshot);
    }
  }

  return Array.from(latestByInstance.values()).sort((left, right) =>
    `${left.serviceName}|${left.serviceInstanceId}`.localeCompare(
      `${right.serviceName}|${right.serviceInstanceId}`,
    ),
  );
}
