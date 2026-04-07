import { AUTHORITY_RUNTIME_STATUS_REPORT_INTENT } from "./runtimeStatusContract";
import {
  AUTHORITY_SERVICE_MANIFEST_REPORT_INTENT,
  AUTHORITY_SERVICE_MANIFEST_UPDATED_SIGNAL,
} from "./serviceManifestContract";

export const AUTHORITY_BOOTSTRAP_FULL_SYNC_INTENT =
  "meta-service-registry-full-sync";

export const AUTHORITY_BOOTSTRAP_FULL_SYNC_RESPONDER_TASK_NAME =
  "Respond service registry full sync";
export const AUTHORITY_SERVICE_INSTANCE_REGISTER_INTENT =
  "meta-service-registry-authority-service-instance-register";
export const AUTHORITY_SERVICE_INSTANCE_REGISTER_TASK_NAME =
  "Register service instance with authority";
export const AUTHORITY_SERVICE_INSTANCE_TRANSPORT_REGISTER_INTENT =
  "meta-service-registry-authority-service-instance-transport-register";
export const AUTHORITY_SERVICE_INSTANCE_TRANSPORT_REGISTER_TASK_NAME =
  "Register service instance transport with authority";

export interface AuthorityBootstrapIntentSpec {
  intentName: string;
  authorityTaskName: string;
  defaultTimeoutMs: number;
}

export const AUTHORITY_BOOTSTRAP_INTENT_SPECS: AuthorityBootstrapIntentSpec[] = [
  {
    intentName: AUTHORITY_BOOTSTRAP_FULL_SYNC_INTENT,
    authorityTaskName: AUTHORITY_BOOTSTRAP_FULL_SYNC_RESPONDER_TASK_NAME,
    defaultTimeoutMs: 120_000,
  },
  {
    intentName: AUTHORITY_SERVICE_INSTANCE_REGISTER_INTENT,
    authorityTaskName: AUTHORITY_SERVICE_INSTANCE_REGISTER_TASK_NAME,
    defaultTimeoutMs: 15_000,
  },
  {
    intentName: AUTHORITY_SERVICE_INSTANCE_TRANSPORT_REGISTER_INTENT,
    authorityTaskName: AUTHORITY_SERVICE_INSTANCE_TRANSPORT_REGISTER_TASK_NAME,
    defaultTimeoutMs: 15_000,
  },
  {
    intentName: AUTHORITY_SERVICE_MANIFEST_REPORT_INTENT,
    authorityTaskName: "Report service manifest to authority",
    defaultTimeoutMs: 15_000,
  },
  {
    intentName: AUTHORITY_RUNTIME_STATUS_REPORT_INTENT,
    authorityTaskName: "Record authority runtime status report",
    defaultTimeoutMs: 5_000,
  },
];

export const AUTHORITY_BOOTSTRAP_SIGNAL_NAMES = [
  AUTHORITY_SERVICE_MANIFEST_UPDATED_SIGNAL,
] as const;

const AUTHORITY_BOOTSTRAP_INTENT_SPEC_BY_NAME = new Map(
  AUTHORITY_BOOTSTRAP_INTENT_SPECS.map((spec) => [spec.intentName, spec]),
);

const AUTHORITY_BOOTSTRAP_SIGNAL_NAME_SET = new Set<string>(
  AUTHORITY_BOOTSTRAP_SIGNAL_NAMES,
);

const AUTHORITY_BOOTSTRAP_INSERT_INTENT_BY_TABLE = new Map<
  string,
  string
>([
  ["service_instance", AUTHORITY_SERVICE_INSTANCE_REGISTER_INTENT],
  [
    "service_instance_transport",
    AUTHORITY_SERVICE_INSTANCE_TRANSPORT_REGISTER_INTENT,
  ],
]);

export function getAuthorityBootstrapIntentSpec(
  intentName: string | null | undefined,
): AuthorityBootstrapIntentSpec | null {
  const normalizedIntentName = String(intentName ?? "").trim();
  if (!normalizedIntentName) {
    return null;
  }

  return AUTHORITY_BOOTSTRAP_INTENT_SPEC_BY_NAME.get(normalizedIntentName) ?? null;
}

export function isAuthorityBootstrapIntent(
  intentName: string | null | undefined,
): boolean {
  return getAuthorityBootstrapIntentSpec(intentName) !== null;
}

export function isAuthorityBootstrapSignal(
  signalName: string | null | undefined,
): boolean {
  return AUTHORITY_BOOTSTRAP_SIGNAL_NAME_SET.has(String(signalName ?? "").trim());
}

export function getAuthorityBootstrapInsertIntentSpecForTable(
  tableName: string | null | undefined,
): AuthorityBootstrapIntentSpec | null {
  const normalizedTableName = String(tableName ?? "").trim();
  if (!normalizedTableName) {
    return null;
  }

  const intentName =
    AUTHORITY_BOOTSTRAP_INSERT_INTENT_BY_TABLE.get(normalizedTableName);
  if (!intentName) {
    return null;
  }

  return getAuthorityBootstrapIntentSpec(intentName);
}
