export type ServiceManifestPublicationLayer =
  | "routing_capability"
  | "business_structural"
  | "local_meta_structural";

export type ServiceManifestTaskDefinition = {
  name: string;
  version: number;
  description: string;
  service_name: string;
  is_meta: boolean;
  is_sub_meta: boolean;
  is_hidden: boolean;
  is_ephemeral: boolean;
  is_signal: boolean;
  concurrency: number;
  timeout: number;
  retry_count: number;
  retry_delay: number;
  retry_delay_max: number;
  retry_delay_factor: number;
  validate_input_context: boolean;
  validate_output_context: boolean;
  input_context_schema: Record<string, unknown>;
  output_context_schema: Record<string, unknown>;
  signals: {
    emits: string[];
    emits_after: string[];
    emits_on_fail: string[];
    observed: string[];
  };
  intents: {
    handles: string[];
    inquires: string[];
  };
};

export type ServiceManifestSignalDefinition = {
  name: string;
  is_global: boolean;
  domain: string | null;
  action: string;
  is_meta: boolean;
  delivery_mode: "single" | "broadcast";
  broadcast_filter: Record<string, unknown> | null;
};

export type ServiceManifestIntentDefinition = {
  name: string;
  description: string;
  input: Record<string, unknown>;
  output: Record<string, unknown>;
  is_meta: boolean;
};

export type ServiceManifestActorDefinition = {
  name: string;
  version: number;
  description: string;
  service_name: string;
  default_key: string;
  load_policy: string;
  write_contract: string;
  runtime_read_guard: string;
  consistency_profile: string | null;
  key_definition: Record<string, unknown> | null;
  state_definition: Record<string, unknown>;
  retry_policy: Record<string, unknown>;
  idempotency_policy: Record<string, unknown>;
  session_policy: Record<string, unknown>;
  is_meta: boolean;
};

export type ServiceManifestRoutineDefinition = {
  name: string;
  version: number;
  description: string;
  service_name: string;
  is_meta: boolean;
};

export type ServiceManifestHelperDefinition = {
  name: string;
  version: number;
  description: string;
  service_name: string;
  is_meta: boolean;
  handler_source: string;
  language: "js" | "ts";
};

export type ServiceManifestGlobalDefinition = {
  name: string;
  version: number;
  description: string;
  service_name: string;
  is_meta: boolean;
  value: unknown;
};

export type ServiceManifestDirectionalTaskMap = {
  task_name: string;
  task_version: number;
  predecessor_task_name: string;
  predecessor_task_version: number;
  service_name: string;
  predecessor_service_name: string;
};

export type ServiceManifestSignalTaskMap = {
  signal_name: string;
  is_global: boolean;
  task_name: string;
  task_version: number;
  service_name: string;
};

export type ServiceManifestIntentTaskMap = {
  intent_name: string;
  task_name: string;
  task_version: number;
  service_name: string;
};

export type ServiceManifestActorTaskMap = {
  actor_name: string;
  actor_version: number;
  task_name: string;
  task_version: number;
  service_name: string;
  mode: "read" | "write" | "meta";
  description: string;
  is_meta: boolean;
};

export type ServiceManifestTaskRoutineMap = {
  task_name: string;
  task_version: number;
  routine_name: string;
  routine_version: number;
  service_name: string;
};

export type ServiceManifestTaskHelperMap = {
  task_name: string;
  task_version: number;
  service_name: string;
  alias: string;
  helper_name: string;
  helper_version: number;
};

export type ServiceManifestHelperHelperMap = {
  helper_name: string;
  helper_version: number;
  service_name: string;
  alias: string;
  dependency_helper_name: string;
  dependency_helper_version: number;
};

export type ServiceManifestTaskGlobalMap = {
  task_name: string;
  task_version: number;
  service_name: string;
  alias: string;
  global_name: string;
  global_version: number;
};

export type ServiceManifestHelperGlobalMap = {
  helper_name: string;
  helper_version: number;
  service_name: string;
  alias: string;
  global_name: string;
  global_version: number;
};

export type ServiceManifestSnapshot = {
  serviceName: string;
  serviceInstanceId: string;
  revision: number;
  manifestHash: string;
  publishedAt: string;
  publicationLayer: ServiceManifestPublicationLayer;
  tasks: ServiceManifestTaskDefinition[];
  signals: ServiceManifestSignalDefinition[];
  intents: ServiceManifestIntentDefinition[];
  actors: ServiceManifestActorDefinition[];
  routines: ServiceManifestRoutineDefinition[];
  helpers: ServiceManifestHelperDefinition[];
  globals: ServiceManifestGlobalDefinition[];
  directionalTaskMaps: ServiceManifestDirectionalTaskMap[];
  signalToTaskMaps: ServiceManifestSignalTaskMap[];
  intentToTaskMaps: ServiceManifestIntentTaskMap[];
  actorTaskMaps: ServiceManifestActorTaskMap[];
  taskToRoutineMaps: ServiceManifestTaskRoutineMap[];
  taskToHelperMaps: ServiceManifestTaskHelperMap[];
  helperToHelperMaps: ServiceManifestHelperHelperMap[];
  taskToGlobalMaps: ServiceManifestTaskGlobalMap[];
  helperToGlobalMaps: ServiceManifestHelperGlobalMap[];
};
