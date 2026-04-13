import Cadenza from "../Cadenza";
import { decomposeSignalName } from "../utils/tools";
import { isMetaIntentName } from "../utils/inquiry";
import CoreCadenza from "@cadenza.io/core";
import type { Task, GraphRoutine } from "@cadenza.io/core";
import type {
  ServiceManifestActorDefinition,
  ServiceManifestActorTaskMap,
  ServiceManifestGlobalDefinition,
  ServiceManifestHelperDefinition,
  ServiceManifestHelperGlobalMap,
  ServiceManifestHelperHelperMap,
  ServiceManifestIntentDefinition,
  ServiceManifestIntentTaskMap,
  ServiceManifestPublicationLayer,
  ServiceManifestRoutineDefinition,
  ServiceManifestSignalDefinition,
  ServiceManifestSignalTaskMap,
  ServiceManifestSnapshot,
  ServiceManifestTaskDefinition,
  ServiceManifestTaskGlobalMap,
  ServiceManifestTaskHelperMap,
  ServiceManifestTaskRoutineMap,
  ServiceManifestDirectionalTaskMap,
} from "../types/serviceManifest";

type HelperDefinition = {
  name: string;
  version: number;
  description: string;
  isMeta: boolean;
  helperFunction: (...args: any[]) => any;
  helperAliases: Map<string, string>;
  globalAliases: Map<string, string>;
  destroyed?: boolean;
};

type GlobalDefinition = {
  name: string;
  version: number;
  description: string;
  isMeta: boolean;
  value: unknown;
  destroyed?: boolean;
};

const ACTOR_TASK_METADATA = Symbol.for("@cadenza.io/core/actor-task-meta");

type ActorTaskRuntimeMetadata = {
  actorName: string;
  actorDescription?: string;
  actorKind: "standard" | "meta";
  mode: "read" | "write" | "meta";
  forceMeta: boolean;
};

function getActorTaskRuntimeMetadata(
  taskFunction: unknown,
): ActorTaskRuntimeMetadata | undefined {
  if (typeof taskFunction !== "function") {
    return undefined;
  }

  return (taskFunction as { [ACTOR_TASK_METADATA]?: ActorTaskRuntimeMetadata })[
    ACTOR_TASK_METADATA
  ];
}

function sanitizeManifestValue(value: unknown): unknown {
  if (value === null) {
    return null;
  }

  if (value === undefined || typeof value === "function") {
    return undefined;
  }

  if (Array.isArray(value)) {
    const items: unknown[] = [];
    for (const item of value) {
      const sanitizedItem = sanitizeManifestValue(item);
      if (sanitizedItem !== undefined) {
        items.push(sanitizedItem);
      }
    }
    return items;
  }

  if (typeof value === "object") {
    const output: Record<string, unknown> = {};
    for (const [key, nestedValue] of Object.entries(value)) {
      const sanitizedNestedValue = sanitizeManifestValue(nestedValue);
      if (sanitizedNestedValue !== undefined) {
        output[key] = sanitizedNestedValue;
      }
    }
    return output;
  }

  return value;
}

function stableSerialize(value: unknown): string {
  const sanitized = sanitizeManifestValue(value);

  const serialize = (input: unknown): string => {
    if (input === null || typeof input !== "object") {
      return JSON.stringify(input);
    }

    if (Array.isArray(input)) {
      return `[${input.map((item) => serialize(item)).join(",")}]`;
    }

    const record = input as Record<string, unknown>;
    const keys = Object.keys(record).sort();
    return `{${keys
      .map((key) => `${JSON.stringify(key)}:${serialize(record[key])}`)
      .join(",")}}`;
  };

  return serialize(sanitized);
}

function hashManifest(value: unknown): string {
  const serialized = stableSerialize(value);
  let hash = 5381;

  for (let index = 0; index < serialized.length; index += 1) {
    hash = (hash * 33) ^ serialized.charCodeAt(index);
  }

  return `m${(hash >>> 0).toString(16)}`;
}

function canonicalizeSignalName(signalName: string | undefined): string {
  if (typeof signalName !== "string") {
    return "";
  }

  return signalName.split(":")[0]?.trim() ?? "";
}

function buildTaskDefinition(task: Task, serviceName: string): ServiceManifestTaskDefinition {
  return {
    name: task.name,
    version: task.version,
    description: task.description,
    service_name: serviceName,
    is_meta: task.isMeta === true,
    is_sub_meta: task.isSubMeta === true,
    is_hidden: task.isHidden === true,
    is_ephemeral: task.isEphemeral === true,
    is_signal: task.isSignal === true,
    concurrency: task.concurrency,
    timeout: task.timeout,
    retry_count: task.retryCount,
    retry_delay: task.retryDelay,
    retry_delay_max: task.retryDelayMax,
    retry_delay_factor: task.retryDelayFactor,
    validate_input_context: task.validateInputContext,
    validate_output_context: task.validateOutputContext,
    input_context_schema:
      (sanitizeManifestValue(task.inputContextSchema) as Record<string, unknown>) ??
      { type: "object" },
    output_context_schema:
      (sanitizeManifestValue(task.outputContextSchema) as Record<string, unknown>) ??
      { type: "object" },
    signals: {
      emits: Array.from(task.emitsSignals).sort(),
      emits_after: Array.from(task.signalsToEmitAfter).sort(),
      emits_on_fail: Array.from(task.signalsToEmitOnFail).sort(),
      observed: Array.from(task.observedSignals).sort(),
    },
    intents: {
      handles: Array.from(task.handlesIntents).sort(),
      inquires: Array.from(task.inquiresIntents).sort(),
    },
  };
}

function buildActorDefinition(
  actor: any,
  serviceName: string,
): ServiceManifestActorDefinition | null {
  const definition = sanitizeManifestValue(
    typeof actor?.toDefinition === "function" ? actor.toDefinition() : {},
  ) as Record<string, unknown>;
  const name = String(definition?.name ?? actor?.spec?.name ?? "").trim();

  if (!name) {
    return null;
  }

  const stateDefinition =
    definition?.state && typeof definition.state === "object"
      ? (definition.state as Record<string, unknown>)
      : {};
  const actorKind =
    typeof definition?.kind === "string" ? definition.kind : actor?.kind;

  return {
    name,
    version: 1,
    description: String(definition?.description ?? actor?.spec?.description ?? ""),
    service_name: serviceName,
    default_key: String(definition?.defaultKey ?? actor?.spec?.defaultKey ?? "default"),
    load_policy: String(definition?.loadPolicy ?? actor?.spec?.loadPolicy ?? "eager"),
    write_contract: String(
      definition?.writeContract ?? actor?.spec?.writeContract ?? "overwrite",
    ),
    runtime_read_guard: String(
      definition?.runtimeReadGuard ?? actor?.spec?.runtimeReadGuard ?? "none",
    ),
    consistency_profile:
      definition?.consistencyProfile === null || definition?.consistencyProfile === undefined
        ? null
        : String(definition.consistencyProfile),
    key_definition:
      definition?.key && typeof definition.key === "object"
        ? (definition.key as Record<string, unknown>)
        : null,
    state_definition: stateDefinition,
    retry_policy:
      (definition?.retry as Record<string, unknown> | undefined) ?? {},
    idempotency_policy:
      (definition?.idempotency as Record<string, unknown> | undefined) ?? {},
    session_policy:
      (definition?.session as Record<string, unknown> | undefined) ?? {},
    is_meta: actorKind === "meta",
  };
}

function buildRoutineDefinition(
  routine: GraphRoutine,
  serviceName: string,
): ServiceManifestRoutineDefinition | null {
  const name = String(routine?.name ?? "").trim();
  if (!name) {
    return null;
  }

  return {
    name,
    version: routine.version,
    description: routine.description,
    service_name: serviceName,
    is_meta: routine.isMeta === true,
  };
}

function buildHelperDefinition(
  helper: HelperDefinition,
  serviceName: string,
): ServiceManifestHelperDefinition {
  return {
    name: helper.name,
    version: helper.version,
    description: helper.description,
    service_name: serviceName,
    is_meta: helper.isMeta === true,
    handler_source: helper.helperFunction.toString(),
    language: "js",
  };
}

function buildGlobalDefinition(
  globalDefinition: GlobalDefinition,
  serviceName: string,
): ServiceManifestGlobalDefinition {
  return {
    name: globalDefinition.name,
    version: globalDefinition.version,
    description: globalDefinition.description,
    service_name: serviceName,
    is_meta: globalDefinition.isMeta === true,
    value: sanitizeManifestValue(globalDefinition.value),
  };
}

function shouldExportTask(task: Task): boolean {
  return (
    task.register !== false &&
    task.isHidden !== true &&
    !task.isDeputy &&
    !task.isEphemeral
  );
}

function shouldExportRoutine(routine: GraphRoutine): boolean {
  return Boolean(String(routine?.name ?? "").trim());
}

function buildTaskKey(task: { service_name: string; name: string; version: number }): string {
  return `${task.service_name}|${task.name}|${task.version}`;
}

function buildActorKey(actor: {
  service_name: string;
  name: string;
  version: number;
}): string {
  return `${actor.service_name}|${actor.name}|${actor.version}`;
}

function buildRoutineKey(routine: {
  service_name: string;
  name: string;
  version: number;
}): string {
  return `${routine.service_name}|${routine.name}|${routine.version}`;
}

function buildHelperKey(helper: {
  service_name: string;
  name: string;
  version: number;
}): string {
  return `${helper.service_name}|${helper.name}|${helper.version}`;
}

function buildGlobalKey(globalDefinition: {
  service_name: string;
  name: string;
  version: number;
}): string {
  return `${globalDefinition.service_name}|${globalDefinition.name}|${globalDefinition.version}`;
}

function listManifestTasks(): Task[] {
  const registryTasks = Array.from(Cadenza.registry.tasks.values()).filter(
    (task): task is Task => Boolean(task),
  );
  const cachedTasks = Array.from(
    ((CoreCadenza as any).taskCache?.values?.() ?? []) as Iterable<Task>,
  ).filter((task): task is Task => Boolean(task));

  return Array.from(
    new Map(
      [...registryTasks, ...cachedTasks].map((task) => [task.name, task] as const),
    ).values(),
  );
}

function listManifestHelpers(): HelperDefinition[] {
  const toolRuntime = Cadenza as typeof Cadenza & {
    getAllHelpers?: () => unknown[];
  };
  return (toolRuntime.getAllHelpers?.() ?? []).filter(
    (helper): helper is HelperDefinition => Boolean(helper && !(helper as HelperDefinition).destroyed),
  );
}

function listManifestGlobals(): GlobalDefinition[] {
  const toolRuntime = Cadenza as typeof Cadenza & {
    getAllGlobals?: () => unknown[];
  };
  return (toolRuntime.getAllGlobals?.() ?? []).filter(
    (globalDefinition): globalDefinition is GlobalDefinition =>
      Boolean(globalDefinition && !(globalDefinition as GlobalDefinition).destroyed),
  );
}

function isRoutingCriticalMetaSignal(
  _signal: ServiceManifestSignalDefinition,
): boolean {
  return false;
}

function isRoutingCriticalMetaIntent(
  _intent: ServiceManifestIntentDefinition,
): boolean {
  return false;
}

export function buildServiceManifestSnapshot(params: {
  serviceName: string;
  serviceInstanceId: string;
  revision: number;
  publishedAt: string;
  publicationLayer?: ServiceManifestPublicationLayer;
}): ServiceManifestSnapshot {
  const {
    serviceName,
    serviceInstanceId,
    revision,
    publishedAt,
    publicationLayer = "business_structural",
  } = params;
  const tasks = listManifestTasks().filter(shouldExportTask);
  const helpers = listManifestHelpers();
  const globals = listManifestGlobals();
  const routines = Array.from(Cadenza.registry.routines.values())
    .filter((routine): routine is GraphRoutine => Boolean(routine))
    .filter(shouldExportRoutine);
  const actors = Cadenza.getAllActors();

  const taskDefinitions = tasks
    .map((task) => buildTaskDefinition(task, serviceName))
    .sort((left, right) =>
      `${left.name}:${left.version}`.localeCompare(`${right.name}:${right.version}`),
    );

  const signalDefinitions = new Map<string, ServiceManifestSignalDefinition>();
  const signalTaskMaps = new Map<string, ServiceManifestSignalTaskMap>();
  const intentTaskMaps = new Map<string, ServiceManifestIntentTaskMap>();
  const directionalTaskMaps = new Map<string, ServiceManifestDirectionalTaskMap>();
  const actorTaskMaps = new Map<string, ServiceManifestActorTaskMap>();
  const taskToRoutineMaps = new Map<string, ServiceManifestTaskRoutineMap>();
  const helperTaskMaps = new Map<string, ServiceManifestTaskHelperMap>();
  const helperHelperMaps = new Map<string, ServiceManifestHelperHelperMap>();
  const taskGlobalMaps = new Map<string, ServiceManifestTaskGlobalMap>();
  const helperGlobalMaps = new Map<string, ServiceManifestHelperGlobalMap>();

  const registerSignal = (signalName: string) => {
    const normalizedSignalName = canonicalizeSignalName(signalName);
    if (!normalizedSignalName || signalDefinitions.has(normalizedSignalName)) {
      return;
    }

    const { isGlobal, isMeta, domain, action } = decomposeSignalName(normalizedSignalName);
    const signalMetadata =
      (Cadenza.signalBroker as any).getSignalMetadata?.(normalizedSignalName) ??
      null;
    const deliveryMode =
      signalMetadata?.deliveryMode === "broadcast" ? "broadcast" : "single";
    const broadcastFilter =
      signalMetadata?.broadcastFilter &&
      typeof signalMetadata.broadcastFilter === "object" &&
      !Array.isArray(signalMetadata.broadcastFilter)
        ? (sanitizeManifestValue(signalMetadata.broadcastFilter) as
            | Record<string, unknown>
            | null)
        : null;
    signalDefinitions.set(normalizedSignalName, {
      name: normalizedSignalName,
      is_global: isGlobal,
      domain,
      action,
      is_meta: isMeta,
      delivery_mode: deliveryMode,
      broadcast_filter: broadcastFilter,
    });
  };

  for (const [signalName] of (Cadenza.signalBroker as any).signalObservers?.entries?.() ??
    []) {
    registerSignal(String(signalName));
  }

  for (const task of tasks) {
    for (const signalName of task.observedSignals) {
      registerSignal(signalName);
      const normalizedSignalName = canonicalizeSignalName(signalName);
      if (!normalizedSignalName) {
        continue;
      }
      const { isGlobal } = decomposeSignalName(normalizedSignalName);
      const key = `${normalizedSignalName}|${task.name}|${task.version}|${serviceName}`;
      signalTaskMaps.set(key, {
        signal_name: normalizedSignalName,
        is_global: isGlobal,
        task_name: task.name,
        task_version: task.version,
        service_name: serviceName,
      });
    }

    for (const signalName of task.emitsSignals) registerSignal(signalName);
    for (const signalName of task.signalsToEmitAfter) registerSignal(signalName);
    for (const signalName of task.signalsToEmitOnFail) registerSignal(signalName);

    for (const intentName of task.handlesIntents) {
      const normalizedIntentName = String(intentName ?? "").trim();
      if (!normalizedIntentName) {
        continue;
      }

      const key = `${normalizedIntentName}|${task.name}|${task.version}|${serviceName}`;
      intentTaskMaps.set(key, {
        intent_name: normalizedIntentName,
        task_name: task.name,
        task_version: task.version,
        service_name: serviceName,
      });
    }

    for (const nextTask of task.nextTasks) {
      if (!nextTask || !shouldExportTask(nextTask)) {
        continue;
      }

      const key = `${task.name}|${task.version}|${nextTask.name}|${nextTask.version}|${serviceName}`;
      directionalTaskMaps.set(key, {
        task_name: nextTask.name,
        task_version: nextTask.version,
        predecessor_task_name: task.name,
        predecessor_task_version: task.version,
        service_name: serviceName,
        predecessor_service_name: serviceName,
      });
    }

    const actorTaskMetadata = getActorTaskRuntimeMetadata(task.taskFunction);
    if (actorTaskMetadata?.actorName) {
      const key = `${actorTaskMetadata.actorName}|${task.name}|${task.version}|${serviceName}`;
      actorTaskMaps.set(key, {
        actor_name: actorTaskMetadata.actorName,
        actor_version: 1,
        task_name: task.name,
        task_version: task.version,
        service_name: serviceName,
        mode: actorTaskMetadata.mode,
        description: task.description ?? actorTaskMetadata.actorDescription ?? "",
        is_meta: actorTaskMetadata.actorKind === "meta" || task.isMeta === true,
      });
    }

    const taskTools = task as Task & {
      helperAliases?: Map<string, string>;
      globalAliases?: Map<string, string>;
    };

    for (const [alias, helperName] of taskTools.helperAliases?.entries?.() ?? []) {
      const helper = (Cadenza as typeof Cadenza & {
        getHelper?: (name: string) => HelperDefinition | undefined;
      }).getHelper?.(helperName);
      if (!helper) {
        continue;
      }
      const key = `${task.name}|${task.version}|${alias}|${helper.name}|${helper.version}|${serviceName}`;
      helperTaskMaps.set(key, {
        task_name: task.name,
        task_version: task.version,
        service_name: serviceName,
        alias,
        helper_name: helper.name,
        helper_version: helper.version,
      });
    }

    for (const [alias, globalName] of taskTools.globalAliases?.entries?.() ?? []) {
      const globalDefinition = (Cadenza as typeof Cadenza & {
        getGlobal?: (name: string) => GlobalDefinition | undefined;
      }).getGlobal?.(globalName);
      if (!globalDefinition) {
        continue;
      }
      const key = `${task.name}|${task.version}|${alias}|${globalDefinition.name}|${globalDefinition.version}|${serviceName}`;
      taskGlobalMaps.set(key, {
        task_name: task.name,
        task_version: task.version,
        service_name: serviceName,
        alias,
        global_name: globalDefinition.name,
        global_version: globalDefinition.version,
      });
    }
  }

  const intentDefinitions = Array.from(Cadenza.inquiryBroker.intents.values())
    .map((intent) => {
      const intentRecord = intent as unknown as Record<string, unknown> | undefined;
      const name = String(intentRecord?.name ?? "").trim();
      if (!name) {
        return null;
      }

      return {
        name,
        description:
          typeof intentRecord?.description === "string"
            ? String(intentRecord.description)
            : "",
        input:
          ((sanitizeManifestValue(
            intentRecord?.input,
          ) as Record<string, unknown> | undefined) ?? { type: "object" }),
        output:
          ((sanitizeManifestValue(
            intentRecord?.output,
          ) as Record<string, unknown> | undefined) ?? { type: "object" }),
        is_meta: isMetaIntentName(name),
      } satisfies ServiceManifestIntentDefinition;
    })
    .filter(
      (intentDefinition): intentDefinition is ServiceManifestIntentDefinition =>
        intentDefinition !== null,
    )
    .sort((left, right) => left.name.localeCompare(right.name));

  const actorDefinitions = actors
    .map((actor) => buildActorDefinition(actor, serviceName))
    .filter(
      (actorDefinition): actorDefinition is ServiceManifestActorDefinition =>
        actorDefinition !== null,
    )
    .sort((left, right) =>
      `${left.name}:${left.version}`.localeCompare(`${right.name}:${right.version}`),
    );

  const routineDefinitions = routines
    .map((routine) => buildRoutineDefinition(routine, serviceName))
    .filter(
      (routineDefinition): routineDefinition is ServiceManifestRoutineDefinition =>
        routineDefinition !== null,
    )
    .sort((left, right) =>
      `${left.name}:${left.version}`.localeCompare(`${right.name}:${right.version}`),
    );

  const helperDefinitions = helpers
    .map((helper) => buildHelperDefinition(helper, serviceName))
    .sort((left, right) =>
      `${left.name}:${left.version}`.localeCompare(`${right.name}:${right.version}`),
    );

  const globalDefinitions = globals
    .map((globalDefinition) => buildGlobalDefinition(globalDefinition, serviceName))
    .sort((left, right) =>
      `${left.name}:${left.version}`.localeCompare(`${right.name}:${right.version}`),
    );

  for (const helper of helpers) {
    for (const [alias, dependencyHelperName] of helper.helperAliases.entries()) {
      const dependencyHelper = (Cadenza as typeof Cadenza & {
        getHelper?: (name: string) => HelperDefinition | undefined;
      }).getHelper?.(dependencyHelperName);
      if (!dependencyHelper) {
        continue;
      }

      const key = `${helper.name}|${helper.version}|${alias}|${dependencyHelper.name}|${dependencyHelper.version}|${serviceName}`;
      helperHelperMaps.set(key, {
        helper_name: helper.name,
        helper_version: helper.version,
        service_name: serviceName,
        alias,
        dependency_helper_name: dependencyHelper.name,
        dependency_helper_version: dependencyHelper.version,
      });
    }

    for (const [alias, globalName] of helper.globalAliases.entries()) {
      const globalDefinition = (Cadenza as typeof Cadenza & {
        getGlobal?: (name: string) => GlobalDefinition | undefined;
      }).getGlobal?.(globalName);
      if (!globalDefinition) {
        continue;
      }

      const key = `${helper.name}|${helper.version}|${alias}|${globalDefinition.name}|${globalDefinition.version}|${serviceName}`;
      helperGlobalMaps.set(key, {
        helper_name: helper.name,
        helper_version: helper.version,
        service_name: serviceName,
        alias,
        global_name: globalDefinition.name,
        global_version: globalDefinition.version,
      });
    }
  }

  for (const routine of routines) {
    for (const task of routine.tasks) {
      if (!task) {
        continue;
      }

      const iterator = task.getIterator();
      while (iterator.hasNext()) {
        const nextTask = iterator.next();
        if (!nextTask || !shouldExportTask(nextTask)) {
          continue;
        }

        const key = `${routine.name}|${routine.version}|${nextTask.name}|${nextTask.version}|${serviceName}`;
        taskToRoutineMaps.set(key, {
          task_name: nextTask.name,
          task_version: nextTask.version,
          routine_name: routine.name,
          routine_version: routine.version,
          service_name: serviceName,
        });
      }
    }
  }

  const taskDefinitionsByKey = new Map(
    taskDefinitions.map((task) => [buildTaskKey(task), task] as const),
  );
  const signalDefinitionsByName = new Map(
    Array.from(signalDefinitions.values()).map((signal) => [signal.name, signal] as const),
  );
  const intentDefinitionsByName = new Map(
    intentDefinitions.map((intent) => [intent.name, intent] as const),
  );
  const actorDefinitionsByKey = new Map(
    actorDefinitions.map((actor) => [buildActorKey(actor), actor] as const),
  );
  const routineDefinitionsByKey = new Map(
    routineDefinitions.map((routine) => [buildRoutineKey(routine), routine] as const),
  );
  const helperDefinitionsByKey = new Map(
    helperDefinitions.map((helper) => [buildHelperKey(helper), helper] as const),
  );
  const globalDefinitionsByKey = new Map(
    globalDefinitions.map((globalDefinition) => [
      buildGlobalKey(globalDefinition),
      globalDefinition,
    ] as const),
  );

  const routingTaskKeys = new Set<string>();
  const routingSignalNames = new Set<string>();
  const routingIntentNames = new Set<string>();

  const publishedSignalTaskMaps = Array.from(signalTaskMaps.values())
    .filter((map) => {
      const signal = signalDefinitionsByName.get(map.signal_name);
      return signal?.is_meta !== true || (signal ? isRoutingCriticalMetaSignal(signal) : false);
    })
    .sort((left, right) =>
      `${left.signal_name}:${left.task_name}`.localeCompare(
        `${right.signal_name}:${right.task_name}`,
      ),
    );

  const publishedIntentTaskMaps = Array.from(intentTaskMaps.values())
    .filter((map) => {
      const intent = intentDefinitionsByName.get(map.intent_name);
      return intent?.is_meta !== true || (intent ? isRoutingCriticalMetaIntent(intent) : false);
    })
    .sort((left, right) =>
      `${left.intent_name}:${left.task_name}`.localeCompare(
        `${right.intent_name}:${right.task_name}`,
      ),
    );

  const localMetaSignalTaskMaps = Array.from(signalTaskMaps.values())
    .filter((map) => !publishedSignalTaskMaps.includes(map))
    .sort((left, right) =>
      `${left.signal_name}:${left.task_name}`.localeCompare(
        `${right.signal_name}:${right.task_name}`,
      ),
    );

  const localMetaIntentTaskMaps = Array.from(intentTaskMaps.values())
    .filter((map) => !publishedIntentTaskMaps.includes(map))
    .sort((left, right) =>
      `${left.intent_name}:${left.task_name}`.localeCompare(
        `${right.intent_name}:${right.task_name}`,
      ),
    );

  for (const map of publishedSignalTaskMaps) {
    routingTaskKeys.add(
      buildTaskKey({
        service_name: map.service_name,
        name: map.task_name,
        version: map.task_version,
      }),
    );
    routingSignalNames.add(map.signal_name);
  }

  for (const map of publishedIntentTaskMaps) {
    routingTaskKeys.add(
      buildTaskKey({
        service_name: map.service_name,
        name: map.task_name,
        version: map.task_version,
      }),
    );
    routingIntentNames.add(map.intent_name);
  }

  const routingTasks = Array.from(routingTaskKeys)
    .map((key) => taskDefinitionsByKey.get(key) ?? null)
    .filter((task): task is ServiceManifestTaskDefinition => task !== null)
    .sort((left, right) =>
      `${left.name}:${left.version}`.localeCompare(`${right.name}:${right.version}`),
    );
  const routingSignals = Array.from(routingSignalNames)
    .map((name) => signalDefinitionsByName.get(name) ?? null)
    .filter((signal): signal is ServiceManifestSignalDefinition => signal !== null)
    .sort((left, right) => left.name.localeCompare(right.name));
  const routingIntents = Array.from(routingIntentNames)
    .map((name) => intentDefinitionsByName.get(name) ?? null)
    .filter((intent): intent is ServiceManifestIntentDefinition => intent !== null)
    .sort((left, right) => left.name.localeCompare(right.name));

  const businessTasks = taskDefinitions
    .filter((task) => task.is_meta !== true && task.is_sub_meta !== true)
    .sort((left, right) =>
      `${left.name}:${left.version}`.localeCompare(`${right.name}:${right.version}`),
    );
  const businessSignals = Array.from(signalDefinitions.values())
    .filter((signal) => signal.is_meta !== true)
    .sort((left, right) => left.name.localeCompare(right.name));
  const businessIntents = intentDefinitions
    .filter((intent) => intent.is_meta !== true)
    .sort((left, right) => left.name.localeCompare(right.name));
  const businessActors = actorDefinitions
    .filter((actor) => actor.is_meta !== true)
    .sort((left, right) =>
      `${left.name}:${left.version}`.localeCompare(`${right.name}:${right.version}`),
    );
  const businessRoutines = routineDefinitions
    .filter((routine) => routine.is_meta !== true)
    .sort((left, right) =>
      `${left.name}:${left.version}`.localeCompare(`${right.name}:${right.version}`),
    );
  const businessDirectionalTaskMaps = Array.from(directionalTaskMaps.values())
    .filter((map) => {
      const predecessor = taskDefinitionsByKey.get(
        buildTaskKey({
          service_name: map.predecessor_service_name,
          name: map.predecessor_task_name,
          version: map.predecessor_task_version,
        }),
      );
      const task = taskDefinitionsByKey.get(
        buildTaskKey({
          service_name: map.service_name,
          name: map.task_name,
          version: map.task_version,
        }),
      );
      return (
        predecessor?.is_meta !== true &&
        predecessor?.is_sub_meta !== true &&
        task?.is_meta !== true &&
        task?.is_sub_meta !== true
      );
    })
    .sort((left, right) =>
      `${left.predecessor_task_name}:${left.task_name}`.localeCompare(
        `${right.predecessor_task_name}:${right.task_name}`,
      ),
    );
  const businessActorTaskMaps = Array.from(actorTaskMaps.values())
    .filter((map) => {
      const actor = actorDefinitionsByKey.get(
        buildActorKey({
          service_name: map.service_name,
          name: map.actor_name,
          version: map.actor_version,
        }),
      );
      const task = taskDefinitionsByKey.get(
        buildTaskKey({
          service_name: map.service_name,
          name: map.task_name,
          version: map.task_version,
        }),
      );
      return actor?.is_meta !== true && task?.is_meta !== true && task?.is_sub_meta !== true;
    })
    .sort((left, right) =>
      `${left.actor_name}:${left.task_name}`.localeCompare(
        `${right.actor_name}:${right.task_name}`,
      ),
    );
  const businessTaskToRoutineMaps = Array.from(taskToRoutineMaps.values())
    .filter((map) => {
      const routine = routineDefinitionsByKey.get(
        buildRoutineKey({
          service_name: map.service_name,
          name: map.routine_name,
          version: map.routine_version,
        }),
      );
      const task = taskDefinitionsByKey.get(
        buildTaskKey({
          service_name: map.service_name,
          name: map.task_name,
          version: map.task_version,
        }),
      );
      return routine?.is_meta !== true && task?.is_meta !== true && task?.is_sub_meta !== true;
    })
    .sort((left, right) =>
      `${left.routine_name}:${left.task_name}`.localeCompare(
        `${right.routine_name}:${right.task_name}`,
      ),
    );
  const localMetaTasks = taskDefinitions
    .filter((task) => task.is_meta === true || task.is_sub_meta === true)
    .sort((left, right) =>
      `${left.name}:${left.version}`.localeCompare(`${right.name}:${right.version}`),
    );
  const localMetaSignals = Array.from(signalDefinitions.values())
    .filter((signal) => signal.is_meta === true)
    .sort((left, right) => left.name.localeCompare(right.name));
  const localMetaIntents = intentDefinitions
    .filter((intent) => intent.is_meta === true)
    .sort((left, right) => left.name.localeCompare(right.name));
  const localMetaActors = actorDefinitions
    .filter((actor) => actor.is_meta === true)
    .sort((left, right) =>
      `${left.name}:${left.version}`.localeCompare(`${right.name}:${right.version}`),
    );
  const localMetaRoutines = routineDefinitions
    .filter((routine) => routine.is_meta === true)
    .sort((left, right) =>
      `${left.name}:${left.version}`.localeCompare(`${right.name}:${right.version}`),
    );
  const localMetaDirectionalTaskMaps = Array.from(directionalTaskMaps.values())
    .filter((map) => !businessDirectionalTaskMaps.includes(map))
    .sort((left, right) =>
      `${left.predecessor_task_name}:${left.task_name}`.localeCompare(
        `${right.predecessor_task_name}:${right.task_name}`,
      ),
    );
  const localMetaActorTaskMaps = Array.from(actorTaskMaps.values())
    .filter((map) => !businessActorTaskMaps.includes(map))
    .sort((left, right) =>
      `${left.actor_name}:${left.task_name}`.localeCompare(
        `${right.actor_name}:${right.task_name}`,
      ),
    );
  const localMetaTaskToRoutineMaps = Array.from(taskToRoutineMaps.values())
    .filter((map) => !businessTaskToRoutineMaps.includes(map))
    .sort((left, right) =>
      `${left.routine_name}:${left.task_name}`.localeCompare(
        `${right.routine_name}:${right.task_name}`,
      ),
    );

  const businessLocalMetaTaskKeys = new Set<string>();
  const businessLocalMetaSignalNames = new Set<string>();
  const businessLocalMetaIntentNames = new Set<string>();
  const businessLocalMetaActorKeys = new Set<string>();
  const businessLocalMetaRoutineKeys = new Set<string>();

  for (const map of localMetaSignalTaskMaps) {
    businessLocalMetaSignalNames.add(map.signal_name);
    businessLocalMetaTaskKeys.add(
      buildTaskKey({
        service_name: map.service_name,
        name: map.task_name,
        version: map.task_version,
      }),
    );
  }

  for (const map of localMetaIntentTaskMaps) {
    businessLocalMetaIntentNames.add(map.intent_name);
    businessLocalMetaTaskKeys.add(
      buildTaskKey({
        service_name: map.service_name,
        name: map.task_name,
        version: map.task_version,
      }),
    );
  }

  for (const map of localMetaActorTaskMaps) {
    businessLocalMetaActorKeys.add(
      buildActorKey({
        service_name: map.service_name,
        name: map.actor_name,
        version: map.actor_version,
      }),
    );
    businessLocalMetaTaskKeys.add(
      buildTaskKey({
        service_name: map.service_name,
        name: map.task_name,
        version: map.task_version,
      }),
    );
  }

  for (const map of localMetaTaskToRoutineMaps) {
    businessLocalMetaRoutineKeys.add(
      buildRoutineKey({
        service_name: map.service_name,
        name: map.routine_name,
        version: map.routine_version,
      }),
    );
    businessLocalMetaTaskKeys.add(
      buildTaskKey({
        service_name: map.service_name,
        name: map.task_name,
        version: map.task_version,
      }),
    );
  }

  const businessLocalMetaTasks = Array.from(businessLocalMetaTaskKeys)
    .map((key) => taskDefinitionsByKey.get(key) ?? null)
    .filter(
      (task): task is ServiceManifestTaskDefinition =>
        task !== null && (task.is_meta === true || task.is_sub_meta === true),
    )
    .sort((left, right) =>
      `${left.name}:${left.version}`.localeCompare(`${right.name}:${right.version}`),
    );
  const businessLocalMetaSignals = Array.from(businessLocalMetaSignalNames)
    .map((name) => signalDefinitionsByName.get(name) ?? null)
    .filter(
      (signal): signal is ServiceManifestSignalDefinition =>
        signal !== null && signal.is_meta === true,
    )
    .sort((left, right) => left.name.localeCompare(right.name));
  const businessLocalMetaIntents = Array.from(businessLocalMetaIntentNames)
    .map((name) => intentDefinitionsByName.get(name) ?? null)
    .filter(
      (intent): intent is ServiceManifestIntentDefinition =>
        intent !== null && intent.is_meta === true,
    )
    .sort((left, right) => left.name.localeCompare(right.name));
  const businessLocalMetaActors = Array.from(businessLocalMetaActorKeys)
    .map((key) => actorDefinitionsByKey.get(key) ?? null)
    .filter(
      (actor): actor is ServiceManifestActorDefinition =>
        actor !== null && actor.is_meta === true,
    )
    .sort((left, right) =>
      `${left.name}:${left.version}`.localeCompare(`${right.name}:${right.version}`),
    );
  const businessLocalMetaRoutines = Array.from(businessLocalMetaRoutineKeys)
    .map((key) => routineDefinitionsByKey.get(key) ?? null)
    .filter(
      (routine): routine is ServiceManifestRoutineDefinition =>
        routine !== null && routine.is_meta === true,
    )
    .sort((left, right) =>
      `${left.name}:${left.version}`.localeCompare(`${right.name}:${right.version}`),
    );

  const cumulativeTasks =
    publicationLayer === "routing_capability"
      ? routingTasks
      : publicationLayer === "business_structural"
        ? Array.from(
            new Map(
              [...routingTasks, ...businessTasks, ...businessLocalMetaTasks].map((task) => [
                buildTaskKey(task),
                task,
              ]),
            ).values(),
          ).sort((left, right) =>
            `${left.name}:${left.version}`.localeCompare(`${right.name}:${right.version}`),
          )
        : Array.from(
          new Map(
            [...routingTasks, ...businessTasks, ...localMetaTasks].map((task) => [
              buildTaskKey(task),
              task,
            ]),
          ).values(),
        ).sort((left, right) =>
          `${left.name}:${left.version}`.localeCompare(`${right.name}:${right.version}`),
        );
  const cumulativeSignals =
    publicationLayer === "routing_capability"
      ? routingSignals
      : publicationLayer === "business_structural"
        ? Array.from(
            new Map(
              [...routingSignals, ...businessSignals, ...businessLocalMetaSignals].map(
                (signal) => [signal.name, signal],
              ),
            ).values(),
          ).sort((left, right) => left.name.localeCompare(right.name))
        : Array.from(
          new Map(
            [...routingSignals, ...businessSignals, ...localMetaSignals].map((signal) => [
              signal.name,
              signal,
            ]),
          ).values(),
        ).sort((left, right) => left.name.localeCompare(right.name));
  const cumulativeIntents =
    publicationLayer === "routing_capability"
      ? routingIntents
      : publicationLayer === "business_structural"
        ? Array.from(
            new Map(
              [...routingIntents, ...businessIntents, ...businessLocalMetaIntents].map(
                (intent) => [intent.name, intent],
              ),
            ).values(),
          ).sort((left, right) => left.name.localeCompare(right.name))
        : Array.from(
          new Map(
            [...routingIntents, ...businessIntents, ...localMetaIntents].map((intent) => [
              intent.name,
              intent,
            ]),
          ).values(),
        ).sort((left, right) => left.name.localeCompare(right.name));
  const cumulativeActors =
    publicationLayer === "routing_capability"
      ? []
      : publicationLayer === "business_structural"
        ? Array.from(
            new Map(
              [...businessActors, ...businessLocalMetaActors].map((actor) => [
                buildActorKey(actor),
                actor,
              ]),
            ).values(),
          ).sort((left, right) =>
            `${left.name}:${left.version}`.localeCompare(`${right.name}:${right.version}`),
          )
        : Array.from(
            new Map(
              [...businessActors, ...localMetaActors].map((actor) => [
                buildActorKey(actor),
                actor,
              ]),
            ).values(),
          ).sort((left, right) =>
            `${left.name}:${left.version}`.localeCompare(`${right.name}:${right.version}`),
          );
  const cumulativeRoutines =
    publicationLayer === "routing_capability"
      ? []
      : publicationLayer === "business_structural"
        ? Array.from(
            new Map(
              [...businessRoutines, ...businessLocalMetaRoutines].map((routine) => [
                buildRoutineKey(routine),
                routine,
              ]),
            ).values(),
          ).sort((left, right) =>
            `${left.name}:${left.version}`.localeCompare(`${right.name}:${right.version}`),
          )
        : Array.from(
            new Map(
              [...businessRoutines, ...localMetaRoutines].map((routine) => [
                buildRoutineKey(routine),
                routine,
              ]),
            ).values(),
          ).sort((left, right) =>
            `${left.name}:${left.version}`.localeCompare(`${right.name}:${right.version}`),
          );
  const businessHelpers = helperDefinitions
    .filter((helper) => helper.is_meta !== true)
    .sort((left, right) =>
      `${left.name}:${left.version}`.localeCompare(`${right.name}:${right.version}`),
    );
  const localMetaHelpers = helperDefinitions
    .filter((helper) => helper.is_meta === true)
    .sort((left, right) =>
      `${left.name}:${left.version}`.localeCompare(`${right.name}:${right.version}`),
    );
  const businessGlobals = globalDefinitions
    .filter((globalDefinition) => globalDefinition.is_meta !== true)
    .sort((left, right) =>
      `${left.name}:${left.version}`.localeCompare(`${right.name}:${right.version}`),
    );
  const localMetaGlobals = globalDefinitions
    .filter((globalDefinition) => globalDefinition.is_meta === true)
    .sort((left, right) =>
      `${left.name}:${left.version}`.localeCompare(`${right.name}:${right.version}`),
    );
  const businessTaskToHelperMaps = Array.from(helperTaskMaps.values())
    .filter((map) => {
      const task = taskDefinitionsByKey.get(
        buildTaskKey({
          service_name: map.service_name,
          name: map.task_name,
          version: map.task_version,
        }),
      );
      const helper = helperDefinitionsByKey.get(
        buildHelperKey({
          service_name: map.service_name,
          name: map.helper_name,
          version: map.helper_version,
        }),
      );
      return task?.is_meta !== true && task?.is_sub_meta !== true && helper?.is_meta !== true;
    })
    .sort((left, right) =>
      `${left.task_name}:${left.alias}`.localeCompare(`${right.task_name}:${right.alias}`),
    );
  const localMetaTaskToHelperMaps = Array.from(helperTaskMaps.values())
    .filter((map) => !businessTaskToHelperMaps.includes(map))
    .sort((left, right) =>
      `${left.task_name}:${left.alias}`.localeCompare(`${right.task_name}:${right.alias}`),
    );
  const businessHelperToHelperMaps = Array.from(helperHelperMaps.values())
    .filter((map) => {
      const helper = helperDefinitionsByKey.get(
        buildHelperKey({
          service_name: map.service_name,
          name: map.helper_name,
          version: map.helper_version,
        }),
      );
      const dependencyHelper = helperDefinitionsByKey.get(
        buildHelperKey({
          service_name: map.service_name,
          name: map.dependency_helper_name,
          version: map.dependency_helper_version,
        }),
      );
      return helper?.is_meta !== true && dependencyHelper?.is_meta !== true;
    })
    .sort((left, right) =>
      `${left.helper_name}:${left.alias}`.localeCompare(`${right.helper_name}:${right.alias}`),
    );
  const localMetaHelperToHelperMaps = Array.from(helperHelperMaps.values())
    .filter((map) => !businessHelperToHelperMaps.includes(map))
    .sort((left, right) =>
      `${left.helper_name}:${left.alias}`.localeCompare(`${right.helper_name}:${right.alias}`),
    );
  const businessTaskToGlobalMaps = Array.from(taskGlobalMaps.values())
    .filter((map) => {
      const task = taskDefinitionsByKey.get(
        buildTaskKey({
          service_name: map.service_name,
          name: map.task_name,
          version: map.task_version,
        }),
      );
      const globalDefinition = globalDefinitionsByKey.get(
        buildGlobalKey({
          service_name: map.service_name,
          name: map.global_name,
          version: map.global_version,
        }),
      );
      return task?.is_meta !== true && task?.is_sub_meta !== true && globalDefinition?.is_meta !== true;
    })
    .sort((left, right) =>
      `${left.task_name}:${left.alias}`.localeCompare(`${right.task_name}:${right.alias}`),
    );
  const localMetaTaskToGlobalMaps = Array.from(taskGlobalMaps.values())
    .filter((map) => !businessTaskToGlobalMaps.includes(map))
    .sort((left, right) =>
      `${left.task_name}:${left.alias}`.localeCompare(`${right.task_name}:${right.alias}`),
    );
  const businessHelperToGlobalMaps = Array.from(helperGlobalMaps.values())
    .filter((map) => {
      const helper = helperDefinitionsByKey.get(
        buildHelperKey({
          service_name: map.service_name,
          name: map.helper_name,
          version: map.helper_version,
        }),
      );
      const globalDefinition = globalDefinitionsByKey.get(
        buildGlobalKey({
          service_name: map.service_name,
          name: map.global_name,
          version: map.global_version,
        }),
      );
      return helper?.is_meta !== true && globalDefinition?.is_meta !== true;
    })
    .sort((left, right) =>
      `${left.helper_name}:${left.alias}`.localeCompare(`${right.helper_name}:${right.alias}`),
    );
  const localMetaHelperToGlobalMaps = Array.from(helperGlobalMaps.values())
    .filter((map) => !businessHelperToGlobalMaps.includes(map))
    .sort((left, right) =>
      `${left.helper_name}:${left.alias}`.localeCompare(`${right.helper_name}:${right.alias}`),
    );
  const cumulativeHelpers =
    publicationLayer === "routing_capability"
      ? []
      : publicationLayer === "business_structural"
        ? businessHelpers
        : [...businessHelpers, ...localMetaHelpers];
  const cumulativeGlobals =
    publicationLayer === "routing_capability"
      ? []
      : publicationLayer === "business_structural"
        ? businessGlobals
        : [...businessGlobals, ...localMetaGlobals];
  const cumulativeTaskToHelperMaps =
    publicationLayer === "routing_capability"
      ? []
      : publicationLayer === "business_structural"
        ? businessTaskToHelperMaps
        : [...businessTaskToHelperMaps, ...localMetaTaskToHelperMaps];
  const cumulativeHelperToHelperMaps =
    publicationLayer === "routing_capability"
      ? []
      : publicationLayer === "business_structural"
        ? businessHelperToHelperMaps
        : [...businessHelperToHelperMaps, ...localMetaHelperToHelperMaps];
  const cumulativeTaskToGlobalMaps =
    publicationLayer === "routing_capability"
      ? []
      : publicationLayer === "business_structural"
        ? businessTaskToGlobalMaps
        : [...businessTaskToGlobalMaps, ...localMetaTaskToGlobalMaps];
  const cumulativeHelperToGlobalMaps =
    publicationLayer === "routing_capability"
      ? []
      : publicationLayer === "business_structural"
        ? businessHelperToGlobalMaps
        : [...businessHelperToGlobalMaps, ...localMetaHelperToGlobalMaps];
  const cumulativeDirectionalTaskMaps =
    publicationLayer === "routing_capability"
      ? []
      : publicationLayer === "business_structural"
        ? businessDirectionalTaskMaps
        : Array.from(
            new Map(
              [...businessDirectionalTaskMaps, ...localMetaDirectionalTaskMaps].map(
                (map) => [
                  `${map.predecessor_service_name}|${map.predecessor_task_name}|${map.predecessor_task_version}|${map.service_name}|${map.task_name}|${map.task_version}`,
                  map,
                ],
              ),
            ).values(),
          );
  const cumulativeActorTaskMaps =
    publicationLayer === "routing_capability"
      ? []
      : publicationLayer === "business_structural"
        ? Array.from(
            new Map(
              [...businessActorTaskMaps, ...localMetaActorTaskMaps].map((map) => [
                `${map.service_name}|${map.actor_name}|${map.actor_version}|${map.task_name}|${map.task_version}|${map.mode}`,
                map,
              ]),
            ).values(),
          )
        : Array.from(
            new Map(
              [...businessActorTaskMaps, ...localMetaActorTaskMaps].map((map) => [
                `${map.service_name}|${map.actor_name}|${map.actor_version}|${map.task_name}|${map.task_version}|${map.mode}`,
                map,
              ]),
            ).values(),
          );
  const cumulativeTaskToRoutineMaps =
    publicationLayer === "routing_capability"
      ? []
      : publicationLayer === "business_structural"
        ? businessTaskToRoutineMaps
        : Array.from(
            new Map(
              [...businessTaskToRoutineMaps, ...localMetaTaskToRoutineMaps].map((map) => [
                `${map.service_name}|${map.task_name}|${map.task_version}|${map.routine_name}|${map.routine_version}`,
                map,
              ]),
            ).values(),
          );

  const manifestBody = {
    serviceName,
    serviceInstanceId,
    publicationLayer,
    tasks: cumulativeTasks,
    signals: cumulativeSignals,
    intents: cumulativeIntents,
    actors: cumulativeActors,
    routines: cumulativeRoutines,
    helpers: cumulativeHelpers,
    globals: cumulativeGlobals,
    directionalTaskMaps: cumulativeDirectionalTaskMaps,
    signalToTaskMaps:
      publicationLayer === "routing_capability"
        ? publishedSignalTaskMaps
        : publicationLayer === "local_meta_structural"
        ? [...publishedSignalTaskMaps, ...localMetaSignalTaskMaps]
        : [...publishedSignalTaskMaps, ...localMetaSignalTaskMaps],
    intentToTaskMaps:
      publicationLayer === "routing_capability"
        ? publishedIntentTaskMaps
        : publicationLayer === "local_meta_structural"
        ? [...publishedIntentTaskMaps, ...localMetaIntentTaskMaps]
        : [...publishedIntentTaskMaps, ...localMetaIntentTaskMaps],
    actorTaskMaps: cumulativeActorTaskMaps,
    taskToRoutineMaps: cumulativeTaskToRoutineMaps,
    taskToHelperMaps: cumulativeTaskToHelperMaps,
    helperToHelperMaps: cumulativeHelperToHelperMaps,
    taskToGlobalMaps: cumulativeTaskToGlobalMaps,
    helperToGlobalMaps: cumulativeHelperToGlobalMaps,
  };

  return {
    ...manifestBody,
    revision,
    manifestHash: hashManifest(manifestBody),
    publishedAt,
  };
}

export function explodeServiceManifestSnapshots(
  snapshots: ServiceManifestSnapshot[],
): {
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
} {
  const dedupe = <T>(
    items: T[],
    keyOf: (item: T) => string,
    sortOf: (left: T, right: T) => number,
  ): T[] =>
    Array.from(
      new Map(items.map((item) => [keyOf(item), item] as const)).values(),
    ).sort(sortOf);

  const tasks = dedupe(
    snapshots.flatMap((snapshot) => snapshot.tasks),
    (task) => `${task.service_name}|${task.name}|${task.version}`,
    (left, right) =>
      `${left.service_name}|${left.name}|${left.version}`.localeCompare(
        `${right.service_name}|${right.name}|${right.version}`,
      ),
  );
  const signals = dedupe(
    snapshots.flatMap((snapshot) => snapshot.signals),
    (signal) => signal.name,
    (left, right) => left.name.localeCompare(right.name),
  );
  const intents = dedupe(
    snapshots.flatMap((snapshot) => snapshot.intents),
    (intent) => intent.name,
    (left, right) => left.name.localeCompare(right.name),
  );
  const actors = dedupe(
    snapshots.flatMap((snapshot) => snapshot.actors),
    (actor) => `${actor.service_name}|${actor.name}|${actor.version}`,
    (left, right) =>
      `${left.service_name}|${left.name}|${left.version}`.localeCompare(
        `${right.service_name}|${right.name}|${right.version}`,
      ),
  );
  const routines = dedupe(
    snapshots.flatMap((snapshot) => snapshot.routines),
    (routine) => `${routine.service_name}|${routine.name}|${routine.version}`,
    (left, right) =>
      `${left.service_name}|${left.name}|${left.version}`.localeCompare(
        `${right.service_name}|${right.name}|${right.version}`,
      ),
  );
  const helpers = dedupe(
    snapshots.flatMap((snapshot) => snapshot.helpers),
    (helper) => `${helper.service_name}|${helper.name}|${helper.version}`,
    (left, right) =>
      `${left.service_name}|${left.name}|${left.version}`.localeCompare(
        `${right.service_name}|${right.name}|${right.version}`,
      ),
  );
  const globals = dedupe(
    snapshots.flatMap((snapshot) => snapshot.globals),
    (globalDefinition) =>
      `${globalDefinition.service_name}|${globalDefinition.name}|${globalDefinition.version}`,
    (left, right) =>
      `${left.service_name}|${left.name}|${left.version}`.localeCompare(
        `${right.service_name}|${right.name}|${right.version}`,
      ),
  );
  const directionalTaskMaps = dedupe(
    snapshots.flatMap((snapshot) => snapshot.directionalTaskMaps),
    (map) =>
      `${map.predecessor_service_name}|${map.predecessor_task_name}|${map.predecessor_task_version}|${map.service_name}|${map.task_name}|${map.task_version}`,
    (left, right) =>
      `${left.predecessor_service_name}|${left.predecessor_task_name}|${left.predecessor_task_version}|${left.service_name}|${left.task_name}|${left.task_version}`.localeCompare(
        `${right.predecessor_service_name}|${right.predecessor_task_name}|${right.predecessor_task_version}|${right.service_name}|${right.task_name}|${right.task_version}`,
      ),
  );
  const signalToTaskMaps = dedupe(
    snapshots.flatMap((snapshot) => snapshot.signalToTaskMaps),
    (map) =>
      `${map.signal_name}|${map.service_name}|${map.task_name}|${map.task_version}`,
    (left, right) =>
      `${left.signal_name}|${left.service_name}|${left.task_name}|${left.task_version}`.localeCompare(
        `${right.signal_name}|${right.service_name}|${right.task_name}|${right.task_version}`,
      ),
  );
  const intentToTaskMaps = dedupe(
    snapshots.flatMap((snapshot) => snapshot.intentToTaskMaps),
    (map) =>
      `${map.intent_name}|${map.service_name}|${map.task_name}|${map.task_version}`,
    (left, right) =>
      `${left.intent_name}|${left.service_name}|${left.task_name}|${left.task_version}`.localeCompare(
        `${right.intent_name}|${right.service_name}|${right.task_name}|${right.task_version}`,
      ),
  );
  const actorTaskMaps = dedupe(
    snapshots.flatMap((snapshot) => snapshot.actorTaskMaps),
    (map) =>
      `${map.actor_name}|${map.actor_version}|${map.service_name}|${map.task_name}|${map.task_version}`,
    (left, right) =>
      `${left.actor_name}|${left.actor_version}|${left.service_name}|${left.task_name}|${left.task_version}`.localeCompare(
        `${right.actor_name}|${right.actor_version}|${right.service_name}|${right.task_name}|${right.task_version}`,
      ),
  );
  const taskToRoutineMaps = dedupe(
    snapshots.flatMap((snapshot) => snapshot.taskToRoutineMaps),
    (map) =>
      `${map.routine_name}|${map.routine_version}|${map.service_name}|${map.task_name}|${map.task_version}`,
    (left, right) =>
      `${left.routine_name}|${left.routine_version}|${left.service_name}|${left.task_name}|${left.task_version}`.localeCompare(
        `${right.routine_name}|${right.routine_version}|${right.service_name}|${right.task_name}|${right.task_version}`,
      ),
  );
  const taskToHelperMaps = dedupe(
    snapshots.flatMap((snapshot) => snapshot.taskToHelperMaps),
    (map) =>
      `${map.service_name}|${map.task_name}|${map.task_version}|${map.alias}|${map.helper_name}|${map.helper_version}`,
    (left, right) =>
      `${left.service_name}|${left.task_name}|${left.alias}|${left.helper_name}`.localeCompare(
        `${right.service_name}|${right.task_name}|${right.alias}|${right.helper_name}`,
      ),
  );
  const helperToHelperMaps = dedupe(
    snapshots.flatMap((snapshot) => snapshot.helperToHelperMaps),
    (map) =>
      `${map.service_name}|${map.helper_name}|${map.helper_version}|${map.alias}|${map.dependency_helper_name}|${map.dependency_helper_version}`,
    (left, right) =>
      `${left.service_name}|${left.helper_name}|${left.alias}|${left.dependency_helper_name}`.localeCompare(
        `${right.service_name}|${right.helper_name}|${right.alias}|${right.dependency_helper_name}`,
      ),
  );
  const taskToGlobalMaps = dedupe(
    snapshots.flatMap((snapshot) => snapshot.taskToGlobalMaps),
    (map) =>
      `${map.service_name}|${map.task_name}|${map.task_version}|${map.alias}|${map.global_name}|${map.global_version}`,
    (left, right) =>
      `${left.service_name}|${left.task_name}|${left.alias}|${left.global_name}`.localeCompare(
        `${right.service_name}|${right.task_name}|${right.alias}|${right.global_name}`,
      ),
  );
  const helperToGlobalMaps = dedupe(
    snapshots.flatMap((snapshot) => snapshot.helperToGlobalMaps),
    (map) =>
      `${map.service_name}|${map.helper_name}|${map.helper_version}|${map.alias}|${map.global_name}|${map.global_version}`,
    (left, right) =>
      `${left.service_name}|${left.helper_name}|${left.alias}|${left.global_name}`.localeCompare(
        `${right.service_name}|${right.helper_name}|${right.alias}|${right.global_name}`,
      ),
  );

  return {
    tasks,
    signals,
    intents,
    actors,
    routines,
    helpers,
    globals,
    directionalTaskMaps,
    signalToTaskMaps,
    intentToTaskMaps,
    actorTaskMaps,
    taskToRoutineMaps,
    taskToHelperMaps,
    helperToHelperMaps,
    taskToGlobalMaps,
    helperToGlobalMaps,
  };
}
