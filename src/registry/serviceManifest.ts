import Cadenza from "../Cadenza";
import { decomposeSignalName } from "../utils/tools";
import { isMetaIntentName } from "../utils/inquiry";
import type { Task, GraphRoutine } from "@cadenza.io/core";
import type {
  ServiceManifestActorDefinition,
  ServiceManifestActorTaskMap,
  ServiceManifestIntentDefinition,
  ServiceManifestIntentTaskMap,
  ServiceManifestRoutineDefinition,
  ServiceManifestSignalDefinition,
  ServiceManifestSignalTaskMap,
  ServiceManifestSnapshot,
  ServiceManifestTaskDefinition,
  ServiceManifestTaskRoutineMap,
  ServiceManifestDirectionalTaskMap,
} from "../types/serviceManifest";

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

function shouldExportTask(task: Task): boolean {
  return !task.isDeputy && !task.isEphemeral;
}

function shouldExportRoutine(routine: GraphRoutine): boolean {
  return Boolean(String(routine?.name ?? "").trim());
}

export function buildServiceManifestSnapshot(params: {
  serviceName: string;
  serviceInstanceId: string;
  revision: number;
  publishedAt: string;
}): ServiceManifestSnapshot {
  const { serviceName, serviceInstanceId, revision, publishedAt } = params;
  const tasks = Array.from(Cadenza.registry.tasks.values())
    .filter((task): task is Task => Boolean(task))
    .filter(shouldExportTask);
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

  const manifestBody = {
    serviceName,
    serviceInstanceId,
    tasks: taskDefinitions,
    signals: Array.from(signalDefinitions.values()).sort((left, right) =>
      left.name.localeCompare(right.name),
    ),
    intents: intentDefinitions,
    actors: actorDefinitions,
    routines: routineDefinitions,
    directionalTaskMaps: Array.from(directionalTaskMaps.values()).sort((left, right) =>
      `${left.predecessor_task_name}:${left.task_name}`.localeCompare(
        `${right.predecessor_task_name}:${right.task_name}`,
      ),
    ),
    signalToTaskMaps: Array.from(signalTaskMaps.values()).sort((left, right) =>
      `${left.signal_name}:${left.task_name}`.localeCompare(
        `${right.signal_name}:${right.task_name}`,
      ),
    ),
    intentToTaskMaps: Array.from(intentTaskMaps.values()).sort((left, right) =>
      `${left.intent_name}:${left.task_name}`.localeCompare(
        `${right.intent_name}:${right.task_name}`,
      ),
    ),
    actorTaskMaps: Array.from(actorTaskMaps.values()).sort((left, right) =>
      `${left.actor_name}:${left.task_name}`.localeCompare(
        `${right.actor_name}:${right.task_name}`,
      ),
    ),
    taskToRoutineMaps: Array.from(taskToRoutineMaps.values()).sort((left, right) =>
      `${left.routine_name}:${left.task_name}`.localeCompare(
        `${right.routine_name}:${right.task_name}`,
      ),
    ),
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
  directionalTaskMaps: ServiceManifestDirectionalTaskMap[];
  signalToTaskMaps: ServiceManifestSignalTaskMap[];
  intentToTaskMaps: ServiceManifestIntentTaskMap[];
  actorTaskMaps: ServiceManifestActorTaskMap[];
  taskToRoutineMaps: ServiceManifestTaskRoutineMap[];
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

  return {
    tasks,
    signals,
    intents,
    actors,
    routines,
    directionalTaskMaps,
    signalToTaskMaps,
    intentToTaskMaps,
    actorTaskMaps,
    taskToRoutineMaps,
  };
}
