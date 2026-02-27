import { GraphRoutine, Task } from "@cadenza.io/core";
import type { AnyObject } from "@cadenza.io/core";
import Cadenza from "../Cadenza";
import { isBrowser } from "../utils/environment";
import { InquiryResponderDescriptor } from "../types/inquiry";
import {
  isMetaIntentName,
  META_READINESS_INTENT,
  META_RUNTIME_STATUS_INTENT,
  META_RUNTIME_TRANSPORT_DIAGNOSTICS_INTENT,
} from "../utils/inquiry";
import {
  evaluateDependencyReadiness,
  resolveServiceReadinessState,
  summarizeDependencyReadiness,
  type DependencyReadinessState,
  type ReadinessState,
} from "../utils/readiness";
import {
  hasSignificantRuntimeStatusChange,
  resolveRuntimeStatus,
  runtimeStatusPriority,
  type RuntimeStatusSnapshot,
  type RuntimeStatusState,
} from "../utils/runtimeStatus";

const META_SERVICE_REGISTRY_FULL_SYNC_INTENT =
  "meta-service-registry-full-sync";
const META_RUNTIME_STATUS_HEARTBEAT_TICK_SIGNAL =
  "meta.service_registry.runtime_status.heartbeat_tick";
const META_RUNTIME_STATUS_MONITOR_TICK_SIGNAL =
  "meta.service_registry.runtime_status.monitor_tick";
const INTERNAL_RUNTIME_STATUS_TASK_NAMES = new Set([
  "Track local routine start",
  "Track local routine end",
  "Start runtime status sharing intervals",
  "Broadcast runtime status",
  "Monitor dependee heartbeat freshness",
  "Resolve runtime status fallback inquiry",
  "Respond runtime status inquiry",
  "Respond readiness inquiry",
  "Collect distributed readiness",
  "Get status",
]);

function readPositiveIntegerEnv(name: string, fallback: number): number {
  if (typeof process === "undefined") {
    return fallback;
  }

  const raw = process.env?.[name];
  const parsed = Number(raw);
  if (!Number.isFinite(parsed)) {
    return fallback;
  }

  const normalized = Math.trunc(parsed);
  if (normalized <= 0) {
    return fallback;
  }

  return normalized;
}

export interface ServiceInstanceDescriptor {
  uuid: string;
  address: string;
  port: number;
  serviceName: string;
  numberOfRunningGraphs?: number;
  isPrimary: boolean;
  isActive: boolean;
  isNonResponsive: boolean;
  isBlocked: boolean;
  runtimeState?: RuntimeStatusState;
  acceptingWork?: boolean;
  reportedAt?: string;
  health: AnyObject;
  exposed: boolean;
  clientCreated?: boolean;
  isFrontend: boolean;
}

export interface DeputyDescriptor {
  serviceName: string;
  remoteRoutineName?: string;
  signalName?: string;
  localTaskName: string;
  communicationType: string;
}

interface RemoteIntentDeputyDescriptor {
  key: string;
  intentName: string;
  serviceName: string;
  remoteTaskName: string;
  remoteTaskVersion: number;
  localTaskName: string;
  localTask: Task;
}

interface RuntimeStatusReport {
  serviceName: string;
  serviceInstanceId: string;
  serviceAddress?: string;
  servicePort?: number;
  exposed?: boolean;
  isFrontend?: boolean;
  reportedAt: string;
  state: RuntimeStatusState;
  acceptingWork: boolean;
  numberOfRunningGraphs: number;
  isActive: boolean;
  isNonResponsive: boolean;
  isBlocked: boolean;
  health?: AnyObject;
}

interface DependencyReadinessDetail {
  serviceName: string;
  serviceInstanceId: string;
  dependencyState: DependencyReadinessState;
  runtimeState: RuntimeStatusState;
  acceptingWork: boolean;
  missedHeartbeats: number;
  stale: boolean;
  blocked: boolean;
  reason:
    | "missing"
    | "heartbeat-timeout"
    | "heartbeat-stale"
    | "runtime-unavailable"
    | "runtime-overloaded"
    | "runtime-degraded"
    | "runtime-healthy";
  lastHeartbeatAt: string | null;
  reportedAt: string | null;
}

interface ReadinessReport {
  serviceName: string;
  serviceInstanceId: string;
  reportedAt: string;
  readinessState: ReadinessState;
  runtimeState: RuntimeStatusState;
  acceptingWork: boolean;
  dependencySummary: {
    total: number;
    ready: number;
    degraded: number;
    overloaded: number;
    unavailable: number;
    stale: number;
  };
  dependencies?: DependencyReadinessDetail[];
}

/**
 * The ServiceRegistry class is a singleton that manages the registration and lifecycle of
 * service instances, deputies, and remote signals in a distributed service architecture.
 * It handles various tasks such as instance updates, remote signal registration,
 * service status synchronization, and error/event broadcasting.
 */
export default class ServiceRegistry {
  private static _instance: ServiceRegistry;
  public static get instance(): ServiceRegistry {
    if (!this._instance) this._instance = new ServiceRegistry();
    return this._instance;
  }

  private instances: Map<string, ServiceInstanceDescriptor[]> = new Map();
  private deputies: Map<string, DeputyDescriptor[]> = new Map();
  private remoteSignals: Map<string, Set<string>> = new Map();
  private remoteIntents: Map<string, Set<string>> = new Map();
  private remoteIntentDeputiesByKey: Map<string, RemoteIntentDeputyDescriptor> =
    new Map();
  private remoteIntentDeputiesByTask: Map<Task, RemoteIntentDeputyDescriptor> =
    new Map();
  private dependeesByService: Map<string, Set<string>> = new Map();
  private dependeeByInstance: Map<string, string> = new Map();
  private readinessDependeesByService: Map<string, Set<string>> = new Map();
  private readinessDependeeByInstance: Map<string, string> = new Map();
  private lastHeartbeatAtByInstance: Map<string, number> = new Map();
  private missedHeartbeatsByInstance: Map<string, number> = new Map();
  private runtimeStatusFallbackInFlightByInstance: Set<string> = new Set();
  private activeRoutineExecutionIds: Set<string> = new Set();
  private runtimeStatusHeartbeatStarted = false;
  private lastRuntimeStatusSnapshot: RuntimeStatusSnapshot | null = null;
  private readonly runtimeStatusHeartbeatIntervalMs = readPositiveIntegerEnv(
    "CADENZA_RUNTIME_STATUS_HEARTBEAT_MS",
    30_000,
  );
  private readonly runtimeStatusMissThreshold = readPositiveIntegerEnv(
    "CADENZA_RUNTIME_STATUS_MISSED_HEARTBEATS",
    3,
  );
  private readonly runtimeStatusFallbackTimeoutMs = readPositiveIntegerEnv(
    "CADENZA_RUNTIME_STATUS_FALLBACK_TIMEOUT_MS",
    1_500,
  );
  private readonly degradedGraphThreshold = readPositiveIntegerEnv(
    "CADENZA_RUNTIME_STATUS_DEGRADED_GRAPH_THRESHOLD",
    10,
  );
  private readonly overloadedGraphThreshold = readPositiveIntegerEnv(
    "CADENZA_RUNTIME_STATUS_OVERLOADED_GRAPH_THRESHOLD",
    20,
  );
  serviceName: string | null = null;
  serviceInstanceId: string | null = null;
  numberOfRunningGraphs: number = 0;
  useSocket: boolean = false;
  retryCount: number = 3;

  handleInstanceUpdateTask: Task;
  handleGlobalSignalRegistrationTask: Task;
  handleGlobalIntentRegistrationTask: Task;
  handleSocketStatusUpdateTask: Task;
  fullSyncTask: GraphRoutine | Task;
  getAllInstances: Task;
  doForEachInstance: Task;
  deleteInstance: Task;
  getBalancedInstance: Task;
  getInstanceById: Task;
  getInstancesByServiceName: Task;
  handleDeputyRegistrationTask: Task;
  getStatusTask: Task;
  insertServiceTask: Task;
  insertServiceInstanceTask: Task;
  handleServiceNotRespondingTask: Task;
  handleServiceHandshakeTask: Task;
  collectTransportDiagnosticsTask: Task;
  collectReadinessTask: Task;

  private buildRemoteIntentDeputyKey(map: {
    intentName: string;
    serviceName: string;
    taskName: string;
    taskVersion?: number;
  }): string {
    return `${map.intentName}|${map.serviceName}|${map.taskName}|${map.taskVersion ?? 1}`;
  }

  private normalizeIntentMaps(ctx: AnyObject): Array<{
    intentName: string;
    serviceName: string;
    taskName: string;
    taskVersion: number;
    deleted?: boolean;
  }> {
    if (Array.isArray((ctx as any).intentToTaskMaps)) {
      return (ctx as any).intentToTaskMaps
        .map((m: any) => ({
          intentName: m.intentName ?? m.intent_name,
          serviceName: m.serviceName ?? m.service_name,
          taskName: m.taskName ?? m.task_name,
          taskVersion: m.taskVersion ?? m.task_version ?? 1,
          deleted: !!m.deleted,
        }))
        .filter((m: any) => m.intentName && m.serviceName && m.taskName);
    }

    const single =
      (ctx as any).intentToTaskMap ??
      (ctx as any).data ??
      ((ctx as any).intentName ? ctx : undefined);

    if (!single) return [];

    const normalized = {
      intentName: single.intentName ?? single.intent_name,
      serviceName: single.serviceName ?? single.service_name,
      taskName: single.taskName ?? single.task_name,
      taskVersion: single.taskVersion ?? single.task_version ?? 1,
      deleted: !!single.deleted,
    };

    if (!normalized.intentName || !normalized.serviceName || !normalized.taskName)
      return [];

    return [normalized];
  }

  private registerRemoteIntentDeputy(map: {
    intentName: string;
    serviceName: string;
    taskName: string;
    taskVersion: number;
  }) {
    if (!this.serviceName || map.serviceName === this.serviceName) {
      return;
    }

    const key = this.buildRemoteIntentDeputyKey(map);
    if (this.remoteIntentDeputiesByKey.has(key)) {
      return;
    }

    const deputyTaskName = `Inquire ${map.intentName} via ${map.serviceName} (${map.taskName} v${map.taskVersion})`;

    const deputyTask = isMetaIntentName(map.intentName)
      ? Cadenza.createMetaDeputyTask(map.taskName, map.serviceName, {
          register: false,
          isHidden: true,
          retryCount: 1,
          retryDelay: 50,
          retryDelayFactor: 1.2,
        })
      : Cadenza.createDeputyTask(map.taskName, map.serviceName, {
          register: false,
          isHidden: true,
          retryCount: 1,
          retryDelay: 50,
          retryDelayFactor: 1.2,
        });

    deputyTask.respondsTo(map.intentName);

    if (!this.remoteIntents.has(map.serviceName)) {
      this.remoteIntents.set(map.serviceName, new Set());
    }
    this.remoteIntents.get(map.serviceName)!.add(map.intentName);

    const descriptor: RemoteIntentDeputyDescriptor = {
      key,
      intentName: map.intentName,
      serviceName: map.serviceName,
      remoteTaskName: map.taskName,
      remoteTaskVersion: map.taskVersion,
      localTaskName: deputyTask.name || deputyTaskName,
      localTask: deputyTask,
    };

    this.remoteIntentDeputiesByKey.set(key, descriptor);
    this.remoteIntentDeputiesByTask.set(deputyTask, descriptor);
  }

  private unregisterRemoteIntentDeputy(map: {
    intentName: string;
    serviceName: string;
    taskName: string;
    taskVersion?: number;
  }) {
    const key = this.buildRemoteIntentDeputyKey(map);
    const descriptor = this.remoteIntentDeputiesByKey.get(key);
    if (!descriptor) {
      return;
    }

    const task = descriptor.localTask;
    if (task) {
      Cadenza.inquiryBroker.unsubscribe(descriptor.intentName, task);
      task.destroy();
    }

    this.remoteIntentDeputiesByTask.delete(descriptor.localTask);
    this.remoteIntentDeputiesByKey.delete(key);

    this.remoteIntents.get(descriptor.serviceName)?.delete(descriptor.intentName);
    if (!this.remoteIntents.get(descriptor.serviceName)?.size) {
      this.remoteIntents.delete(descriptor.serviceName);
    }

    const deputies = this.deputies.get(descriptor.serviceName);
    if (deputies) {
      this.deputies.set(
        descriptor.serviceName,
        deputies.filter((d) => d.localTaskName !== descriptor.localTaskName),
      );

      if (this.deputies.get(descriptor.serviceName)?.length === 0) {
        this.deputies.delete(descriptor.serviceName);
      }
    }
  }

  public getInquiryResponderDescriptor(task: Task): InquiryResponderDescriptor {
    const remote = this.remoteIntentDeputiesByTask.get(task);

    if (remote) {
      return {
        isRemote: true,
        serviceName: remote.serviceName,
        taskName: remote.remoteTaskName,
        taskVersion: remote.remoteTaskVersion,
        localTaskName: remote.localTaskName,
      };
    }

    return {
      isRemote: false,
      serviceName: this.serviceName ?? "UnknownService",
      taskName: task.name,
      taskVersion: task.version,
      localTaskName: task.name,
    };
  }

  private getInstance(serviceName: string, instanceId: string) {
    return this.instances
      .get(serviceName)
      ?.find((instance) => instance.uuid === instanceId);
  }

  private getLocalInstance() {
    if (!this.serviceName || !this.serviceInstanceId) {
      return undefined;
    }

    return this.getInstance(this.serviceName, this.serviceInstanceId);
  }

  private registerDependee(
    serviceName: string,
    serviceInstanceId: string,
    options: {
      requiredForReadiness?: boolean;
    } = {},
  ) {
    if (!serviceName || !serviceInstanceId) {
      return;
    }

    if (!this.dependeesByService.has(serviceName)) {
      this.dependeesByService.set(serviceName, new Set());
    }

    this.dependeesByService.get(serviceName)!.add(serviceInstanceId);
    this.dependeeByInstance.set(serviceInstanceId, serviceName);

    if (options.requiredForReadiness) {
      if (!this.readinessDependeesByService.has(serviceName)) {
        this.readinessDependeesByService.set(serviceName, new Set());
      }
      this.readinessDependeesByService.get(serviceName)!.add(serviceInstanceId);
      this.readinessDependeeByInstance.set(serviceInstanceId, serviceName);
    }

    this.lastHeartbeatAtByInstance.set(serviceInstanceId, Date.now());
    this.missedHeartbeatsByInstance.set(serviceInstanceId, 0);
  }

  private unregisterDependee(serviceInstanceId: string, serviceName?: string) {
    const dependeeServiceName =
      serviceName ?? this.dependeeByInstance.get(serviceInstanceId);
    if (dependeeServiceName) {
      this.dependeesByService.get(dependeeServiceName)?.delete(serviceInstanceId);
      if (!this.dependeesByService.get(dependeeServiceName)?.size) {
        this.dependeesByService.delete(dependeeServiceName);
      }
    }

    this.dependeeByInstance.delete(serviceInstanceId);
    const readinessDependeeServiceName =
      serviceName ?? this.readinessDependeeByInstance.get(serviceInstanceId);
    if (readinessDependeeServiceName) {
      this.readinessDependeesByService
        .get(readinessDependeeServiceName)
        ?.delete(serviceInstanceId);
      if (!this.readinessDependeesByService.get(readinessDependeeServiceName)?.size) {
        this.readinessDependeesByService.delete(readinessDependeeServiceName);
      }
    }

    this.readinessDependeeByInstance.delete(serviceInstanceId);
    this.lastHeartbeatAtByInstance.delete(serviceInstanceId);
    this.missedHeartbeatsByInstance.delete(serviceInstanceId);
    this.runtimeStatusFallbackInFlightByInstance.delete(serviceInstanceId);
  }

  private getHeartbeatMisses(serviceInstanceId: string, now = Date.now()): number {
    const observedMisses = this.missedHeartbeatsByInstance.get(serviceInstanceId) ?? 0;
    const lastHeartbeatAt = this.lastHeartbeatAtByInstance.get(serviceInstanceId) ?? 0;
    if (lastHeartbeatAt <= 0) {
      return Math.max(observedMisses, this.runtimeStatusMissThreshold);
    }

    const estimatedMisses = Math.max(
      0,
      Math.floor((now - lastHeartbeatAt) / this.runtimeStatusHeartbeatIntervalMs),
    );

    return Math.max(observedMisses, estimatedMisses);
  }

  private shouldRequireReadinessFromCommunicationTypes(
    communicationTypes: unknown,
  ): boolean {
    if (!Array.isArray(communicationTypes)) {
      return false;
    }

    return communicationTypes.some((type) => {
      const normalized = String(type).toLowerCase();
      return normalized === "delegation" || normalized === "inquiry";
    });
  }

  private resolveRuntimeStatusSnapshot(
    numberOfRunningGraphs: number,
    isActive: boolean,
    isNonResponsive: boolean,
    isBlocked: boolean,
  ): RuntimeStatusSnapshot {
    return resolveRuntimeStatus({
      numberOfRunningGraphs,
      isActive,
      isNonResponsive,
      isBlocked,
      degradedGraphThreshold: this.degradedGraphThreshold,
      overloadedGraphThreshold: this.overloadedGraphThreshold,
    });
  }

  private normalizeRuntimeStatusReport(ctx: AnyObject): RuntimeStatusReport | null {
    const serviceName =
      ctx.serviceName ?? ctx.__serviceName ?? ctx.serviceInstance?.serviceName;
    const serviceInstanceId =
      ctx.serviceInstanceId ??
      ctx.__serviceInstanceId ??
      ctx.serviceInstance?.uuid;
    if (!serviceName || !serviceInstanceId) {
      return null;
    }
    const servicePort = ctx.servicePort ?? ctx.port ?? ctx.serviceInstance?.port;

    const numberOfRunningGraphs = Math.max(
      0,
      Math.trunc(
        Number(ctx.numberOfRunningGraphs ?? ctx.__numberOfRunningGraphs ?? 0),
      ),
    );
    const isActive = Boolean(ctx.isActive ?? ctx.__active ?? true);
    const isNonResponsive = Boolean(ctx.isNonResponsive ?? false);
    const isBlocked = Boolean(ctx.isBlocked ?? false);

    const resolved = this.resolveRuntimeStatusSnapshot(
      numberOfRunningGraphs,
      isActive,
      isNonResponsive,
      isBlocked,
    );

    return {
      serviceName,
      serviceInstanceId,
      serviceAddress:
        ctx.serviceAddress ?? ctx.address ?? ctx.serviceInstance?.address,
      servicePort: typeof servicePort === "number" ? servicePort : undefined,
      exposed:
        typeof ctx.exposed === "boolean"
          ? ctx.exposed
          : typeof ctx.serviceInstance?.exposed === "boolean"
            ? ctx.serviceInstance.exposed
            : undefined,
      isFrontend:
        typeof ctx.isFrontend === "boolean"
          ? ctx.isFrontend
          : typeof ctx.serviceInstance?.isFrontend === "boolean"
            ? ctx.serviceInstance.isFrontend
          : undefined,
      reportedAt:
        ctx.reportedAt ??
        (typeof ctx.__reportedAt === "string" ? ctx.__reportedAt : undefined) ??
        new Date().toISOString(),
      state:
        ctx.state === "healthy" ||
        ctx.state === "degraded" ||
        ctx.state === "overloaded" ||
        ctx.state === "unavailable"
          ? ctx.state
          : resolved.state,
      acceptingWork:
        typeof ctx.acceptingWork === "boolean"
          ? ctx.acceptingWork
          : resolved.acceptingWork,
      numberOfRunningGraphs,
      isActive,
      isNonResponsive,
      isBlocked,
      health: (ctx.health ?? ctx.__health ?? {}) as AnyObject,
    };
  }

  private applyRuntimeStatusReport(report: RuntimeStatusReport): boolean {
    const instance = this.getInstance(report.serviceName, report.serviceInstanceId);
    if (!instance) {
      return false;
    }

    if (report.serviceAddress) {
      instance.address = report.serviceAddress;
    }

    if (typeof report.servicePort === "number") {
      instance.port = report.servicePort;
    }

    if (typeof report.exposed === "boolean") {
      instance.exposed = report.exposed;
    }

    if (typeof report.isFrontend === "boolean") {
      instance.isFrontend = report.isFrontend;
    }

    instance.numberOfRunningGraphs = report.numberOfRunningGraphs;
    instance.isActive = report.isActive;
    instance.isNonResponsive = report.isNonResponsive;
    instance.isBlocked = report.isBlocked;
    instance.runtimeState = report.state;
    instance.acceptingWork = report.acceptingWork;
    instance.reportedAt = report.reportedAt;
    instance.health = {
      ...(instance.health ?? {}),
      ...(report.health ?? {}),
      runtimeStatus: {
        state: report.state,
        acceptingWork: report.acceptingWork,
        reportedAt: report.reportedAt,
      },
    };

    return true;
  }

  private buildLocalRuntimeStatusReport(
    detailLevel: "minimal" | "full" = "minimal",
  ): RuntimeStatusReport | null {
    if (!this.serviceName || !this.serviceInstanceId) {
      return null;
    }

    const localInstance = this.getLocalInstance();
    if (!localInstance) {
      return null;
    }

    const numberOfRunningGraphs =
      this.activeRoutineExecutionIds.size || this.numberOfRunningGraphs || 0;
    this.numberOfRunningGraphs = numberOfRunningGraphs;

    const snapshot = this.resolveRuntimeStatusSnapshot(
      numberOfRunningGraphs,
      localInstance.isActive,
      localInstance.isNonResponsive,
      localInstance.isBlocked,
    );
    const reportedAt = new Date().toISOString();

    const report: RuntimeStatusReport = {
      serviceName: this.serviceName,
      serviceInstanceId: this.serviceInstanceId,
      serviceAddress: localInstance.address,
      servicePort: localInstance.port,
      exposed: localInstance.exposed,
      isFrontend: localInstance.isFrontend,
      reportedAt,
      state: snapshot.state,
      acceptingWork: snapshot.acceptingWork,
      numberOfRunningGraphs: snapshot.numberOfRunningGraphs,
      isActive: snapshot.isActive,
      isNonResponsive: snapshot.isNonResponsive,
      isBlocked: snapshot.isBlocked,
      health: {
        ...(localInstance.health ?? {}),
        runtimeStatus: {
          state: snapshot.state,
          acceptingWork: snapshot.acceptingWork,
          reportedAt,
        },
      },
    };

    this.applyRuntimeStatusReport(report);
    if (detailLevel !== "full") {
      delete report.health;
    }

    return report;
  }

  private selectRuntimeStatusReportForTarget(
    inquiryResult: AnyObject,
    targetServiceName: string,
    targetServiceInstanceId: string,
  ): RuntimeStatusReport | null {
    const reports = Array.isArray(inquiryResult.runtimeStatusReports)
      ? inquiryResult.runtimeStatusReports
      : [];

    for (const candidate of reports) {
      const report = this.normalizeRuntimeStatusReport(candidate);
      if (!report) {
        continue;
      }

      if (
        report.serviceName === targetServiceName &&
        report.serviceInstanceId === targetServiceInstanceId
      ) {
        return report;
      }
    }

    return null;
  }

  private async resolveRuntimeStatusFallbackInquiry(
    serviceName: string,
    serviceInstanceId: string,
    options: {
      detailLevel?: "minimal" | "full";
      overallTimeoutMs?: number;
      perResponderTimeoutMs?: number;
      requireComplete?: boolean;
    } = {},
  ): Promise<{ report: RuntimeStatusReport; inquiryMeta: AnyObject }> {
    const inquiryResult = await Cadenza.inquire(
      META_RUNTIME_STATUS_INTENT,
      {
        targetServiceName: serviceName,
        targetServiceInstanceId: serviceInstanceId,
        detailLevel: options.detailLevel ?? "minimal",
      },
      {
        overallTimeoutMs:
          options.overallTimeoutMs ?? this.runtimeStatusFallbackTimeoutMs,
        perResponderTimeoutMs:
          options.perResponderTimeoutMs ??
          Math.max(250, Math.floor(this.runtimeStatusFallbackTimeoutMs * 0.75)),
        requireComplete: options.requireComplete ?? false,
      },
    );

    const report = this.selectRuntimeStatusReportForTarget(
      inquiryResult,
      serviceName,
      serviceInstanceId,
    );

    if (!report) {
      throw new Error(
        `No runtime status report for ${serviceName}/${serviceInstanceId}`,
      );
    }

    if (!this.applyRuntimeStatusReport(report)) {
      throw new Error(
        `No tracked instance for runtime fallback ${serviceName}/${serviceInstanceId}`,
      );
    }

    this.lastHeartbeatAtByInstance.set(serviceInstanceId, Date.now());
    this.missedHeartbeatsByInstance.set(serviceInstanceId, 0);

    return {
      report,
      inquiryMeta: inquiryResult.__inquiryMeta ?? {},
    };
  }

  private evaluateDependencyReadinessDetail(
    serviceName: string,
    serviceInstanceId: string,
    now = Date.now(),
  ): DependencyReadinessDetail {
    const instance = this.getInstance(serviceName, serviceInstanceId);
    const missedHeartbeats = this.getHeartbeatMisses(serviceInstanceId, now);
    const runtimeState = instance
      ? (instance.runtimeState ??
        this.resolveRuntimeStatusSnapshot(
          instance.numberOfRunningGraphs ?? 0,
          instance.isActive,
          instance.isNonResponsive,
          instance.isBlocked,
        ).state)
      : "unavailable";
    const acceptingWork = instance
      ? (typeof instance.acceptingWork === "boolean"
        ? instance.acceptingWork
        : this.resolveRuntimeStatusSnapshot(
            instance.numberOfRunningGraphs ?? 0,
            instance.isActive,
            instance.isNonResponsive,
            instance.isBlocked,
          ).acceptingWork)
      : false;

    const evaluation = evaluateDependencyReadiness({
      exists: Boolean(instance),
      runtimeState,
      acceptingWork,
      missedHeartbeats,
      missThreshold: this.runtimeStatusMissThreshold,
    });

    const lastHeartbeat = this.lastHeartbeatAtByInstance.get(serviceInstanceId);
    return {
      serviceName,
      serviceInstanceId,
      dependencyState: evaluation.state,
      runtimeState,
      acceptingWork,
      missedHeartbeats,
      stale: evaluation.stale,
      blocked: evaluation.blocked,
      reason: evaluation.reason,
      lastHeartbeatAt: lastHeartbeat
        ? new Date(lastHeartbeat).toISOString()
        : null,
      reportedAt: instance?.reportedAt ?? null,
    };
  }

  private async buildLocalReadinessReport(
    options: {
      detailLevel?: "minimal" | "full";
      includeDependencies?: boolean;
      refreshStaleDependencies?: boolean;
    } = {},
  ): Promise<ReadinessReport | null> {
    const localRuntime = this.buildLocalRuntimeStatusReport("minimal");
    if (!localRuntime) {
      return null;
    }

    const detailLevel = options.detailLevel ?? "minimal";
    const includeDependencies =
      options.includeDependencies ?? detailLevel === "full";
    const refreshStaleDependencies = options.refreshStaleDependencies ?? true;
    const dependencyPairs = Array.from(this.readinessDependeesByService.entries())
      .flatMap(([serviceName, instanceIds]) =>
        Array.from(instanceIds).map((serviceInstanceId) => ({
          serviceName,
          serviceInstanceId,
        })),
      )
      .sort((left, right) => {
        if (left.serviceName !== right.serviceName) {
          return left.serviceName.localeCompare(right.serviceName);
        }
        return left.serviceInstanceId.localeCompare(right.serviceInstanceId);
      });

    if (refreshStaleDependencies) {
      for (const dependency of dependencyPairs) {
        const misses = this.getHeartbeatMisses(dependency.serviceInstanceId);
        if (misses < this.runtimeStatusMissThreshold) {
          continue;
        }

        if (
          this.runtimeStatusFallbackInFlightByInstance.has(
            dependency.serviceInstanceId,
          )
        ) {
          continue;
        }

        this.runtimeStatusFallbackInFlightByInstance.add(
          dependency.serviceInstanceId,
        );
        try {
          await this.resolveRuntimeStatusFallbackInquiry(
            dependency.serviceName,
            dependency.serviceInstanceId,
          );
        } catch (error) {
          Cadenza.log(
            "Readiness dependency fallback failed.",
            {
              serviceName: dependency.serviceName,
              serviceInstanceId: dependency.serviceInstanceId,
              error: error instanceof Error ? error.message : String(error),
            },
            "warning",
          );
        } finally {
          this.runtimeStatusFallbackInFlightByInstance.delete(
            dependency.serviceInstanceId,
          );
        }
      }
    }

    const now = Date.now();
    const dependencyDetails = dependencyPairs.map((dependency) =>
      this.evaluateDependencyReadinessDetail(
        dependency.serviceName,
        dependency.serviceInstanceId,
        now,
      ),
    );
    const dependencySummary = summarizeDependencyReadiness(
      dependencyDetails.map((detail) => ({
        state: detail.dependencyState,
        stale: detail.stale,
        blocked: detail.blocked,
        reason: detail.reason,
      })),
    );
    const readinessState = resolveServiceReadinessState(
      localRuntime.state,
      localRuntime.acceptingWork,
      dependencySummary,
    );

    return {
      serviceName: localRuntime.serviceName,
      serviceInstanceId: localRuntime.serviceInstanceId,
      reportedAt: new Date(now).toISOString(),
      readinessState,
      runtimeState: localRuntime.state,
      acceptingWork: localRuntime.acceptingWork,
      dependencySummary,
      ...(includeDependencies ? { dependencies: dependencyDetails } : {}),
    };
  }

  /**
   * Initializes a private constructor for managing service instances, remote signals,
   * service health, and handling updates or synchronization tasks. The constructor
   * creates a variety of meta tasks that process different lifecycle events and
   * service-related updates in a distributed service registry model.
   *
   * @return {Object} An instance of the constructed class with initialized tasks
   *                  and state management necessary to process service-related events.
   */
  private constructor() {
    Cadenza.defineIntent({
      name: META_RUNTIME_TRANSPORT_DIAGNOSTICS_INTENT,
      description:
        "Gather transport diagnostics across all services and communication clients.",
      input: {
        type: "object",
        properties: {
          detailLevel: {
            type: "string",
            constraints: {
              oneOf: ["summary", "full"],
            },
          },
          includeErrorHistory: {
            type: "boolean",
          },
          errorHistoryLimit: {
            type: "number",
            constraints: {
              min: 1,
              max: 200,
            },
          },
        },
      },
      output: {
        type: "object",
        properties: {
          transportDiagnostics: {
            type: "object",
          },
        },
      },
    });

    Cadenza.defineIntent({
      name: META_RUNTIME_STATUS_INTENT,
      description:
        "Gather lightweight runtime status reports from services in the distributed runtime.",
      input: {
        type: "object",
        properties: {
          detailLevel: {
            type: "string",
            constraints: {
              oneOf: ["minimal", "full"],
            },
          },
          targetServiceName: {
            type: "string",
          },
          targetServiceInstanceId: {
            type: "string",
          },
        },
      },
      output: {
        type: "object",
        properties: {
          runtimeStatusReports: {
            type: "array",
          },
        },
      },
    });

    Cadenza.createMetaTask(
      "Respond runtime status inquiry",
      (ctx) => {
        const targetServiceName = ctx.targetServiceName;
        const targetServiceInstanceId = ctx.targetServiceInstanceId;
        const detailLevel: "minimal" | "full" =
          ctx.detailLevel === "full" ? "full" : "minimal";
        const report = this.buildLocalRuntimeStatusReport(detailLevel);
        if (!report) {
          return {};
        }

        if (
          targetServiceName &&
          targetServiceName !== report.serviceName
        ) {
          return {};
        }

        if (
          targetServiceInstanceId &&
          targetServiceInstanceId !== report.serviceInstanceId
        ) {
          return {};
        }

        return {
          runtimeStatusReports: [report],
        };
      },
      "Responds to runtime-status inquiries with local service instance status.",
    ).respondsTo(META_RUNTIME_STATUS_INTENT);

    Cadenza.defineIntent({
      name: META_READINESS_INTENT,
      description:
        "Gather service readiness reports derived from local runtime status and required dependees.",
      input: {
        type: "object",
        properties: {
          detailLevel: {
            type: "string",
            constraints: {
              oneOf: ["minimal", "full"],
            },
          },
          includeDependencies: {
            type: "boolean",
          },
          refreshStaleDependencies: {
            type: "boolean",
          },
          targetServiceName: {
            type: "string",
          },
          targetServiceInstanceId: {
            type: "string",
          },
        },
      },
      output: {
        type: "object",
        properties: {
          readinessReports: {
            type: "array",
          },
        },
      },
    });

    Cadenza.createMetaTask(
      "Respond readiness inquiry",
      async (ctx) => {
        const targetServiceName = ctx.targetServiceName;
        const targetServiceInstanceId = ctx.targetServiceInstanceId;
        const report = await this.buildLocalReadinessReport({
          detailLevel: ctx.detailLevel === "full" ? "full" : "minimal",
          includeDependencies: ctx.includeDependencies,
          refreshStaleDependencies: ctx.refreshStaleDependencies,
        });
        if (!report) {
          return {};
        }

        if (targetServiceName && targetServiceName !== report.serviceName) {
          return {};
        }

        if (
          targetServiceInstanceId &&
          targetServiceInstanceId !== report.serviceInstanceId
        ) {
          return {};
        }

        return {
          readinessReports: [report],
        };
      },
      "Responds to distributed readiness inquiries using required dependee health.",
    ).respondsTo(META_READINESS_INTENT);

    this.handleInstanceUpdateTask = Cadenza.createMetaTask(
      "Handle Instance Update",
      (ctx, emit) => {
        const serviceInstance =
          ctx.serviceInstance ??
          (ctx.__serviceInstanceId || ctx.serviceInstanceId
            ? {
                uuid: ctx.__serviceInstanceId ?? ctx.serviceInstanceId,
                serviceName: ctx.__serviceName ?? ctx.serviceName,
                address: ctx.serviceAddress ?? "",
                port: ctx.servicePort ?? 0,
                exposed: !!ctx.exposed,
                isFrontend: !!ctx.isFrontend,
                isActive:
                  typeof ctx.isActive === "boolean"
                    ? ctx.isActive
                    : typeof ctx.__active === "boolean"
                      ? ctx.__active
                      : true,
                isNonResponsive: !!ctx.isNonResponsive,
                isBlocked: !!ctx.isBlocked,
                health: (ctx.health ?? ctx.__health ?? {}) as AnyObject,
                numberOfRunningGraphs:
                  ctx.numberOfRunningGraphs ?? ctx.__numberOfRunningGraphs ?? 0,
                isPrimary: false,
              }
            : undefined);
        if (!serviceInstance?.uuid || !serviceInstance?.serviceName) {
          return false;
        }
        const {
          uuid,
          serviceName,
          address,
          port,
          exposed,
          isFrontend,
          deleted,
        } = serviceInstance;
        if (uuid === this.serviceInstanceId) return;

        if (deleted) {
          const indexToDelete =
            this.instances.get(serviceName)?.findIndex((i) => i.uuid === uuid) ??
            -1;
          if (indexToDelete >= 0) {
            this.instances.get(serviceName)?.splice(indexToDelete, 1);
          }

          if (this.instances.get(serviceName)?.length === 0) {
            this.instances.delete(serviceName);
          } else if (
            this.instances
              .get(serviceName)
              ?.filter((i) => i.address === address && i.port === port)
              .length === 0
          ) {
            emit(`meta.socket_shutdown_requested:${address}_${port}`, {});
            emit(`meta.fetch.destroy_requested:${address}_${port}`, {});
          }

          this.unregisterDependee(uuid, serviceName);

          return;
        }

        if (!this.instances.has(serviceName))
          this.instances.set(serviceName, []);
        const instances = this.instances.get(serviceName)!;
        const existing = instances.find((i) => i.uuid === uuid);

        if (existing) {
          Object.assign(existing, serviceInstance); // Update
        } else {
          instances.push(serviceInstance);
        }

        const trackedInstance =
          existing ?? instances.find((instance) => instance.uuid === uuid);
        if (trackedInstance) {
          const snapshot = this.resolveRuntimeStatusSnapshot(
            trackedInstance.numberOfRunningGraphs ?? 0,
            trackedInstance.isActive,
            trackedInstance.isNonResponsive,
            trackedInstance.isBlocked,
          );
          trackedInstance.runtimeState = snapshot.state;
          trackedInstance.acceptingWork = snapshot.acceptingWork;
          trackedInstance.reportedAt =
            trackedInstance.reportedAt ?? new Date().toISOString();
        }

        if (this.serviceName === serviceName) {
          return false;
        }

        if (
          (!isFrontend &&
            (this.deputies.has(serviceName) ||
              this.remoteIntents.has(serviceName))) ||
          this.remoteSignals.has(serviceName)
        ) {
          const clientCreated = instances?.some(
            (i) =>
              i.address === address &&
              i.port === port &&
              i.clientCreated &&
              i.isActive,
          );

          if (!clientCreated) {
            const communicationTypes = Array.from(
              new Set(
                this.deputies
                  .get(serviceName)
                  ?.map((d) => d.communicationType) ?? [],
              ),
            );

            if (
              !communicationTypes.includes("signal") &&
              this.remoteSignals.has(serviceName)
            ) {
              communicationTypes.push("signal");
            }

            emit("meta.service_registry.dependee_registered", {
              serviceName: serviceName,
              serviceInstanceId: uuid,
              serviceAddress: address,
              servicePort: port,
              protocol: exposed ? "https" : "http",
              communicationTypes,
            });

            instances
              ?.filter(
                (i: any) =>
                  i.address === address &&
                  i.port === port &&
                  i.isActive,
              )
              .forEach((i: any) => {
                i.clientCreated = true;
              });
          }
        }

        return true;
      },
      "Handles instance updates to service instances",
    )
      .emits("meta.service_registry.service_discovered")
      .doOn(
        "meta.initializing_service",
        "global.meta.service_instance.inserted",
        "global.meta.service_instance.updated",
        "meta.service_instance.inserted",
        "meta.service_instance.updated",
      )
      .attachSignal(
        "meta.service_registry.dependee_registered",
        "meta.socket_shutdown_requested",
        "meta.fetch.destroy_requested",
      );

    Cadenza.createMetaTask(
      "Track dependee registration",
      (ctx) => {
        if (!ctx.serviceName || !ctx.serviceInstanceId) {
          return false;
        }

        this.registerDependee(ctx.serviceName, ctx.serviceInstanceId, {
          requiredForReadiness: this.shouldRequireReadinessFromCommunicationTypes(
            ctx.communicationTypes,
          ),
        });
        return true;
      },
      "Tracks remote dependency instances for runtime heartbeat monitoring.",
    ).doOn("meta.service_registry.dependee_registered");

    Cadenza.createMetaTask("Split service instances", function* (ctx: any) {
      if (!ctx.serviceInstances) {
        return;
      }

      for (const serviceInstance of ctx.serviceInstances) {
        yield { serviceInstance };
      }
    })
      .doOn(
        "meta.service_registry.registered_global_signals",
        "meta.service_registry.registered_global_intents",
      )
      .then(this.handleInstanceUpdateTask);

    this.handleGlobalSignalRegistrationTask = Cadenza.createMetaTask(
      "Handle global Signal Registration",
      (ctx) => {
        const { signalToTaskMaps } = ctx;
        const sortedSignalToTaskMap = signalToTaskMaps.sort(
          (a: any, b: any) => {
            if (a.deleted && !b.deleted) return -1;
            if (!a.deleted && b.deleted) return 1;
            return 0;
          },
        );

        const locallyEmittedSignals = Cadenza.signalBroker
          .listEmittedSignals()
          .filter((s: any) => s.startsWith("global."));

        for (const map of sortedSignalToTaskMap) {
          if (map.deleted) {
            this.remoteSignals.get(map.serviceName)?.delete(map.signalName);

            if (!this.remoteSignals.get(map.serviceName)?.size) {
              this.remoteSignals.delete(map.serviceName);
            }

            Cadenza.get(
              `Transmit signal: ${map.signalName} to ${map.serviceName}`,
            )?.destroy();
            continue;
          }

          if (locallyEmittedSignals.includes(map.signalName)) {
            if (!this.remoteSignals.get(map.serviceName)) {
              this.remoteSignals.set(map.serviceName, new Set());
            }

            if (!this.remoteSignals.get(map.serviceName)?.has(map.signalName)) {
              Cadenza.createSignalTransmissionTask(
                map.signalName,
                map.serviceName,
              );
            }

            this.remoteSignals.get(map.serviceName)?.add(map.signalName);
          }
        }

        return true;
      },
      "Handles registration of remote signals",
    )
      .emits("meta.service_registry.registered_global_signals")
      .doOn("global.meta.cadenza_db.gathered_sync_data");

    this.handleGlobalIntentRegistrationTask = Cadenza.createMetaTask(
      "Handle global intent registration",
      (ctx) => {
        const intentToTaskMaps = this.normalizeIntentMaps(ctx);
        const sorted = intentToTaskMaps.sort((a, b) => {
          if (a.deleted && !b.deleted) return -1;
          if (!a.deleted && b.deleted) return 1;
          return 0;
        });

        for (const map of sorted) {
          if (map.deleted) {
            this.unregisterRemoteIntentDeputy(map);
            continue;
          }

          Cadenza.inquiryBroker.addIntent({
            name: map.intentName,
          });

          this.registerRemoteIntentDeputy(map);
        }

        return true;
      },
      "Handles registration of remote inquiry intent responders",
    )
      .emits("meta.service_registry.registered_global_intents")
      .doOn(
        "global.meta.cadenza_db.gathered_sync_data",
        "global.meta.graph_metadata.task_intent_associated",
      );

    this.handleServiceNotRespondingTask = Cadenza.createMetaTask(
      "Handle service not responding",
      (ctx, emit) => {
        const { serviceName, serviceAddress, servicePort } = ctx;
        const serviceInstances = this.instances.get(serviceName);
        const instances = serviceInstances?.filter(
          (i) => i.address === serviceAddress && i.port === servicePort,
        );

        Cadenza.log(
          "Service not responding.",
          {
            serviceName,
            serviceAddress,
            servicePort,
            instances,
          },
          "warning",
          serviceName,
        );

        for (const instance of instances ?? []) {
          instance.isActive = false;
          instance.isNonResponsive = true;
          const snapshot = this.resolveRuntimeStatusSnapshot(
            instance.numberOfRunningGraphs ?? 0,
            instance.isActive,
            instance.isNonResponsive,
            instance.isBlocked,
          );
          instance.runtimeState = snapshot.state;
          instance.acceptingWork = snapshot.acceptingWork;
          instance.reportedAt = new Date().toISOString();
          emit("global.meta.service_registry.service_not_responding", {
            data: {
              isActive: false,
              isNonResponsive: true,
            },
            filter: {
              uuid: instance.uuid,
            },
          });
        }

        return true;
      },
      "Handles service not responding",
    )
      .doOn(
        "meta.fetch.handshake_failed",
        "meta.fetch.handshake_failed.*",
        "meta.socket_client.disconnected",
        "meta.socket_client.disconnected.*",
        "meta.service_registry.runtime_status_unreachable",
      )
      .attachSignal("global.meta.service_registry.service_not_responding");

    this.handleServiceHandshakeTask = Cadenza.createMetaTask(
      "Handle service handshake",
      (ctx, emit) => {
        const { serviceName, serviceInstanceId, serviceAddress, servicePort } =
          ctx;
        const serviceInstances = this.instances.get(serviceName);
        const instance = serviceInstances?.find(
          (i) => i.uuid === serviceInstanceId,
        );

        if (!instance) {
          return false;
        }

        instance.isActive = true;
        instance.isNonResponsive = false;
        const snapshot = this.resolveRuntimeStatusSnapshot(
          instance.numberOfRunningGraphs ?? 0,
          instance.isActive,
          instance.isNonResponsive,
          instance.isBlocked,
        );
        instance.runtimeState = snapshot.state;
        instance.acceptingWork = snapshot.acceptingWork;
        instance.reportedAt = new Date().toISOString();
        emit("global.meta.service_registry.service_handshake", {
          data: {
            isActive: instance.isActive,
            isNonResponsive: instance.isNonResponsive,
          },
          filter: {
            uuid: instance.uuid,
          },
        });

        const instancesToDelete = serviceInstances?.filter(
          (i) =>
            i.uuid !== serviceInstanceId &&
            i.address === serviceAddress &&
            i.port === servicePort,
        );

        for (const i of instancesToDelete ?? []) {
          const indexToDelete = this.instances.get(serviceName)?.indexOf(i) ?? -1;
          if (indexToDelete >= 0) {
            this.instances.get(serviceName)?.splice(indexToDelete, 1);
          }
          this.unregisterDependee(i.uuid, serviceName);
          emit("global.meta.service_registry.deleted", {
            data: {
              isActive: false,
              isNonResponsive: false,
              deleted: true,
            },
            filter: {
              uuid: i.uuid,
            },
          });
        }

        return true;
      },
      "Handles service handshake",
    )
      .doOn("meta.fetch.handshake_complete")
      .attachSignal(
        "global.meta.service_registry.service_handshake",
        "global.meta.service_registry.deleted",
      );

    this.handleSocketStatusUpdateTask = Cadenza.createMetaTask(
      "Handle Socket Status Update",
      (ctx) => {
        const report = this.normalizeRuntimeStatusReport(ctx);
        if (!report) {
          return false;
        }

        if (
          report.serviceName === this.serviceName &&
          report.serviceInstanceId === this.serviceInstanceId
        ) {
          return false;
        }

        let applied = this.applyRuntimeStatusReport(report);
        if (
          !applied &&
          report.serviceAddress &&
          typeof report.servicePort === "number"
        ) {
          if (!this.instances.has(report.serviceName)) {
            this.instances.set(report.serviceName, []);
          }

          this.instances.get(report.serviceName)!.push({
            uuid: report.serviceInstanceId,
            serviceName: report.serviceName,
            address: report.serviceAddress,
            port: report.servicePort,
            exposed: !!report.exposed,
            isFrontend: !!report.isFrontend,
            isActive: report.isActive,
            isNonResponsive: report.isNonResponsive,
            isBlocked: report.isBlocked,
            numberOfRunningGraphs: report.numberOfRunningGraphs,
            runtimeState: report.state,
            acceptingWork: report.acceptingWork,
            reportedAt: report.reportedAt,
            health: report.health ?? {},
            isPrimary: false,
          });
          applied = true;
        }

        if (!applied) {
          return false;
        }

        this.registerDependee(report.serviceName, report.serviceInstanceId);
        this.lastHeartbeatAtByInstance.set(report.serviceInstanceId, Date.now());
        this.missedHeartbeatsByInstance.set(report.serviceInstanceId, 0);
        this.runtimeStatusFallbackInFlightByInstance.delete(
          report.serviceInstanceId,
        );
        return true;
      },
      "Handles status update from socket broadcast",
    ).doOn("meta.socket_client.status_received");

    this.fullSyncTask = Cadenza.createMetaTask(
      "Full sync",
      async (ctx) => {
        const inquiryResult = await Cadenza.inquire(
          META_SERVICE_REGISTRY_FULL_SYNC_INTENT,
          {
            syncScope: "service-registry-full-sync",
          },
          ctx.inquiryOptions ?? ctx.__inquiryOptions ?? {},
        );

        const signalToTaskMaps = (inquiryResult.signalToTaskMaps ?? [])
          .filter((m: any) => !!m.isGlobal)
          .map((m: any) => ({
            signalName: m.signalName,
            serviceName: m.serviceName,
            deleted: !!m.deleted,
          }));

        const intentToTaskMaps = (inquiryResult.intentToTaskMaps ?? []).map(
          (m: any) => ({
            intentName: m.intentName,
            taskName: m.taskName,
            taskVersion: m.taskVersion ?? 1,
            serviceName: m.serviceName,
            deleted: !!m.deleted,
          }),
        );

        const serviceInstances = (inquiryResult.serviceInstances ?? [])
          .filter(
            (instance: any) =>
              !instance.deleted &&
              !!instance.isActive &&
              !instance.isNonResponsive &&
              !instance.isBlocked,
          )
          .map((instance: any) => ({
            uuid: instance.uuid,
            address: instance.address,
            port: instance.port,
            serviceName: instance.serviceName,
            isActive: !!instance.isActive,
            isNonResponsive: !!instance.isNonResponsive,
            isBlocked: !!instance.isBlocked,
            health: instance.health ?? {},
            exposed: !!instance.exposed,
            created: instance.created,
            isFrontend: !!instance.isFrontend,
          }));

        return {
          ...ctx,
          signalToTaskMaps,
          intentToTaskMaps,
          serviceInstances,
          __inquiryMeta: inquiryResult.__inquiryMeta,
        };
      },
      "Runs service registry full sync through one distributed inquiry intent.",
    )
      .doOn("meta.sync_requested")
      .emits("meta.service_registry.initial_sync_complete")
      .then(
        this.handleGlobalSignalRegistrationTask,
        this.handleGlobalIntentRegistrationTask,
      );

    this.getInstanceById = Cadenza.createMetaTask(
      "Get instance by id",
      (context) => {
        const { __id } = context;
        let instance;
        for (const instances of this.instances.values()) {
          instance = instances.find((i) => i.uuid === __id);
          if (instance) break;
        }
        return { ...context, __instance: instance };
      },
      "Gets instance by id.",
    );

    this.getInstancesByServiceName = Cadenza.createMetaTask(
      "Get instances by name",
      (context) => {
        const { __serviceName } = context;
        const instances = this.instances.get(__serviceName);
        if (!instances) {
          return false;
        }

        return { ...context, __instances: instances };
      },
      "Gets instances by name.",
    );

    this.handleDeputyRegistrationTask = Cadenza.createMetaTask(
      "Handle Deputy Registration",
      (ctx) => {
        const { serviceName } = ctx;

        if (!this.deputies.has(serviceName)) this.deputies.set(serviceName, []);

        this.deputies.get(serviceName)!.push({
          serviceName,
          remoteRoutineName: ctx.remoteRoutineName,
          signalName: ctx.signalName,
          localTaskName: ctx.localTaskName,
          communicationType: ctx.communicationType,
        });
      },
    ).doOn("meta.deputy.created");

    this.getAllInstances = Cadenza.createMetaTask(
      "Get all instances",
      (context) => ({
        ...context,
        __instances: Array.from(this.instances.values()).flat(),
      }),
      "Gets all instances.",
    );

    this.doForEachInstance = Cadenza.createMetaTask(
      "Do for each instance",
      function* (context: AnyObject) {
        // @ts-ignore
        for (const instances of this.instances.values()) {
          for (const instance of instances) {
            yield { ...context, __instance: instance };
          }
        }
      }.bind(this),
      "Yields each instance for branching.",
    );

    this.deleteInstance = Cadenza.createMetaTask(
      "Delete instance",
      (context) => {
        const { __id } = context;
        this.instances.delete(__id);
        return context;
      },
      "Deletes instance.",
    ).doOn("global.meta.service_instance.deleted");

    this.getBalancedInstance = Cadenza.createMetaTask(
      "Get balanced instance",
      (context, emit) => {
        const { __serviceName, __triedInstances, __retries, __broadcast } =
          context;
        let retries = __retries ?? 0;
        let triedInstances = __triedInstances ?? [];

        const instances = this.instances
          .get(__serviceName)
          ?.filter((i) => i.isActive && !i.isNonResponsive && !i.isBlocked)
          .sort((a, b) => {
            const leftStatus = this.resolveRuntimeStatusSnapshot(
              a.numberOfRunningGraphs ?? 0,
              a.isActive,
              a.isNonResponsive,
              a.isBlocked,
            );
            const rightStatus = this.resolveRuntimeStatusSnapshot(
              b.numberOfRunningGraphs ?? 0,
              b.isActive,
              b.isNonResponsive,
              b.isBlocked,
            );

            const priorityDelta =
              runtimeStatusPriority(leftStatus.state) -
              runtimeStatusPriority(rightStatus.state);
            if (priorityDelta !== 0) {
              return priorityDelta;
            }

            return (
              (a.numberOfRunningGraphs ?? 0) - (b.numberOfRunningGraphs ?? 0)
            );
          });

        if (!instances || instances.length === 0 || retries > this.retryCount) {
          context.errored = true;
          context.__error = `No active instances for ${__serviceName}. Retries: ${retries}. ${this.instances.get(
            __serviceName,
          )}`;
          emit(
            `meta.service_registry.load_balance_failed:${context.__metadata.__deputyExecId}`,
            context,
          );
          return context;
        }

        if (__broadcast || instances[0].isFrontend) {
          for (const instance of instances) {
            const socketKey = instance.isFrontend
              ? instance.address
              : `${instance.address}_${instance.port}`;
            emit(
              `meta.service_registry.selected_instance_for_socket:${socketKey}`,
              context,
            );
          }

          return context;
        }

        let instancesToTry = instances.filter(
          (i) => !__triedInstances?.includes(i.uuid),
        );

        if (instancesToTry.length === 0) {
          if (this.useSocket) {
            emit(
              `meta.service_registry.socket_failed:${context.__fetchId}`,
              context,
            );
          }
          retries++;
          instancesToTry = instances;
          triedInstances = [];
        }

        let selected = instancesToTry[0];
        if (retries > 0) {
          selected =
            instancesToTry[Math.floor(Math.random() * instancesToTry.length)];
        }

        context.__instance = selected.uuid;
        context.__fetchId = `${selected.address}_${selected.port}`;
        context.__triedInstances = triedInstances;
        context.__triedInstances.push(selected.uuid);
        context.__retries = retries;

        if (this.useSocket) {
          emit(
            `meta.service_registry.selected_instance_for_socket:${context.__fetchId}`,
            context,
          );
        } else {
          emit(
            `meta.service_registry.selected_instance_for_fetch:${context.__fetchId}`,
            context,
          );
        }

        return context;
      },
      "Gets a balanced instance for load balancing",
    )
      .doOn(
        "meta.deputy.delegation_requested",
        "meta.signal_transmission.requested",
        "meta.socket_client.delegate_failed",
        "meta.fetch.delegate_failed",
        "meta.socket_client.signal_transmission_failed",
      )
      .attachSignal(
        "meta.service_registry.load_balance_failed",
        "meta.service_registry.selected_instance_for_socket",
        "meta.service_registry.selected_instance_for_fetch",
        "meta.service_registry.socket_failed",
      );

    this.getStatusTask = Cadenza.createMetaTask("Get status", (ctx) => {
      if (!this.serviceName) {
        return {
          __status: "error",
          __error: "No service name defined",
          errored: true,
        };
      }

      if (!this.serviceInstanceId) {
        return {
          __status: "error",
          __error: "No service instance id defined",
          errored: true,
        };
      }

      const report = this.buildLocalRuntimeStatusReport("full");
      if (!report) {
        return {
          ...ctx,
          __status: "error",
          __error: "No local service instance available for status check",
          errored: true,
        };
      }

      return {
        ...ctx,
        __status: "ok",
        __serviceName: report.serviceName,
        __serviceInstanceId: report.serviceInstanceId,
        __numberOfRunningGraphs: report.numberOfRunningGraphs,
        __health: report.health ?? {},
        __active: report.isActive,
        reportedAt: report.reportedAt,
        serviceName: report.serviceName,
        serviceInstanceId: report.serviceInstanceId,
        numberOfRunningGraphs: report.numberOfRunningGraphs,
        health: report.health ?? {},
        isActive: report.isActive,
        isNonResponsive: report.isNonResponsive,
        isBlocked: report.isBlocked,
        state: report.state,
        acceptingWork: report.acceptingWork,
      };
    }).doOn(
      "meta.socket.status_check_requested",
      "meta.rest.status_check_requested",
    );

    Cadenza.createMetaTask(
      "Track local routine start",
      (ctx, emit) => {
        const sourceTaskName = String(ctx.__signalEmission?.taskName ?? "");
        if (INTERNAL_RUNTIME_STATUS_TASK_NAMES.has(sourceTaskName)) {
          return false;
        }

        const routineId = String(
          ctx.filter?.uuid ?? ctx.__routineExecId ?? "",
        );
        if (!routineId) {
          return false;
        }

        this.activeRoutineExecutionIds.add(routineId);
        this.numberOfRunningGraphs = this.activeRoutineExecutionIds.size;
        const localInstance = this.getLocalInstance();
        if (!localInstance) {
          return true;
        }

        const snapshot = this.resolveRuntimeStatusSnapshot(
          this.numberOfRunningGraphs,
          localInstance.isActive,
          localInstance.isNonResponsive,
          localInstance.isBlocked,
        );
        if (
          hasSignificantRuntimeStatusChange(this.lastRuntimeStatusSnapshot, snapshot)
        ) {
          emit("meta.service_registry.runtime_status_broadcast_requested", {
            reason: "runtime-state-change",
          });
        }
        return true;
      },
      "Tracks local routine starts for runtime load status.",
    ).doOn("meta.node.started_routine_execution");

    Cadenza.createMetaTask(
      "Track local routine end",
      (ctx, emit) => {
        const sourceTaskName = String(ctx.__signalEmission?.taskName ?? "");
        if (INTERNAL_RUNTIME_STATUS_TASK_NAMES.has(sourceTaskName)) {
          return false;
        }

        const routineId = String(
          ctx.filter?.uuid ?? ctx.__routineExecId ?? "",
        );
        if (!routineId) {
          return false;
        }

        this.activeRoutineExecutionIds.delete(routineId);
        this.numberOfRunningGraphs = this.activeRoutineExecutionIds.size;
        const localInstance = this.getLocalInstance();
        if (!localInstance) {
          return true;
        }

        const snapshot = this.resolveRuntimeStatusSnapshot(
          this.numberOfRunningGraphs,
          localInstance.isActive,
          localInstance.isNonResponsive,
          localInstance.isBlocked,
        );
        if (
          hasSignificantRuntimeStatusChange(this.lastRuntimeStatusSnapshot, snapshot)
        ) {
          emit("meta.service_registry.runtime_status_broadcast_requested", {
            reason: "runtime-state-change",
          });
        }
        return true;
      },
      "Tracks local routine completion for runtime load status.",
    ).doOn("meta.node.ended_routine_execution");

    Cadenza.createMetaTask(
      "Start runtime status sharing intervals",
      () => {
        if (this.runtimeStatusHeartbeatStarted) {
          return false;
        }

        this.runtimeStatusHeartbeatStarted = true;
        Cadenza.interval(
          META_RUNTIME_STATUS_HEARTBEAT_TICK_SIGNAL,
          { reason: "heartbeat" },
          this.runtimeStatusHeartbeatIntervalMs,
          true,
        );
        Cadenza.interval(
          META_RUNTIME_STATUS_MONITOR_TICK_SIGNAL,
          {},
          this.runtimeStatusHeartbeatIntervalMs,
        );
        return true;
      },
      "Starts runtime status heartbeat and heartbeat-monitor loops once per service instance.",
    ).doOn("meta.service_registry.instance_inserted");

    Cadenza.createMetaTask(
      "Broadcast runtime status",
      (ctx, emit) => {
        const report = this.buildLocalRuntimeStatusReport(
          ctx.detailLevel === "full" ? "full" : "minimal",
        );
        if (!report) {
          return false;
        }

        const snapshot = this.resolveRuntimeStatusSnapshot(
          report.numberOfRunningGraphs,
          report.isActive,
          report.isNonResponsive,
          report.isBlocked,
        );
        const force =
          ctx.reason === "heartbeat" ||
          ctx.force === true ||
          this.lastRuntimeStatusSnapshot === null;

        if (
          !force &&
          !hasSignificantRuntimeStatusChange(this.lastRuntimeStatusSnapshot, snapshot)
        ) {
          return false;
        }

        this.lastRuntimeStatusSnapshot = snapshot;
        emit("meta.service.updated", {
          __serviceName: report.serviceName,
          __serviceInstanceId: report.serviceInstanceId,
          __reportedAt: report.reportedAt,
          __numberOfRunningGraphs: report.numberOfRunningGraphs,
          __health: report.health ?? {},
          __active: report.isActive,
          serviceName: report.serviceName,
          serviceInstanceId: report.serviceInstanceId,
          serviceAddress: report.serviceAddress,
          servicePort: report.servicePort,
          exposed: report.exposed,
          isFrontend: report.isFrontend,
          reportedAt: report.reportedAt,
          numberOfRunningGraphs: report.numberOfRunningGraphs,
          health: report.health ?? {},
          isActive: report.isActive,
          isNonResponsive: report.isNonResponsive,
          isBlocked: report.isBlocked,
          state: report.state,
          acceptingWork: report.acceptingWork,
        });
        return true;
      },
      "Broadcasts local runtime status to connected dependees.",
    ).doOn(
      META_RUNTIME_STATUS_HEARTBEAT_TICK_SIGNAL,
      "meta.service_registry.runtime_status_broadcast_requested",
    );

    Cadenza.createMetaTask(
      "Monitor dependee heartbeat freshness",
      (ctx, emit) => {
        if (!this.useSocket) {
          return false;
        }

        const now = Date.now();
        for (const [serviceName, instanceIds] of this.dependeesByService) {
          for (const serviceInstanceId of instanceIds) {
            const instance = this.getInstance(serviceName, serviceInstanceId);
            if (!instance || !instance.isActive || instance.isBlocked) {
              continue;
            }

            const lastHeartbeat =
              this.lastHeartbeatAtByInstance.get(serviceInstanceId) ?? 0;
            const misses = this.missedHeartbeatsByInstance.get(serviceInstanceId) ?? 0;
            const heartbeatBudget =
              this.runtimeStatusHeartbeatIntervalMs * (misses + 1);

            if (lastHeartbeat > 0 && now - lastHeartbeat < heartbeatBudget) {
              continue;
            }

            const nextMisses = misses + 1;
            this.missedHeartbeatsByInstance.set(serviceInstanceId, nextMisses);

            if (
              nextMisses < this.runtimeStatusMissThreshold ||
              this.runtimeStatusFallbackInFlightByInstance.has(serviceInstanceId)
            ) {
              continue;
            }

            this.runtimeStatusFallbackInFlightByInstance.add(serviceInstanceId);
            emit("meta.service_registry.runtime_status_fallback_requested", {
              ...ctx,
              serviceName,
              serviceInstanceId,
              serviceAddress: instance.address,
              servicePort: instance.port,
            });
          }
        }

        return true;
      },
      "Monitors dependee heartbeat freshness and requests inquiry fallback after repeated misses.",
    ).doOn(META_RUNTIME_STATUS_MONITOR_TICK_SIGNAL);

    Cadenza.createMetaTask(
      "Resolve runtime status fallback inquiry",
      async (ctx, emit) => {
        const serviceName = ctx.serviceName;
        const serviceInstanceId = ctx.serviceInstanceId;
        if (!serviceName || !serviceInstanceId) {
          return false;
        }

        try {
          const { report, inquiryMeta } =
            await this.resolveRuntimeStatusFallbackInquiry(
              serviceName,
              serviceInstanceId,
              {
                detailLevel: ctx.detailLevel === "full" ? "full" : "minimal",
                overallTimeoutMs: ctx.overallTimeoutMs,
                perResponderTimeoutMs: ctx.perResponderTimeoutMs,
                requireComplete: ctx.requireComplete,
              },
            );

          return {
            ...ctx,
            runtimeStatusReport: report,
            __inquiryMeta: inquiryMeta,
          };
        } catch (error) {
          const instance = this.getInstance(serviceName, serviceInstanceId);
          const message =
            error instanceof Error ? error.message : String(error);

          Cadenza.log(
            "Runtime status fallback inquiry failed.",
            {
              serviceName,
              serviceInstanceId,
              error: message,
            },
            "warning",
          );

          emit("meta.service_registry.runtime_status_unreachable", {
            ...ctx,
            serviceName,
            serviceInstanceId,
            serviceAddress: instance?.address ?? ctx.serviceAddress,
            servicePort: instance?.port ?? ctx.servicePort,
            __error: message,
            errored: true,
          });

          return {
            ...ctx,
            __error: message,
            errored: true,
          };
        } finally {
          this.runtimeStatusFallbackInFlightByInstance.delete(serviceInstanceId);
        }
      },
      "Runs runtime-status inquiry fallback for a dependee instance after missed heartbeats.",
    )
      .doOn("meta.service_registry.runtime_status_fallback_requested")
      .emits("meta.service_registry.runtime_status_fallback_resolved")
      .emitsOnFail("meta.service_registry.runtime_status_fallback_failed");

    this.collectReadinessTask = Cadenza.createMetaTask(
      "Collect distributed readiness",
      async (ctx) => {
        const inquiryResult = await Cadenza.inquire(
          META_READINESS_INTENT,
          {
            detailLevel: ctx.detailLevel === "full" ? "full" : "minimal",
            includeDependencies: ctx.includeDependencies,
            refreshStaleDependencies: ctx.refreshStaleDependencies,
            targetServiceName: ctx.targetServiceName,
            targetServiceInstanceId: ctx.targetServiceInstanceId,
          },
          ctx.inquiryOptions ?? ctx.__inquiryOptions ?? {},
        );

        return {
          ...ctx,
          ...inquiryResult,
        };
      },
      "Collects distributed readiness reports from services.",
    )
      .doOn("meta.service_registry.readiness_requested")
      .emits("meta.service_registry.readiness_collected")
      .emitsOnFail("meta.service_registry.readiness_failed");

    this.collectTransportDiagnosticsTask = Cadenza.createMetaTask(
      "Collect transport diagnostics",
      async (ctx) => {
        const inquiryResult = await Cadenza.inquire(
          META_RUNTIME_TRANSPORT_DIAGNOSTICS_INTENT,
          {
            detailLevel: ctx.detailLevel,
            includeErrorHistory: ctx.includeErrorHistory,
            errorHistoryLimit: ctx.errorHistoryLimit,
          },
          ctx.inquiryOptions ?? ctx.__inquiryOptions ?? {},
        );

        return {
          ...ctx,
          ...inquiryResult,
        };
      },
      "Collects distributed transport diagnostics using inquiry responders.",
    )
      .doOn("meta.service_registry.transport_diagnostics_requested")
      .emits("meta.service_registry.transport_diagnostics_collected")
      .emitsOnFail("meta.service_registry.transport_diagnostics_failed");

    this.insertServiceTask = Cadenza.createCadenzaDBInsertTask(
      "service",
      {
        onConflict: {
          target: ["name"],
          action: {
            do: "nothing",
          },
        },
      },
      {
        // validateInputContext: true,
        inputSchema: {
          type: "object",
          properties: {
            data: {
              type: "object",
              properties: {
                name: {
                  type: "string",
                },
                description: {
                  type: "string",
                },
                display_name: {
                  type: "string",
                },
                is_meta: {
                  type: "boolean",
                },
              },
              required: ["name"],
            },
          },
          required: ["data"],
        },
        outputSchema: {
          type: "object",
          properties: {
            __serviceName: {
              type: "string",
            },
          },
          required: ["__serviceName"],
        },
        retryCount: 100,
        retryDelay: 10000,
        retryDelayMax: 60000,
        retryDelayFactor: 1.3,
      },
    )
      .emits("meta.service_registry.service_inserted")
      .emitsOnFail("meta.service_registry.service_insertion_failed");

    this.insertServiceInstanceTask = Cadenza.createCadenzaDBInsertTask(
      "serviceInstance",
      {},
      {
        inputSchema: {
          type: "object",
          properties: {
            uuid: {
              type: "string",
            },
            address: {
              type: "string",
            },
            port: {
              type: "number",
            },
            process_pid: {
              type: "number",
            },
            is_primary: {
              type: "boolean",
            },
            service_name: {
              type: "string",
            },
            is_active: {
              type: "boolean",
            },
            is_non_responsive: {
              type: "boolean",
            },
            is_blocked: {
              type: "boolean",
            },
            exposed: {
              type: "boolean",
            },
          },
          required: [
            "id",
            "address",
            "port",
            "process_pid",
            "service_name",
            "exposed",
          ],
        },
        // validateInputContext: true,
        outputSchema: {
          type: "object",
          properties: {
            id: {
              type: "string",
            },
          },
          required: ["id"],
        },
        // validateOutputContext: true,
        retryCount: 5,
        retryDelay: 1000,
      },
    )
      .doOn("global.meta.rest.network_configured", "meta.rest.browser_detected")
      .then(
        Cadenza.createMetaTask(
          "Setup service",
          (ctx) => {
            const { serviceInstance, data, __useSocket, __retryCount } = ctx;
            this.serviceInstanceId = serviceInstance?.uuid ?? data?.uuid;
            this.instances.set(
              data?.service_name ?? serviceInstance?.service_name,
              [{ ...(serviceInstance ?? data) }],
            );
            this.useSocket = __useSocket;
            this.retryCount = __retryCount;
            console.log("SETUP SERVICE", this.serviceInstanceId);
            return true;
          },
          "Sets service instance id after insertion",
        ).emits("meta.service_registry.instance_inserted"),
      );

    Cadenza.createMetaTask(
      "Handle service creation",
      (ctx) => {
        if (!ctx.__cadenzaDBConnect) {
          ctx.__skipRemoteExecution = true;
        }

        if (isBrowser) {
          Cadenza.createMetaTask("Prepare for signal sync", () => {
            return {};
          })
            // .doAfter(this.fullSyncTask)
            .then(
              Cadenza.createCadenzaDBQueryTask("signal_registry", {
                fields: ["name"],
                filter: {
                  global: true,
                },
              }).then(
                Cadenza.createMetaTask(
                  // TODO this is outdated. Fix it.
                  "Create signal transmission tasks",
                  (ctx, emit) => {
                    const signalRegistry = ctx.signalRegistry;
                    for (const signal of signalRegistry) {
                      emit("meta.service_registry.foreign_signal_registered", {
                        __emitterSignalName: signal.name,
                        __listenerServiceName: signal.serviceName,
                      });
                    }

                    return true;
                  },
                ).then(
                  Cadenza.createMetaTask("Connect to services", (ctx, emit) => {
                    const services: string[] = Array.from(
                      new Set(
                        ctx.signalRegistry.map((s: any) => s.serviceName),
                      ),
                    );
                    for (const service of services) {
                      const instances = this.instances
                        .get(service)!
                        .filter((i) => i.isActive);
                      for (const instance of instances) {
                        if (instance.clientCreated) continue;
                        const address = instance.address;
                        const port = instance.port;

                        const clientCreated = instances?.some(
                          (i) =>
                            i.address === address &&
                            i.port === port &&
                            i.clientCreated &&
                            i.isActive,
                        );

                        if (!clientCreated) {
                          emit("meta.service_registry.dependee_registered", {
                            serviceName: service,
                            serviceInstanceId: instance.uuid,
                            serviceAddress: address,
                            servicePort: port,
                            protocol: instance.exposed ? "https" : "http",
                            communicationTypes: ["signal"],
                          });
                        }

                        instance.clientCreated = true;
                        instances.forEach((i) => {
                          if (i.address === address && i.port === port) {
                            i.clientCreated = true;
                          }
                        });
                      }
                    }
                    return {};
                  }),
                ),
              ),
            );
        }

        return ctx;
      },
      "Handles the request to create a service instance",
    )
      .doOn("meta.create_service_requested")
      .then(this.insertServiceTask);
  }

  reset() {
    this.instances.clear();
    this.deputies.clear();
    this.remoteSignals.clear();
    this.remoteIntents.clear();
    this.remoteIntentDeputiesByKey.clear();
    this.remoteIntentDeputiesByTask.clear();
    this.dependeesByService.clear();
    this.dependeeByInstance.clear();
    this.readinessDependeesByService.clear();
    this.readinessDependeeByInstance.clear();
    this.lastHeartbeatAtByInstance.clear();
    this.missedHeartbeatsByInstance.clear();
    this.runtimeStatusFallbackInFlightByInstance.clear();
    this.activeRoutineExecutionIds.clear();
    this.numberOfRunningGraphs = 0;
    this.runtimeStatusHeartbeatStarted = false;
    this.lastRuntimeStatusSnapshot = null;
  }
}
