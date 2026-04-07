export interface RuntimeMetricsSnapshot {
  sampledAt: string;
  cpuUsage: number | null;
  memoryUsage: number | null;
  eventLoopLag: number | null;
  rssBytes: number | null;
  heapUsedBytes: number | null;
  heapTotalBytes: number | null;
  memoryLimitBytes: number | null;
}

interface MonitorEventLoopDelayHistogram {
  enable(): void;
  disable(): void;
  percentile(percentile: number): number;
}

interface RuntimeMetricsSamplerDependencies {
  nowMs: () => number;
  cpuUsage: () => NodeJS.CpuUsage;
  memoryUsage: () => NodeJS.MemoryUsage;
  effectiveCpuCapacity: () => number;
  effectiveMemoryLimitBytes: () => number | null;
  createEventLoopMonitor: () => MonitorEventLoopDelayHistogram | null;
}

const CPU_MAX = 1;
const MAX_CGROUP_LIMIT_BYTES = 1n << 60n;

function readTextFile(path: string): string | null {
  try {
    const fs = process.getBuiltinModule?.("node:fs") ??
      process.getBuiltinModule?.("fs") as
      | { readFileSync(path: string, encoding: string): string }
      | undefined;
    if (!fs) {
      return null;
    }

    return fs.readFileSync(path, "utf8").trim();
  } catch {
    return null;
  }
}

function readInteger(value: string | null): bigint | null {
  if (!value || value === "max") {
    return null;
  }

  try {
    return BigInt(value);
  } catch {
    return null;
  }
}

function resolveEffectiveCpuCapacity(): number {
  const cpuMax = readTextFile("/sys/fs/cgroup/cpu.max");
  if (cpuMax) {
    const [quotaText, periodText] = cpuMax.split(/\s+/);
    const quota = readInteger(quotaText);
    const period = readInteger(periodText);
    if (quota !== null && period !== null && quota > 0n && period > 0n) {
      return Math.max(1, Number(quota) / Number(period));
    }
  }

  const quota = readInteger(
    readTextFile("/sys/fs/cgroup/cpu/cpu.cfs_quota_us"),
  );
  const period = readInteger(
    readTextFile("/sys/fs/cgroup/cpu/cpu.cfs_period_us"),
  );
  if (quota !== null && period !== null && quota > 0n && period > 0n) {
    return Math.max(1, Number(quota) / Number(period));
  }

  const os = process.getBuiltinModule?.("node:os") ??
    process.getBuiltinModule?.("os") as
    | {
        availableParallelism?: () => number;
        cpus: () => unknown[];
      }
    | undefined;

  if (!os) {
    return 1;
  }

  const availableParallelism =
    typeof os.availableParallelism === "function"
      ? os.availableParallelism()
      : Array.isArray(os.cpus())
        ? os.cpus().length
        : 1;

  return Math.max(1, availableParallelism || 1);
}

function resolveEffectiveMemoryLimitBytes(): number | null {
  const cgroupV2Limit = readInteger(readTextFile("/sys/fs/cgroup/memory.max"));
  if (
    cgroupV2Limit !== null &&
    cgroupV2Limit > 0n &&
    cgroupV2Limit < MAX_CGROUP_LIMIT_BYTES
  ) {
    return Number(cgroupV2Limit);
  }

  const cgroupV1Limit = readInteger(
    readTextFile("/sys/fs/cgroup/memory/memory.limit_in_bytes"),
  );
  if (
    cgroupV1Limit !== null &&
    cgroupV1Limit > 0n &&
    cgroupV1Limit < MAX_CGROUP_LIMIT_BYTES
  ) {
    return Number(cgroupV1Limit);
  }

  try {
    const os = process.getBuiltinModule?.("node:os") ??
      process.getBuiltinModule?.("os") as
      | { totalmem: () => number }
      | undefined;
    if (!os) {
      return null;
    }

    const total = Number(os.totalmem());
    return Number.isFinite(total) && total > 0 ? total : null;
  } catch {
    return null;
  }
}

function createEventLoopMonitor(): MonitorEventLoopDelayHistogram | null {
  try {
    const perfHooks = process.getBuiltinModule?.("node:perf_hooks") ??
      process.getBuiltinModule?.("perf_hooks") as
      | {
          monitorEventLoopDelay: (options?: {
            resolution?: number;
          }) => MonitorEventLoopDelayHistogram;
        }
      | undefined;
    if (!perfHooks?.monitorEventLoopDelay) {
      return null;
    }

    const histogram = perfHooks.monitorEventLoopDelay({ resolution: 20 });
    histogram.enable();
    return histogram;
  } catch {
    return null;
  }
}

function defaultDependencies(): RuntimeMetricsSamplerDependencies {
  return {
    nowMs: () => Date.now(),
    cpuUsage: () => process.cpuUsage(),
    memoryUsage: () => process.memoryUsage(),
    effectiveCpuCapacity: () => resolveEffectiveCpuCapacity(),
    effectiveMemoryLimitBytes: () => resolveEffectiveMemoryLimitBytes(),
    createEventLoopMonitor: () => createEventLoopMonitor(),
  };
}

export function isNodeRuntimeMetricsSupported(): boolean {
  return Boolean(
    typeof process !== "undefined" &&
      process?.versions?.node &&
      typeof process.cpuUsage === "function" &&
      typeof process.memoryUsage === "function",
  );
}

export class RuntimeMetricsSampler {
  private readonly dependencies: RuntimeMetricsSamplerDependencies;
  private readonly eventLoopMonitor: MonitorEventLoopDelayHistogram | null;
  private previousCpuUsage: NodeJS.CpuUsage | null = null;
  private previousSampleAtMs: number | null = null;
  private lastSnapshot: RuntimeMetricsSnapshot | null = null;

  public constructor(
    dependencies: Partial<RuntimeMetricsSamplerDependencies> = {},
  ) {
    this.dependencies = {
      ...defaultDependencies(),
      ...dependencies,
    };
    this.eventLoopMonitor = this.dependencies.createEventLoopMonitor();
  }

  public sample(): RuntimeMetricsSnapshot {
    const sampledAtMs = this.dependencies.nowMs();
    const sampledAt = new Date(sampledAtMs).toISOString();
    const cpuUsage = this.dependencies.cpuUsage();
    const memoryUsage = this.dependencies.memoryUsage();
    const effectiveCpuCapacity = Math.max(
      1,
      this.dependencies.effectiveCpuCapacity(),
    );
    const memoryLimitBytes = this.dependencies.effectiveMemoryLimitBytes();

    let normalizedCpuUsage: number | null = null;
    if (
      this.previousCpuUsage &&
      this.previousSampleAtMs !== null &&
      sampledAtMs > this.previousSampleAtMs
    ) {
      const elapsedMicros = (sampledAtMs - this.previousSampleAtMs) * 1000;
      const cpuMicros =
        cpuUsage.user -
        this.previousCpuUsage.user +
        (cpuUsage.system - this.previousCpuUsage.system);
      if (elapsedMicros > 0 && cpuMicros >= 0) {
        normalizedCpuUsage = Math.max(
          0,
          Math.min(CPU_MAX, cpuMicros / (elapsedMicros * effectiveCpuCapacity)),
        );
      }
    }

    this.previousCpuUsage = cpuUsage;
    this.previousSampleAtMs = sampledAtMs;

    const rssBytes = Number.isFinite(memoryUsage.rss) ? memoryUsage.rss : null;
    const heapUsedBytes = Number.isFinite(memoryUsage.heapUsed)
      ? memoryUsage.heapUsed
      : null;
    const heapTotalBytes = Number.isFinite(memoryUsage.heapTotal)
      ? memoryUsage.heapTotal
      : null;
    const normalizedMemoryUsage =
      rssBytes !== null && memoryLimitBytes && memoryLimitBytes > 0
        ? Math.max(0, Math.min(1, rssBytes / memoryLimitBytes))
        : null;

    const eventLoopLag =
      this.eventLoopMonitor && Number.isFinite(this.eventLoopMonitor.percentile(95))
        ? Number((this.eventLoopMonitor.percentile(95) / 1_000_000).toFixed(3))
        : null;

    const snapshot: RuntimeMetricsSnapshot = {
      sampledAt,
      cpuUsage:
        normalizedCpuUsage === null
          ? null
          : Number(normalizedCpuUsage.toFixed(4)),
      memoryUsage:
        normalizedMemoryUsage === null
          ? null
          : Number(normalizedMemoryUsage.toFixed(4)),
      eventLoopLag,
      rssBytes,
      heapUsedBytes,
      heapTotalBytes,
      memoryLimitBytes,
    };

    this.lastSnapshot = snapshot;
    return snapshot;
  }

  public getLatestSnapshot(): RuntimeMetricsSnapshot | null {
    return this.lastSnapshot;
  }

  public stop(): void {
    this.eventLoopMonitor?.disable();
  }
}
