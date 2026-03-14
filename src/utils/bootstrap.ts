export interface BootstrapOptions {
  url?: string;
  injectedGlobalKey?: string;
}

export interface HydrationOptions {
  initialInquiryResults?: Record<string, unknown>;
}

export interface ResolvedBootstrapEndpoint {
  url: string;
  protocol: "http" | "https";
  address: string;
  port: number;
  exposed: boolean;
  injectedGlobalKey: string;
}

const DEFAULT_BOOTSTRAP_GLOBAL_KEY = "__CADENZA_RUNTIME__";

type ResolveBootstrapEndpointOptions = {
  runtime: "browser" | "server";
  bootstrap?: BootstrapOptions;
  cadenzaDB?: { address?: string; port?: number };
};

function normalizeString(value: unknown): string | undefined {
  if (typeof value !== "string") {
    return undefined;
  }

  const normalized = value.trim();
  return normalized.length > 0 ? normalized : undefined;
}

function readEnvString(name: string): string | undefined {
  if (typeof process === "undefined") {
    return undefined;
  }

  return normalizeString(process.env?.[name]);
}

function readConfiguredPort(value: unknown): number | undefined {
  if (value === undefined || value === null || value === "") {
    return undefined;
  }

  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    throw new Error(`Invalid port value: ${String(value)}`);
  }

  const normalized = Math.trunc(parsed);
  if (normalized <= 0) {
    throw new Error(`Port must be a positive integer: ${String(value)}`);
  }

  return normalized;
}

function buildBootstrapUrl(
  protocol: "http" | "https",
  address: string,
  port: number,
): string {
  return `${protocol}://${address}:${port}`;
}

function readExplicitPortFromOrigin(raw: string): number | undefined {
  const match = raw.match(
    /^[a-z]+:\/\/(?:\[[^\]]+\]|[^\/?#:]+):(\d+)(?:\/)?$/i,
  );

  if (!match?.[1]) {
    return undefined;
  }

  return readConfiguredPort(match[1]);
}

function resolveInjectedBootstrapUrl(injectedGlobalKey: string): string | undefined {
  if (typeof globalThis === "undefined") {
    return undefined;
  }

  const runtimeConfig = (globalThis as Record<string, any>)[injectedGlobalKey];
  return normalizeString(runtimeConfig?.bootstrapUrl);
}

function resolveExplicitBootstrapValue(options: ResolveBootstrapEndpointOptions): {
  value?: string;
  port?: number;
  injectedGlobalKey: string;
} {
  const injectedGlobalKey =
    normalizeString(options.bootstrap?.injectedGlobalKey) ??
    DEFAULT_BOOTSTRAP_GLOBAL_KEY;

  const explicitBootstrapUrl = normalizeString(options.bootstrap?.url);
  if (explicitBootstrapUrl) {
    return {
      value: explicitBootstrapUrl,
      port: readConfiguredPort(options.cadenzaDB?.port),
      injectedGlobalKey,
    };
  }

  if (options.runtime === "browser") {
    const injected = resolveInjectedBootstrapUrl(injectedGlobalKey);
    if (injected) {
      return {
        value: injected,
        port: readConfiguredPort(options.cadenzaDB?.port),
        injectedGlobalKey,
      };
    }
  }

  const explicitAddress = normalizeString(options.cadenzaDB?.address);
  if (explicitAddress) {
    return {
      value: explicitAddress,
      port: readConfiguredPort(options.cadenzaDB?.port),
      injectedGlobalKey,
    };
  }

  const envAddress = readEnvString("CADENZA_DB_ADDRESS");
  return {
    value: envAddress,
    port: readConfiguredPort(options.cadenzaDB?.port ?? readEnvString("CADENZA_DB_PORT")),
    injectedGlobalKey,
  };
}

export function readIntegerEnv(name: string, fallback: number): number {
  const raw = readEnvString(name);
  if (!raw) {
    return fallback;
  }

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

export function readStringEnv(name: string): string | undefined {
  return readEnvString(name);
}

export function readListEnv(name: string): string[][] {
  const raw = readEnvString(name);
  if (!raw) {
    return [];
  }

  return raw
    .split("|")
    .map((entry) => entry.trim())
    .filter((entry) => entry.length > 0)
    .map((entry) => entry.split(",").map((part) => part.trim()));
}

export function resolveBootstrapEndpoint(
  options: ResolveBootstrapEndpointOptions,
): ResolvedBootstrapEndpoint {
  const { value, port: fallbackPort, injectedGlobalKey } =
    resolveExplicitBootstrapValue(options);

  if (!value) {
    throw new Error(
      options.runtime === "browser"
        ? `No bootstrap URL available. Pass bootstrap.url or inject globalThis.${injectedGlobalKey}.bootstrapUrl.`
        : "No bootstrap URL available. Set CADENZA_DB_ADDRESS or pass bootstrap.url.",
    );
  }

  const raw = value.trim();
  if (raw.length === 0) {
    throw new Error("Bootstrap URL cannot be empty");
  }

  if (raw.includes("://")) {
    const parsed = new URL(raw);
    const protocol = parsed.protocol.replace(":", "");
    if (protocol !== "http" && protocol !== "https") {
      throw new Error(`Unsupported bootstrap protocol: ${parsed.protocol}`);
    }

    if (
      parsed.pathname &&
      parsed.pathname !== "/" &&
      parsed.pathname.trim().length > 0
    ) {
      throw new Error(
        "Bootstrap URL must be an origin without a path component.",
      );
    }

    const explicitPort = readExplicitPortFromOrigin(raw);
    const port = explicitPort ?? (parsed.port ? readConfiguredPort(parsed.port) : fallbackPort);
    if (!port) {
      throw new Error(
        "Bootstrap URL must include a port or CADENZA_DB_PORT must be provided.",
      );
    }

    return {
      url: buildBootstrapUrl(protocol, parsed.hostname, port),
      protocol,
      address: parsed.hostname,
      port,
      exposed: protocol === "https",
      injectedGlobalKey,
    };
  }

  if (raw.includes("/") || raw.includes("?") || raw.includes("#")) {
    throw new Error(
      "Bootstrap address without protocol must not include a path, query, or hash.",
    );
  }

  const parsed = new URL(`http://${raw}`);
  const protocol: "http" = "http";
  const port = parsed.port ? readConfiguredPort(parsed.port) : fallbackPort;
  if (!port) {
    throw new Error(
      "Bootstrap address must include a port or CADENZA_DB_PORT must be provided.",
    );
  }

  return {
    url: buildBootstrapUrl(protocol, parsed.hostname, port),
    protocol,
    address: parsed.hostname,
    port,
    exposed: false,
    injectedGlobalKey,
  };
}
