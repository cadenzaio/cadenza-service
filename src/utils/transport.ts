import type {
  ServiceTransportConfig,
  ServiceTransportDescriptor,
  ServiceTransportProtocol,
  ServiceTransportRole,
  ServiceTransportSecurityProfile,
} from "../types/transport";

const DEFAULT_PROTOCOLS: ServiceTransportProtocol[] = ["rest", "socket"];
const TRANSPORT_HANDLE_ROUTE_KEY_BY_HANDLE = new Map<string, string>();

function normalizeString(value: unknown): string {
  return typeof value === "string" ? value.trim() : "";
}

function hashString(value: string): string {
  let hash = 2166136261;

  for (let index = 0; index < value.length; index += 1) {
    hash ^= value.charCodeAt(index);
    hash = Math.imul(hash, 16777619);
  }

  return (hash >>> 0).toString(36);
}

export function normalizeTransportProtocols(
  value: unknown,
): ServiceTransportProtocol[] {
  const rawValues = Array.isArray(value)
    ? value
    : typeof value === "string"
      ? value.split(",")
      : [];

  const normalized = rawValues
    .map((entry) => normalizeString(entry))
    .filter(
      (entry): entry is ServiceTransportProtocol =>
        entry === "rest" || entry === "socket",
    );

  return Array.from(new Set(normalized));
}

export function normalizeTransportOrigin(origin: unknown): string | null {
  const raw = normalizeString(origin);
  if (!raw) {
    return null;
  }

  let parsed: URL;
  try {
    parsed = new URL(raw);
  } catch {
    return null;
  }

  if (parsed.protocol !== "http:" && parsed.protocol !== "https:") {
    return null;
  }

  if (parsed.pathname && parsed.pathname !== "/") {
    return null;
  }

  if (parsed.search || parsed.hash) {
    return null;
  }

  return parsed.origin;
}

function normalizeSecurityProfile(
  value: unknown,
): ServiceTransportSecurityProfile | null {
  const normalized = normalizeString(value);
  if (
    normalized === "low" ||
    normalized === "medium" ||
    normalized === "high"
  ) {
    return normalized;
  }

  return null;
}

export function normalizeServiceTransportConfig(
  value: unknown,
): ServiceTransportConfig | null {
  const raw = (value ?? {}) as Record<string, unknown>;
  const role = normalizeString(raw.role) as ServiceTransportRole;
  const origin = normalizeTransportOrigin(raw.origin);
  const protocols = normalizeTransportProtocols(raw.protocols);

  if (!origin) {
    return null;
  }

  if (role !== "internal" && role !== "public") {
    return null;
  }

  return {
    role,
    origin,
    protocols: protocols.length > 0 ? protocols : [...DEFAULT_PROTOCOLS],
    securityProfile: normalizeSecurityProfile(raw.securityProfile),
    authStrategy: normalizeString(raw.authStrategy) || null,
  };
}

export function normalizeServiceTransportDescriptor(
  value: unknown,
): ServiceTransportDescriptor | null {
  const raw = (value ?? {}) as Record<string, unknown>;
  const uuid = normalizeString(raw.uuid);
  const serviceInstanceId = normalizeString(
    raw.serviceInstanceId ?? raw.service_instance_id,
  );
  const config = normalizeServiceTransportConfig(raw);

  if (!uuid || !serviceInstanceId || !config) {
    return null;
  }

  return {
    uuid,
    serviceInstanceId,
    role: config.role,
    origin: config.origin,
    protocols: config.protocols ?? [...DEFAULT_PROTOCOLS],
    securityProfile: config.securityProfile ?? null,
    authStrategy: config.authStrategy ?? null,
    deleted: Boolean(raw.deleted),
    clientCreated: Boolean(raw.clientCreated ?? raw.client_created ?? false),
  };
}

export function transportSupportsProtocol(
  transport: ServiceTransportDescriptor | null | undefined,
  protocol: ServiceTransportProtocol,
): boolean {
  return !!transport && transport.protocols.includes(protocol);
}

export function selectTransportForRole(
  transports: ServiceTransportDescriptor[],
  role: ServiceTransportRole,
  protocol?: ServiceTransportProtocol,
): ServiceTransportDescriptor | undefined {
  const filtered = transports.filter(
    (transport) =>
      !transport.deleted &&
      transport.role === role &&
      (!protocol || transportSupportsProtocol(transport, protocol)),
  );

  return filtered.sort((left, right) => {
    const leftIsBootstrap = left.uuid.endsWith("-bootstrap") ? 1 : 0;
    const rightIsBootstrap = right.uuid.endsWith("-bootstrap") ? 1 : 0;
    if (leftIsBootstrap !== rightIsBootstrap) {
      return leftIsBootstrap - rightIsBootstrap;
    }

    return left.origin.localeCompare(right.origin);
  })[0];
}

export function buildTransportClientKey(
  transport: Pick<ServiceTransportDescriptor, "uuid" | "role" | "origin">,
  serviceName?: string,
): string {
  return buildTransportRouteKey(transport, serviceName);
}

export function buildTransportRouteKey(
  transport: Pick<ServiceTransportDescriptor, "uuid" | "role" | "origin">,
  serviceName?: string,
): string {
  const normalizedServiceName =
    typeof serviceName === "string" ? serviceName.trim() : "";
  const normalizedOrigin = normalizeTransportOrigin(transport.origin);

  if (normalizedServiceName && transport.role && normalizedOrigin) {
    return `${normalizedServiceName}|${transport.role}|${normalizedOrigin}`;
  }

  return transport.uuid;
}

export function buildTransportHandleKey(
  routeKey: string,
  protocol: ServiceTransportProtocol,
): string {
  const normalizedRouteKey = String(routeKey ?? "").trim();
  if (!normalizedRouteKey) {
    return protocol;
  }

  const servicePrefix =
    normalizedRouteKey.split("|")[0]?.trim().slice(0, 32) || "route";
  const handleKey = `${servicePrefix}|${hashString(normalizedRouteKey)}|${protocol}`;
  TRANSPORT_HANDLE_ROUTE_KEY_BY_HANDLE.set(handleKey, normalizedRouteKey);
  return handleKey;
}

export function parseTransportHandleKey(
  value: unknown,
): {
  routeKey: string;
  protocol: ServiceTransportProtocol | null;
} | null {
  const normalized = normalizeString(value);
  if (!normalized) {
    return null;
  }

  if (normalized.endsWith("|rest")) {
    return {
      routeKey: TRANSPORT_HANDLE_ROUTE_KEY_BY_HANDLE.get(normalized) ?? normalized.slice(0, -5),
      protocol: "rest",
    };
  }

  if (normalized.endsWith("|socket")) {
    return {
      routeKey:
        TRANSPORT_HANDLE_ROUTE_KEY_BY_HANDLE.get(normalized) ?? normalized.slice(0, -7),
      protocol: "socket",
    };
  }

  return {
    routeKey: normalized,
    protocol: null,
  };
}

export function parseTransportOrigin(
  origin: string,
): { protocol: "http" | "https"; hostname: string; port: number } | null {
  const normalized = normalizeTransportOrigin(origin);
  if (!normalized) {
    return null;
  }

  const parsed = new URL(normalized);
  const protocol = parsed.protocol === "https:" ? "https" : "http";
  const port = parsed.port
    ? Number(parsed.port)
    : protocol === "https"
      ? 443
      : 80;

  return {
    protocol,
    hostname: parsed.hostname,
    port,
  };
}
