import type { AnyObject } from "@cadenza.io/core";
import { v4 as uuid } from "uuid";
import type { BootstrapOptions, HydrationOptions } from "../utils/bootstrap";
import { resolveBootstrapEndpoint } from "../utils/bootstrap";
import type {
  DistributedInquiryMeta,
  DistributedInquiryOptions,
  InquiryResponderStatus,
} from "../types/inquiry";
import type { ServiceInstanceDescriptor } from "../types/serviceRegistry";
import {
  isMetaIntentName,
  mergeInquiryContexts,
  summarizeResponderStatuses,
} from "../utils/inquiry";
import { normalizeServiceInstanceDescriptor } from "../utils/serviceInstance";
import { selectTransportForRole } from "../utils/transport";

export interface SSRInquiryBridgeOptions {
  bootstrap?: BootstrapOptions;
  cadenzaDB?: {
    address?: string;
    port?: number;
  };
}

export interface SSRInquiryBridge {
  inquire: (
    inquiry: string,
    context?: AnyObject,
    options?: DistributedInquiryOptions,
  ) => Promise<AnyObject>;
  dehydrate: () => HydrationOptions;
}

type NormalizedIntentMap = {
  intentName: string;
  serviceName: string;
  taskName: string;
  taskVersion: number;
  deleted: boolean;
};

function ensureFetch(): typeof fetch {
  if (typeof globalThis.fetch !== "function") {
    throw new Error("SSR inquiry bridge requires global fetch support.");
  }

  return globalThis.fetch.bind(globalThis);
}

function normalizeArrayResponse(
  value: AnyObject,
  keys: string[],
): AnyObject[] {
  for (const key of keys) {
    if (Array.isArray((value as any)?.[key])) {
      return (value as any)[key];
    }
  }

  if (Array.isArray((value as any)?.rows)) {
    return (value as any).rows;
  }

  if (Array.isArray((value as any)?.data)) {
    return (value as any).data;
  }

  const joinedContexts = Array.isArray((value as any)?.joinedContexts)
    ? ((value as any).joinedContexts as AnyObject[])
    : [];
  for (let index = joinedContexts.length - 1; index >= 0; index -= 1) {
    const nested = joinedContexts[index];
    if (!nested || typeof nested !== "object") {
      continue;
    }

    const rows = normalizeArrayResponse(nested as AnyObject, keys);
    if (rows.length > 0) {
      return rows;
    }
  }

  return [];
}

function buildQueryResponseKeys(tableName: string): string[] {
  const camelCased = tableName.replace(/_([a-z])/g, (_match, char) =>
    char.toUpperCase(),
  );

  return [
    `${tableName}Rows`,
    `${tableName}s`,
    tableName,
    `${camelCased}s`,
    camelCased,
  ];
}

function normalizeIntentMap(raw: AnyObject): NormalizedIntentMap | null {
  const intentName = String(raw.intentName ?? raw.intent_name ?? "").trim();
  const serviceName = String(raw.serviceName ?? raw.service_name ?? "").trim();
  const taskName = String(raw.taskName ?? raw.task_name ?? "").trim();
  const taskVersion = Math.max(
    1,
    Math.trunc(Number(raw.taskVersion ?? raw.task_version ?? 1) || 1),
  );

  if (!intentName || !serviceName || !taskName) {
    return null;
  }

  return {
    intentName,
    serviceName,
    taskName,
    taskVersion,
    deleted: Boolean(raw.deleted),
  };
}

function buildInquiryMeta(
  inquiry: string,
  startedAt: number,
  statuses: InquiryResponderStatus[],
): DistributedInquiryMeta {
  const counts = summarizeResponderStatuses(statuses);
  return {
    inquiry,
    isMetaInquiry: isMetaIntentName(inquiry),
    totalResponders: statuses.length,
    eligibleResponders: statuses.length,
    filteredOutResponders: 0,
    responded: counts.responded,
    failed: counts.failed,
    timedOut: counts.timedOut,
    pending: counts.pending,
    durationMs: Date.now() - startedAt,
    responders: statuses,
  };
}

export function createSSRInquiryBridge(
  options: SSRInquiryBridgeOptions = {},
): SSRInquiryBridge {
  const bootstrapEndpoint = resolveBootstrapEndpoint({
    runtime: "server",
    bootstrap: options.bootstrap,
    cadenzaDB: options.cadenzaDB,
  });
  const fetchImplementation = ensureFetch();
  const initialInquiryResults: Record<string, unknown> = {};

  const postDelegation = async (
    targetUrl: string,
    remoteRoutineName: string,
    context: AnyObject,
    timeoutMs: number,
  ): Promise<AnyObject> => {
    const signal = AbortSignal.timeout(timeoutMs);
    const response = await fetchImplementation(`${targetUrl}/delegation`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        ...context,
        __remoteRoutineName: remoteRoutineName,
        __metadata: {
          ...(context.__metadata ?? {}),
          __deputyExecId: uuid(),
        },
      }),
      signal,
    });

    return (await response.json()) as AnyObject;
  };

  const queryTable = async (
    tableName: string,
    queryData: AnyObject,
    timeoutMs: number,
  ): Promise<AnyObject[]> => {
    const response = await postDelegation(
      bootstrapEndpoint.url,
      `Query ${tableName}`,
      { queryData },
      timeoutMs,
    );
    return normalizeArrayResponse(response, buildQueryResponseKeys(tableName));
  };

  return {
    async inquire(
      inquiry: string,
      context: AnyObject = {},
      inquiryOptions: DistributedInquiryOptions = {},
    ): Promise<AnyObject> {
      const startedAt = Date.now();
      const overallTimeoutMs =
        inquiryOptions.overallTimeoutMs ?? inquiryOptions.timeout ?? 30_000;
      const perResponderTimeoutMs =
        inquiryOptions.perResponderTimeoutMs ?? overallTimeoutMs;

      const intentMaps = (
        await queryTable(
          "intent_to_task_map",
          {
            filter: {
              intent_name: inquiry,
            },
          },
          overallTimeoutMs,
        )
      )
        .map(normalizeIntentMap)
        .filter(
          (item): item is NormalizedIntentMap =>
            !!item && item.intentName === inquiry && !item.deleted,
        );

      if (intentMaps.length === 0) {
        return {
          __inquiryMeta: buildInquiryMeta(inquiry, startedAt, []),
        };
      }

      const relevantServiceNames = Array.from(
        new Set(intentMaps.map((item) => item.serviceName)),
      );

      const rawServiceInstances = (
        await queryTable(
          "service_instance",
          {
            filter: {
              service_name: relevantServiceNames,
            },
          },
          overallTimeoutMs,
        )
      );

      const rawServiceTransports = await queryTable(
        "service_instance_transport",
        {
          filter: {
            deleted: false,
          },
        },
        overallTimeoutMs,
      );

      const transportsByInstance = new Map<string, AnyObject[]>();
      for (const transport of rawServiceTransports) {
        const serviceInstanceId = String(
          transport.serviceInstanceId ?? transport.service_instance_id ?? "",
        ).trim();
        if (!serviceInstanceId) {
          continue;
        }

        if (!transportsByInstance.has(serviceInstanceId)) {
          transportsByInstance.set(serviceInstanceId, []);
        }
        transportsByInstance.get(serviceInstanceId)!.push(transport);
      }

      const serviceInstances = rawServiceInstances
        .map((instance) =>
          normalizeServiceInstanceDescriptor({
            ...instance,
            transports:
              transportsByInstance.get(String(instance.uuid ?? "").trim()) ?? [],
          }),
        )
        .filter(
          (item): item is ServiceInstanceDescriptor =>
            !!item &&
            relevantServiceNames.includes(item.serviceName) &&
            item.isActive &&
            !item.isNonResponsive &&
            !item.isBlocked,
        )
        .sort((left, right) => {
          if (left.serviceName !== right.serviceName) {
            return left.serviceName.localeCompare(right.serviceName);
          }
          if (left.isPrimary !== right.isPrimary) {
            return left.isPrimary ? -1 : 1;
          }
          return (
            (left.numberOfRunningGraphs ?? 0) - (right.numberOfRunningGraphs ?? 0)
          );
        });

      const statuses: InquiryResponderStatus[] = intentMaps.map((map) => ({
        isRemote: true,
        serviceName: map.serviceName,
        taskName: map.taskName,
        taskVersion: map.taskVersion,
        localTaskName: `SSR inquiry via ${map.serviceName} (${map.taskName} v${map.taskVersion})`,
        status: "timed_out",
        durationMs: 0,
      }));

      const fulfilledContexts = await Promise.all(
        intentMaps.map(async (map, index) => {
          const status = statuses[index];
          const startedAtForResponder = Date.now();
          const selectedInstance = serviceInstances.find(
            (instance) =>
              instance.serviceName === map.serviceName &&
              !!selectTransportForRole(
                instance.transports,
                "internal",
                "rest",
              ),
          );

          if (!selectedInstance) {
            status.status = "failed";
            status.error = `No routeable internal instances for ${map.serviceName}`;
            status.durationMs = Date.now() - startedAtForResponder;
            return null;
          }

          const targetTransport = selectTransportForRole(
            selectedInstance.transports,
            "internal",
            "rest",
          );
          if (!targetTransport) {
            status.status = "failed";
            status.error = `No internal transport for ${selectedInstance.serviceName}/${selectedInstance.uuid}`;
            status.durationMs = Date.now() - startedAtForResponder;
            return null;
          }

          const targetUrl = targetTransport.origin;

          try {
            const result = await postDelegation(
              targetUrl,
              map.taskName,
              context,
              perResponderTimeoutMs,
            );
            status.durationMs = Date.now() - startedAtForResponder;
            if (result?.errored || result?.failed) {
              status.status = "failed";
              status.error = String(
                result?.__error ?? result?.error ?? "Remote inquiry failed",
              );
              return null;
            }

            status.status = "fulfilled";
            return result;
          } catch (error) {
            status.durationMs = Date.now() - startedAtForResponder;
            if (
              error instanceof Error &&
              (error.name === "AbortError" || /timed out/i.test(error.message))
            ) {
              status.status = "timed_out";
              status.error = error.message;
              return null;
            }

            status.status = "failed";
            status.error =
              error instanceof Error ? error.message : String(error);
            return null;
          }
        }),
      );

      const mergedContext = mergeInquiryContexts(
        fulfilledContexts.filter(
          (item): item is AnyObject => item !== null,
        ),
      );
      const responseContext = {
        ...mergedContext,
        __inquiryMeta: buildInquiryMeta(inquiry, startedAt, statuses),
      };

      if (inquiryOptions.hydrationKey) {
        initialInquiryResults[inquiryOptions.hydrationKey] = responseContext;
      }

      if (
        inquiryOptions.requireComplete &&
        statuses.some((status) => status.status !== "fulfilled")
      ) {
        throw {
          ...responseContext,
          __error: `Inquiry '${inquiry}' did not complete successfully`,
          errored: true,
        };
      }

      return responseContext;
    },

    dehydrate(): HydrationOptions {
      return {
        initialInquiryResults: { ...initialInquiryResults },
      };
    },
  };
}
