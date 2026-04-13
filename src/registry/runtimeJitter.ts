function normalizeKey(key: string): string {
  return key.trim() || "default";
}

export function hashKeyToUnitInterval(key: string): number {
  const normalizedKey = normalizeKey(key);
  let hash = 2166136261;

  for (let index = 0; index < normalizedKey.length; index += 1) {
    hash ^= normalizedKey.charCodeAt(index);
    hash = Math.imul(hash, 16777619);
  }

  return (hash >>> 0) / 0xffffffff;
}

export function normalizeJitterRatio(value: number): number {
  if (!Number.isFinite(value) || value <= 0) {
    return 0;
  }

  return Math.min(value, 1);
}

export function buildDeterministicJitterOffsetMs(
  baseMs: number,
  ratio: number,
  key: string,
): number {
  if (!Number.isFinite(baseMs) || baseMs <= 0) {
    return 0;
  }

  const normalizedRatio = normalizeJitterRatio(ratio);
  if (normalizedRatio <= 0) {
    return 0;
  }

  const maxOffsetMs = Math.round(baseMs * normalizedRatio);
  if (maxOffsetMs <= 0) {
    return 0;
  }

  return Math.round(hashKeyToUnitInterval(key) * maxOffsetMs);
}

export function buildDeterministicJitteredDelayMs(
  baseMs: number,
  ratio: number,
  key: string,
): number {
  return Math.max(
    0,
    Math.round(baseMs) + buildDeterministicJitterOffsetMs(baseMs, ratio, key),
  );
}
