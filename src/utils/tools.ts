/**
 * Formats a given timestamp into an ISO 8601 date string.
 *
 * @param {number} timestamp - The timestamp to format, represented as the number of milliseconds since the UNIX epoch.
 * @return {string} The formatted ISO 8601 string representing the date and time corresponding to the provided timestamp.
 */
export function formatTimestamp(timestamp: number) {
  return new Date(timestamp).toISOString();
}

/**
 * Decomposes a signal name into its constituent parts such as metadata indicator, source service name, domain, and action.
 *
 * @param {string} signalName - The signal name string to be decomposed. Typically a dot-separated string.
 * @return {Object} An object containing the decomposed parts:
 * - `isMeta` {boolean} - Indicates whether the signal is a metadata signal.
 * - `sourceServiceName` {string|null} - The name of the source service, or null if not applicable.
 * - `domain` {string} - The domain extracted from the signal name.
 * - `action` {string} - The action or last component of the signal name.
 */
export function decomposeSignalName(signalName: string) {
  const parts = signalName.split(".");
  const firstChar = signalName.charAt(0);
  let isMeta = false;
  let sourceServiceName = null;
  let domain = parts.length === 2 ? parts[0] : "";
  if (parts[0] === "meta") {
    isMeta = true;
    if (parts.length === 3) {
      domain = parts[1];
    } else {
      domain = "";
    }
  } else if (firstChar === "*" || firstChar === firstChar.toUpperCase()) {
    sourceServiceName = parts[0];

    if (parts[1] === "meta") {
      isMeta = true;
      if (parts.length === 4) {
        domain = parts[2];
      } else {
        domain = "";
      }
    } else {
      domain = parts[1];
    }
  }
  const action = parts[parts.length - 1];

  return {
    isMeta,
    sourceServiceName,
    domain,
    action,
  };
}
