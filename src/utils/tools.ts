export function formatTimestamp(timestamp: number) {
  return new Date(timestamp).toISOString();
}

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
