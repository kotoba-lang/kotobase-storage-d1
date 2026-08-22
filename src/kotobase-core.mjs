const encoder = new TextEncoder();
const BASE32 = "abcdefghijklmnopqrstuvwxyz234567";
const GRAPH_CID = /^bafyrei[a-z2-7]{52}$/;

function base32LowerNoPad(bytes) {
  let bits = 0;
  let value = 0;
  let output = "";
  for (const byte of bytes) {
    value = (value << 8) | byte;
    bits += 8;
    while (bits >= 5) {
      bits -= 5;
      output += BASE32[(value >>> bits) & 31];
    }
  }
  if (bits > 0) output += BASE32[(value << (5 - bits)) & 31];
  return output;
}

export async function graphCidFromName(name) {
  const digest = new Uint8Array(
    await crypto.subtle.digest("SHA-256", encoder.encode(name))
  );
  const cid = new Uint8Array(36);
  cid.set([0x01, 0x71, 0x12, 0x20]);
  cid.set(digest, 4);
  return `b${base32LowerNoPad(cid)}`;
}

export function looksLikeGraphCid(value) {
  return typeof value === "string" && GRAPH_CID.test(value);
}

export function cacaoSiweMessage(cacao) {
  const payload = cacao?.p || {};
  const parts = String(payload.iss || "").split(":");
  const address = parts.at(-1) || payload.iss || "";
  const chainId = String(payload.iss || "").startsWith("did:key:")
    ? "1"
    : (parts.at(-2) || "1");
  const lines = [
    `${payload.domain || ""} wants you to sign in with your Ethereum account:`,
    address,
    ""
  ];
  if (payload.statement) lines.push(payload.statement, "");
  lines.push(
    `URI: ${payload.aud || ""}`,
    `Version: ${payload.version || "1"}`,
    `Chain ID: ${chainId}`,
    `Nonce: ${payload.nonce || ""}`,
    `Issued At: ${payload.iat || ""}`
  );
  if (payload.exp) lines.push(`Expiration Time: ${payload.exp}`);
  if (Array.isArray(payload.resources) && payload.resources.length > 0) {
    lines.push("Resources:");
    for (const resource of payload.resources) lines.push(`- ${resource}`);
  }
  return lines.join("\n");
}
