import { ed25519 } from "@noble/curves/ed25519";
import * as dagCbor from "@ipld/dag-cbor";
import { base58btc } from "multiformats/bases/base58";
import { cacaoSiweMessage } from "../src/kotobase-core.mjs";

const candidate = process.env.KOTOBASE_R2_URL || "https://candidate-storage.aozora.app";
const rollback = process.env.KOTOBASE_D1_URL || "https://kotobase-storage-d1.aozora.app";
const entities = Number(process.env.KOTOBASE_R2_QUALIFY_ENTITIES || 100);
const samples = Number(process.env.KOTOBASE_R2_QUALIFY_SAMPLES || 8);
const gateMs = Number(process.env.KOTOBASE_R2_QUALIFY_P95_MS || 5000);
const observedColos = new Map();
const encoder = new TextEncoder();
const secret = ed25519.utils.randomSecretKey();
const pub = ed25519.getPublicKey(secret);
const prefixed = new Uint8Array(34);
prefixed.set([0xed, 0x01]);
prefixed.set(pub, 2);
const did = `did:key:${base58btc.encode(prefixed)}`;
const ref = `kotobase/db/${did}/r2-qualify-${crypto.randomUUID()}`;

function base64(bytes) {
  return Buffer.from(bytes).toString("base64");
}

function instant(offsetSeconds = 0) {
  const value = new Date(Date.now() + offsetSeconds * 1000);
  value.setMilliseconds(0);
  return value.toISOString().replace(".000Z", "Z");
}

function cacao(endpoint, capability) {
  const payload = {
    iss: did,
    domain: new URL(endpoint).host,
    aud: endpoint,
    version: "1",
    nonce: crypto.randomUUID(),
    iat: instant(),
    exp: instant(300),
    resources: [capability]
  };
  const signature = ed25519.sign(encoder.encode(cacaoSiweMessage({ p: payload })), secret);
  return `CACAO ${base64(dagCbor.encode({
    h: { t: "eip4361" },
    p: payload,
    s: { t: "EdDSA", s: base64(signature) }
  }))}`;
}

async function call(endpoint, path, capability, body) {
  const started = performance.now();
  const response = await fetch(`${endpoint}${path}`, {
    method: "POST",
    headers: {
      "content-type": "application/edn",
      authorization: cacao(endpoint, capability),
      "x-kotobase-ref": ref,
      "x-request-id": crypto.randomUUID()
    },
    body
  });
  const text = await response.text();
  const ms = Math.round((performance.now() - started) * 10) / 10;
  const host = new URL(endpoint).host;
  const colo = response.headers.get("cf-ray")?.split("-").at(-1) || "unknown";
  if (!observedColos.has(host)) observedColos.set(host, new Set());
  observedColos.get(host).add(colo);
  if (!response.ok) throw new Error(`${new URL(endpoint).host}${path}: ${response.status} ${text}`);
  return { text, ms };
}

function percentile(values, p) {
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.min(sorted.length - 1, Math.ceil(sorted.length * p) - 1)];
}

async function qualifyReads(endpoint, expectedCount) {
  const queries = {
    point: `{:query [:find ?e . :where [?e :bench/email "user-${expectedCount - 1}@example.test"]]}`,
    count: "{:query [:find (count ?e) . :where [?e :bench/email _]]}",
    join: "{:query [:find (count ?e) . :where [?e :bench/role \"admin\"] [?e :bench/name ?name]]}"
  };
  const result = {};
  for (const [name, query] of Object.entries(queries)) {
    const latency = [];
    let last;
    for (let index = 0; index < samples; index += 1) {
      const response = await call(endpoint, "/api/q", "kotoba://can/graph:query", query);
      last = response.text;
      if (index > 0) latency.push(response.ms);
    }
    if (name === "count" && last !== String(expectedCount)) {
      throw new Error(`${new URL(endpoint).host} count ${last} != ${expectedCount}`);
    }
    result[name] = {
      samples: latency.length,
      p50_ms: percentile(latency, 0.50),
      p95_ms: percentile(latency, 0.95)
    };
  }
  return result;
}

const txEntities = Array.from({ length: entities }, (_, index) =>
  `{:db/id "u${index}" :bench/email "user-${index}@example.test" ` +
  `:bench/name "User ${index}" :bench/role "${index % 10 === 0 ? "admin" : "member"}"}`
).join(" ");
const tx = await call(
  candidate,
  "/api/transact",
  "kotoba://can/datom:transact",
  `{:tx-data [${txEntities}]}`
);
const r2 = await qualifyReads(candidate, entities);
const d1Rollback = await qualifyReads(rollback, entities);
const p95Values = Object.values(r2).map((entry) => entry.p95_ms);
const pass = tx.ms <= gateMs && p95Values.every((value) => value <= gateMs);
const evidence = {
  version: 1,
  checked_at: new Date().toISOString(),
  entities,
  samples,
  candidate: new URL(candidate).host,
  rollback: new URL(rollback).host,
  transaction_ms: tx.ms,
  r2,
  d1_rollback: d1Rollback,
  observed_colos: Object.fromEntries(
    [...observedColos].map(([host, values]) => [host, [...values].sort()])
  ),
  gate_p95_ms: gateMs,
  pass
};
console.log(JSON.stringify(evidence));
if (!pass) process.exitCode = 1;
