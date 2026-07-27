/**
 * Load / SLO qualification for D1 Client API.
 * Uses the same CACAO helpers as verify.mjs; fails non-zero if any gate trips.
 */
import { ed25519 } from "@noble/curves/ed25519";
import * as dagCbor from "@ipld/dag-cbor";
import { base58btc } from "multiformats/bases/base58";
import { cacaoSiweMessage } from "../../../gftdcojp/net-kotobase/worker/js/kotobase-core.js";

const endpoint = process.env.KOTOBASE_D1_URL ||
  "https://kotobase-storage-d1.aozora.app";
const encoder = new TextEncoder();
const secret = ed25519.utils.randomSecretKey();
const pub = ed25519.getPublicKey(secret);
const prefixed = new Uint8Array(34);
prefixed.set([0xed, 0x01]);
prefixed.set(pub, 2);
const did = `did:key:${base58btc.encode(prefixed)}`;

const gates = {
  pointP50Ms: Number(process.env.KOTOBASE_SLO_POINT_P50_MS || 80),
  countP50Ms: Number(process.env.KOTOBASE_SLO_COUNT_P50_MS || 100),
  joinP50Ms: Number(process.env.KOTOBASE_SLO_JOIN_P50_MS || 150),
  txMs: Number(process.env.KOTOBASE_SLO_TX_MS || 1500),
  reindexMs: Number(process.env.KOTOBASE_SLO_REINDEX_MS || 5000),
  entities: Number(process.env.KOTOBASE_SLO_ENTITIES || 1000)
};

function base64(bytes) {
  return Buffer.from(bytes).toString("base64");
}
function instant(offsetSeconds = 0) {
  const value = new Date(Date.now() + offsetSeconds * 1000);
  value.setMilliseconds(0);
  return value.toISOString().replace(".000Z", "Z");
}
function cacao(capability) {
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
  const message = cacaoSiweMessage({ p: payload });
  const signature = ed25519.sign(encoder.encode(message), secret);
  return `CACAO ${base64(dagCbor.encode({
    h: { t: "eip4361" },
    p: payload,
    s: { t: "EdDSA", s: base64(signature) }
  }))}`;
}

async function call(path, { auth, ref, body }) {
  const started = performance.now();
  const response = await fetch(`${endpoint}${path}`, {
    method: "POST",
    headers: {
      "content-type": "application/edn",
      authorization: auth,
      "x-kotobase-ref": ref,
      "x-request-id": crypto.randomUUID()
    },
    body
  });
  const text = await response.text();
  return {
    status: response.status,
    body: text,
    ms: performance.now() - started
  };
}

function p50(samples) {
  const sorted = [...samples].sort((a, b) => a - b);
  return sorted[Math.floor((sorted.length - 1) * 0.5)];
}

function gate(label, actual, limit) {
  const ok = actual <= limit;
  console.log(`${ok ? "ok" : "FAIL"} - ${label}: ${actual.toFixed(1)}ms (gate ≤ ${limit}ms)`);
  if (!ok) throw new Error(`SLO gate failed: ${label}`);
}

const txCap = "kotoba://can/datom:transact";
const readCap = "kotoba://can/graph:query";
const ref = `kotobase/db/${did}/slo-${crypto.randomUUID()}`;
const n = gates.entities;

const entities = Array.from({ length: n }, (_, i) =>
  `{:db/id "u${i}" :bench/email "user-${i}@example.test" :bench/name "User ${i}" :bench/role "${i % 10 === 0 ? "admin" : "member"}"}`
).join(" ");

const tx = await call("/api/transact", {
  auth: cacao(txCap),
  ref,
  body: `{:tx-data [${entities}]}`
});
if (tx.status !== 200) throw new Error(`transact failed: ${tx.status} ${tx.body}`);
gate("transact wall", tx.ms, gates.txMs);

const reindex = await call("/v1/reindex", {
  auth: cacao(txCap),
  ref,
  body: "{}"
});
if (reindex.status !== 200) {
  throw new Error(`reindex failed: ${reindex.status} ${reindex.body}`);
}
gate("cold reindex wall", reindex.ms, gates.reindexMs);

async function sample(name, body, samples = 8) {
  const times = [];
  let last;
  for (let i = 0; i < samples; i += 1) {
    last = await call("/api/q", { auth: cacao(readCap), ref, body });
    if (last.status !== 200) {
      throw new Error(`${name} failed: ${last.status} ${last.body}`);
    }
    if (i > 0) times.push(last.ms); // warm
  }
  return { p50: p50(times), last };
}

const point = await sample(
  "point",
  `{:query [:find ?e . :where [?e :bench/email "user-${n - 1}@example.test"]]}`
);
gate("point p50", point.p50, gates.pointP50Ms);

const count = await sample(
  "count",
  `{:query [:find (count ?e) . :where [?e :bench/email _]]}`
);
if (count.last.body !== String(n)) {
  throw new Error(`count result ${count.last.body} != ${n}`);
}
gate("count p50", count.p50, gates.countP50Ms);

const join = await sample(
  "join",
  `{:query [:find (count ?e) . :where [?e :bench/role "admin"] [?e :bench/name ?name]]}`
);
gate("join p50", join.p50, gates.joinP50Ms);

console.log(JSON.stringify({
  ok: true,
  endpoint,
  entities: n,
  gates,
  measured: {
    transactMs: tx.ms,
    reindexMs: reindex.ms,
    pointP50Ms: point.p50,
    countP50Ms: count.p50,
    joinP50Ms: join.p50
  }
}, null, 2));
