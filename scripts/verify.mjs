import { ed25519 } from "@noble/curves/ed25519";
import * as dagCbor from "@ipld/dag-cbor";
import { base58btc } from "multiformats/bases/base58";
import {
  cacaoSiweMessage
} from "../../../gftdcojp/net-kotobase/worker/js/kotobase-core.js";

const endpoint = process.env.KOTOBASE_D1_URL ||
  "https://kotobase-storage-d1.aozora.app";
const encoder = new TextEncoder();
const secret = ed25519.utils.randomSecretKey();
const pub = ed25519.getPublicKey(secret);
const prefixed = new Uint8Array(34);
prefixed.set([0xed, 0x01]);
prefixed.set(pub, 2);
const did = `did:key:${base58btc.encode(prefixed)}`;
const ref = `kotobase/db/${did}/verification`;

function base64(bytes) {
  return Buffer.from(bytes).toString("base64");
}

function instant(offsetSeconds = 0) {
  const value = new Date(Date.now() + offsetSeconds * 1000);
  value.setMilliseconds(0);
  return value.toISOString().replace(".000Z", "Z");
}

function cacao(capability, nonce = crypto.randomUUID()) {
  const payload = {
    iss: did,
    domain: new URL(endpoint).host,
    aud: endpoint,
    version: "1",
    nonce,
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

async function call(path, { method = "GET", auth, body, ref } = {}) {
  const headers = { "x-request-id": crypto.randomUUID() };
  if (auth) headers.authorization = auth;
  if (ref) headers["x-kotobase-ref"] = ref;
  if (body !== undefined) {
    headers["content-type"] =
      typeof body === "string" ? "application/edn" : "application/json";
  }
  const response = await fetch(`${endpoint}${path}`, {
    method,
    headers,
    body: body === undefined ? undefined :
      (typeof body === "string" ? body : JSON.stringify(body))
  });
  let value;
  const contentType = response.headers.get("content-type") || "";
  try {
    value = contentType.includes("application/edn") ?
      await response.text() : await response.json();
  } catch {
    value = null;
  }
  return { status: response.status, body: value };
}

function check(label, actual, expected, context) {
  if (actual !== expected) {
    throw new Error(
      `${label}: expected ${expected}, got ${actual} ${JSON.stringify(context)}`
    );
  }
  console.log(`ok - ${label} (${actual})`);
}

const tx = "kotoba://can/datom:transact";
const read = "kotoba://can/graph:query";
const bytesA = base64(encoder.encode("immutable-block-a"));
const bytesB = base64(encoder.encode("immutable-block-b"));

let result = await call("/health");
check("D1 health", result.status, 200);
check("health backend", result.body.backend, "cloudflare-d1");

result = await call("/v1/commit", {
  method: "POST",
  body: { ref, cid: "cid-a", bytes: bytesA }
});
check("missing authentication denied", result.status, 401);

result = await call("/v1/commit", {
  method: "POST", auth: cacao(read),
  body: { ref, cid: "cid-a", bytes: bytesA }
});
check("wrong capability denied by authz", result.status, 403);

result = await call("/v1/commit", {
  method: "POST", auth: cacao(tx),
  body: { ref: "kotobase/db/did:key:attacker/verification",
          cid: "cid-a", bytes: bytesA }
});
check("cross-tenant ref denied", result.status, 403);

const replayAuth = cacao(tx);
result = await call("/v1/commit", {
  method: "POST", auth: replayAuth,
  body: { ref, cid: "cid-a", bytes: bytesA }
});
check("authenticated genesis commit", result.status, 200, result.body);
check("genesis revision", result.body.revision, 1);

result = await call("/v1/commit", {
  method: "POST", auth: replayAuth,
  body: { ref, expected: "cid-a", cid: "cid-b", bytes: bytesB }
});
check("CACAO nonce replay denied", result.status, 401);

result = await call("/v1/commit", {
  method: "POST", auth: cacao(tx),
  body: { ref, expected: "stale", cid: "cid-b", bytes: bytesB }
});
check("stale CAS denied", result.status, 409);

result = await call("/v1/commit", {
  method: "POST", auth: cacao(tx),
  body: { ref, expected: "cid-a", cid: "cid-b", bytes: bytesB }
});
check("current CAS commit", result.status, 200);
check("updated revision", result.body.revision, 2);

result = await call(`/v1/ref?name=${encodeURIComponent(ref)}`, {
  auth: cacao(read)
});
check("authorized ref read", result.status, 200);
check("read current CID", result.body.ref.cid, "cid-b");

result = await call("/v1/commit", {
  method: "POST", auth: cacao(tx),
  body: { ref, expected: "cid-b", cid: "cid-b", bytes: bytesA }
});
check("CID collision denied", result.status, 409);

const datomicRef =
  `kotobase/db/${did}/datomic-${crypto.randomUUID()}`;

result = await call("/v1/q", {
  method: "POST", ref: datomicRef,
  body: `{:query [:find ?e :where [?e :person/name _]]}`
});
check("Datomic q requires authentication", result.status, 401);

result = await call("/v1/q", {
  method: "POST", auth: cacao(tx), ref: datomicRef,
  body: `{:query [:find ?e :where [?e :person/name _]]}`
});
check("Datomic q requires read capability", result.status, 403);

result = await call("/v1/q", {
  method: "POST", auth: cacao(read),
  ref: "kotobase/db/did:key:attacker/datomic",
  body: `{:query [:find ?e :where [?e :person/name _]]}`
});
check("Datomic q enforces tenant scope", result.status, 403);

result = await call("/v1/transact", {
  method: "POST", auth: cacao(tx), ref: datomicRef,
  body: `{:tx-data
          [{:db/id "alice"
            :person/name "Alice"
            :person/role "admin"}
           {:db/id "bob"
            :person/name "Bob"}]}`
});
check("Datomic transact", result.status, 200, result.body);
if (!result.body.includes(":db-after")) {
  throw new Error(`Datomic tx-report missing :db-after: ${result.body}`);
}

result = await call("/v1/q", {
  method: "POST", auth: cacao(read), ref: datomicRef,
  body: `{:query
          [:find ?e ?name
           :where
           [?e :person/role "admin"]
           [?e :person/name ?name]]}`
});
check("Datomic relation q", result.status, 200, result.body);
check("Datomic relation result", result.body, `#{["alice" "Alice"]}`);

result = await call("/v1/q", {
  method: "POST", auth: cacao(read), ref: datomicRef,
  body: `{:query
          [:find ?e
           :in $ ?role
           :where [?e :person/role ?role]]
          :args ["admin"]}`
});
check("Datomic :in q", result.status, 200, result.body);
check("Datomic :in result", result.body, `#{["alice"]}`);

result = await call("/v1/q", {
  method: "POST", auth: cacao(read), ref: datomicRef,
  body: `{:query
          [:find ?name .
           :where ["alice" :person/name ?name]]}`
});
check("Datomic scalar q", result.status, 200, result.body);
check("Datomic scalar result", result.body, `"Alice"`);

result = await call("/v1/pull", {
  method: "POST", auth: cacao(read), ref: datomicRef,
  body: `{:selector [:person/name :person/role] :eid "alice"}`
});
check("Datomic pull", result.status, 200, result.body);
if (!(result.body.includes(`":person/name"`) &&
      result.body.includes(`"Alice"`))) {
  throw new Error(`Datomic pull result mismatch: ${result.body}`);
}

result = await call("/v1/datoms", {
  method: "POST", auth: cacao(read), ref: datomicRef,
  body: `{:index :eavt :components ["alice"]}`
});
check("Datomic datoms", result.status, 200, result.body);
if (!(result.body.includes(`:e "alice"`) &&
      result.body.includes(`:a ":person/name"`))) {
  throw new Error(`Datomic datoms result mismatch: ${result.body}`);
}

result = await call("/v1/datoms", {
  method: "POST", auth: cacao(read), ref: datomicRef,
  body: `{:index :avet :components [:person/name "Alice"]}`
});
check("Datomic AVET datoms", result.status, 200, result.body);
if (!result.body.includes(`:e "alice"`)) {
  throw new Error(`Datomic AVET result mismatch: ${result.body}`);
}

result = await call("/v1/q", {
  method: "POST", auth: cacao(read), ref: datomicRef,
  body: `{:query
          [:find (count ?e) .
           :where [?e :person/name _]]}`
});
check("Datomic aggregate q", result.status, 200, result.body);
check("Datomic aggregate result", result.body, `2`);

result = await call("/v1/transact", {
  method: "POST", auth: cacao(tx), ref: datomicRef,
  body: `{:tx-data [[:db/retract "bob" :person/name "Bob"]]}`
});
check("Datomic retract transaction", result.status, 200, result.body);

result = await call("/v1/q", {
  method: "POST", auth: cacao(read), ref: datomicRef,
  body: `{:query
          [:find [?name ...]
           :where [_ :person/name ?name]]}`
});
check("Datomic collection q", result.status, 200, result.body);
check("Datomic retract is visible", result.body, `["Alice"]`);

result = await call("/v1/head", {
  auth: cacao(read), ref: datomicRef
});
check("Datomic head", result.status, 200, result.body);
if (!result.body.startsWith(`"b`)) {
  throw new Error(`Datomic head is not a CID: ${result.body}`);
}

console.log(JSON.stringify({
  ok: true,
  endpoint,
  principal: did,
  ref,
  datomicRef
}, null, 2));

function percentile(values, fraction) {
  const sorted = [...values].sort((left, right) => left - right);
  return sorted[Math.floor((sorted.length - 1) * fraction)];
}

async function timedCall(path, options) {
  const started = performance.now();
  const response = await call(path, options);
  return {
    ...response,
    elapsedMs: performance.now() - started
  };
}

if (process.env.KOTOBASE_BENCH === "1") {
  const sizes = (process.env.KOTOBASE_BENCH_SIZES || "100,1000,5000")
    .split(",")
    .map(Number)
    .filter((value) => Number.isInteger(value) && value > 0);
  const benchmark = [];

  for (const entityCount of sizes) {
    const benchmarkRef =
      `kotobase/db/${did}/benchmark-${entityCount}-${crypto.randomUUID()}`;
    const entities = Array.from(
      { length: entityCount },
      (_, index) =>
        `{:db/id "user-${index}" ` +
        `:bench/email "user-${index}@example.test" ` +
        `:bench/name "User ${index}" ` +
        `:bench/role "${index % 10 === 0 ? "admin" : "member"}"}`
    );
    const txBody = `{:tx-data [${entities.join(" ")}]}`;
    const transaction = await timedCall("/v1/transact", {
      method: "POST",
      auth: cacao(tx),
      ref: benchmarkRef,
      body: txBody
    });
    check(
      `benchmark transact ${entityCount}`,
      transaction.status,
      200,
      transaction.body
    );

    const probes = {
      point:
        `{:query [:find ?e . :where ` +
        `[?e :bench/email "user-${entityCount - 1}@example.test"]]}`,
      count:
        `{:query [:find (count ?e) . ` +
        `:where [?e :bench/email _]]}`,
      join:
        `{:query [:find (count ?e) . :where ` +
        `[?e :bench/role "admin"] [?e :bench/name ?name]]}`
    };
    const measurements = {};

    for (const [name, query] of Object.entries(probes)) {
      const samples = [];
      for (let iteration = 0; iteration < 6; iteration += 1) {
        const measured = await timedCall("/v1/q", {
          method: "POST",
          auth: cacao(read),
          ref: benchmarkRef,
          body: query
        });
        check(
          `benchmark ${name} ${entityCount}`,
          measured.status,
          200,
          measured.body
        );
        const expected =
          name === "point" ? `"user-${entityCount - 1}"` :
          name === "join" ? String(Math.ceil(entityCount / 10)) :
          String(entityCount);
        check(
          `benchmark ${name} result ${entityCount}`,
          measured.body,
          expected,
          measured.body
        );
        if (iteration > 0) samples.push(measured.elapsedMs);
      }
      measurements[name] = {
        p50Ms: percentile(samples, 0.5),
        minMs: Math.min(...samples),
        maxMs: Math.max(...samples)
      };
    }

    benchmark.push({
      entityCount,
      datomCount: entityCount * 3,
      txRequestBytes: Buffer.byteLength(txBody),
      transactionMs: transaction.elapsedMs,
      measurements
    });
  }

  console.log(JSON.stringify({
    benchmark: "d1-datomic-projection-v1",
    endpoint,
    samplesPerQuery: 5,
    results: benchmark
  }, null, 2));
}
