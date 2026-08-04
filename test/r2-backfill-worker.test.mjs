import assert from "node:assert/strict";
import test from "node:test";
import worker from "../src/r2-backfill-worker.mjs";

class ObjectBody {
  constructor(entry) {
    this.entry = entry;
    this.etag = entry.etag;
  }
  async text() { return new TextDecoder().decode(this.entry.bytes); }
  async arrayBuffer() { return this.entry.bytes.slice().buffer; }
}

class Bucket {
  constructor() { this.objects = new Map(); this.serial = 0; }
  async get(key) {
    const entry = this.objects.get(key);
    return entry ? new ObjectBody(entry) : null;
  }
  async put(key, body, options = {}) {
    const current = this.objects.get(key);
    const condition = options.onlyIf;
    if (condition?.etagDoesNotMatch === "*" && current) return null;
    if (condition?.etagMatches && current?.etag !== condition.etagMatches) return null;
    const bytes = body instanceof Uint8Array ? body.slice() : new Uint8Array(body);
    const entry = { bytes, etag: `etag-${++this.serial}` };
    this.objects.set(key, entry);
    return { etag: entry.etag };
  }
  async list({ prefix }) {
    return {
      objects: [...this.objects.entries()]
        .filter(([key]) => key.startsWith(prefix))
        .map(([key, value]) => ({ key, size: value.bytes.byteLength })),
      truncated: false
    };
  }
}

class Database {
  constructor() {
    this.blocks = [
      { cid: "cid-a", bytes: [1, 2], byte_length: 2, created_at: 0 },
      { cid: "cid-b", bytes: new Uint8Array([3]), byte_length: 1, created_at: 0 }
    ];
    this.refs = [{ name: "kotobase/db/did:key:z/ref", cid: "cid-b", revision: 2, updated_at: 1 }];
  }
  prepare(sql) {
    return { bind: (...args) => ({
      all: async () => {
        if (sql.includes("SELECT cid, byte_length FROM kotobase_blocks")) {
          const cursor = args.length === 3 ? args[2] : null;
          return { results: this.blocks.filter((row) => !cursor || row.cid > cursor)
            .map(({ cid, byte_length }) => ({ cid, byte_length })) };
        }
        if (sql.includes("SELECT cid, bytes FROM kotobase_blocks")) {
          return { results: this.blocks.filter((row) => args.includes(row.cid)) };
        }
        if (sql.includes("FROM kotobase_refs")) {
          const cursor = args.length === 2 ? args[1] : null;
          return { results: this.refs.filter((row) => !cursor || row.name > cursor) };
        }
        if (sql.includes("SELECT name, cid, revision FROM kotobase_refs")) {
          return { results: this.refs.map(({ name, cid, revision }) => ({ name, cid, revision })) };
        }
        throw new Error(`unexpected all SQL: ${sql}`);
      },
      first: async () => {
        if (sql.includes("COUNT(*) AS count")) {
          return {
            count: this.blocks.length,
            bytes: this.blocks.reduce((sum, row) => sum + row.byte_length, 0)
          };
        }
        throw new Error(`unexpected first SQL: ${sql}`);
      }
    }) };
  }
}

async function scheduled(workerEnv) {
  let pending;
  worker.scheduled({}, workerEnv, { waitUntil(value) { pending = value; } });
  await pending;
}

test("bounded cron copies immutable blocks then monotonic refs without changing authority", async () => {
  const env = {
    DB: new Database(),
    KOTOBASE_CANONICAL_R2: new Bucket(),
    KOTOBASE_R2_NAMESPACE: "test"
  };
  await scheduled(env);
  await scheduled(env);

  const keys = [...env.KOTOBASE_CANONICAL_R2.objects.keys()];
  assert(keys.some((key) => key.endsWith("/blocks/cid-a")));
  assert(keys.some((key) => key.endsWith("/blocks/cid-b")));
  const refKey = keys.find((key) => key.includes("/refs/"));
  const ref = JSON.parse(await (await env.KOTOBASE_CANONICAL_R2.get(refKey)).text());
  assert.equal(ref.revision, 2);

  const response = await worker.fetch(new Request("https://example.test/health"), env);
  const health = await response.json();
  assert.equal(health.authority, "d1");
  assert.equal(health.gc, false);
  assert.equal(health.phase, "complete");

  env.DB.refs[0] = { ...env.DB.refs[0], cid: "cid-c", revision: 3, updated_at: Date.now() };
  env.DB.blocks.push({ cid: "cid-c", bytes: new Uint8Array([4]), byte_length: 1, created_at: Date.now() });
  await scheduled(env);
  await scheduled(env);
  const advanced = JSON.parse(await (await env.KOTOBASE_CANONICAL_R2.get(refKey)).text());
  assert.equal(advanced.revision, 3);
  assert.equal(advanced.cid, "cid-c");
});

test("health fails closed without exposing migration state errors", async () => {
  const response = await worker.fetch(new Request("https://example.test/health"), {
    KOTOBASE_CANONICAL_R2: { get: async () => { throw new Error("outage"); } },
    KOTOBASE_R2_NAMESPACE: "test"
  });
  assert.equal(response.status, 503);
  assert.deepEqual(await response.json(), { ok: false, authority: "d1", degraded: true });
});

test("a parity-confirmed zero-byte migration is repaired and requalified", async () => {
  const bucket = new Bucket();
  const env = {
    DB: new Database(),
    KOTOBASE_CANONICAL_R2: bucket,
    KOTOBASE_R2_NAMESPACE: "repair",
    KOTOBASE_R2_REPAIR_ZERO_OBJECTS: "1"
  };
  const root = "kotobase/datomic/v2/repair/canonical";
  for (const row of env.DB.blocks) {
    await bucket.put(`${root}/blocks/${row.cid}`, new Uint8Array());
  }
  await bucket.put(`${root}/migration/backfill-state.json`, new TextEncoder().encode(JSON.stringify({
    version: 1,
    phase: "parity-failed",
    cycle: 1,
    lower_cutoff: -1,
    cutoff: Date.now(),
    block_cursor: null,
    ref_cursor: null,
    copied_blocks: 2,
    copied_refs: 1,
    parity: { r2_block_bytes: 0, pass: false }
  })));
  await scheduled(env);
  const repaired = new Uint8Array(
    await (await bucket.get(`${root}/blocks/cid-a`)).arrayBuffer()
  );
  assert.deepEqual([...repaired], [1, 2]);
  const health = await (await worker.fetch(new Request("https://example.test/health"), env)).json();
  assert.equal(health.phase, "complete");
  assert.equal(health.parity.pass, true);
});
