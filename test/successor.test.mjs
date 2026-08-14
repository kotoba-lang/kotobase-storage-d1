import assert from "node:assert/strict";
import { createHandler, successorOrigin } from "../src/successor.mjs";

assert.equal(successorOrigin({}), "https://kotobase.net");
assert.throws(
  () => successorOrigin({ KOTOBASE_SUCCESSOR_URL: "https://example.com" }),
  /must be https:\/\/kotobase\.net/,
);

const calls = [];
const handler = createHandler(async (request) => {
  calls.push(request);
  return new Response('{"ok":true}', {
    status: 200,
    headers: { "content-type": "application/json" },
  });
});

const health = await handler.fetch(
  new Request("https://kotobase-storage-d1.aozora.app/health"),
  {},
);
assert.equal(health.status, 200);
assert.deepEqual(await health.json(), {
  ok: true,
  backend: "kotobase.net",
  storage: "r2",
  mode: "successor-proxy",
  d1: false,
});
assert.equal(calls.length, 0);

const retired = await handler.fetch(
  new Request("https://kotobase-storage-d1.aozora.app/v1/datoms", {
    method: "POST",
    body: "{:index :eavt}",
  }),
  {},
);
assert.equal(retired.status, 410);
assert.equal((await retired.json()).error, "D1Retired");
assert.equal(calls.length, 0);

const proxied = await handler.fetch(
  new Request(
    "https://kotobase-storage-d1.aozora.app/xrpc/ai.gftd.apps.kotobase.datomic.datoms?x=1",
    {
      method: "POST",
      headers: {
        authorization: "CACAO redacted",
        "content-type": "application/json",
        "cf-worker": "untrusted.example",
        "x-kotoba-did": "did:key:z6MkTest",
      },
      body: '{"graph":"bafy-test"}',
    },
  ),
  { KOTOBASE_SUCCESSOR_URL: "https://kotobase.net" },
);
assert.equal(proxied.status, 200);
assert.equal(proxied.headers.get("x-kotobase-storage-successor"), "r2");
assert.equal(calls.length, 1);
assert.equal(
  calls[0].url,
  "https://kotobase.net/xrpc/ai.gftd.apps.kotobase.datomic.datoms?x=1",
);
assert.equal(calls[0].headers.get("authorization"), "CACAO redacted");
assert.equal(calls[0].headers.get("x-kotoba-did"), "did:key:z6MkTest");
assert.equal(calls[0].headers.get("cf-worker"), null);
assert.equal(calls[0].headers.get("x-kotobase-storage-successor"), "r2");
assert.equal(await calls[0].text(), '{"graph":"bafy-test"}');

console.log("successor proxy: 3/3 pass");
