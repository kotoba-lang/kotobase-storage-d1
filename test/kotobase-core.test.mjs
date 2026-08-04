import assert from "node:assert/strict";
import test from "node:test";
import {
  cacaoSiweMessage, graphCidFromName, looksLikeGraphCid
} from "../src/kotobase-core.mjs";

test("local graph CID helper remains byte-compatible with the gateway contract", async () => {
  const cid = await graphCidFromName("kotobase/db/did:key:z6Mk/test");
  assert.equal(cid, "bafyreihviw5fr3rzyeaox7h5laqjfy527xcriu4lbxpney3kt2sne5wm2m");
  assert.equal(looksLikeGraphCid(cid), true);
  assert.equal(looksLikeGraphCid("not-a-cid"), false);
});

test("local CACAO SIWE reconstruction preserves the signed line layout", () => {
  const message = cacaoSiweMessage({ p: {
    domain: "kotobase.net",
    iss: "did:key:zExample",
    aud: "https://kotobase.net",
    nonce: "nonce-1",
    iat: "2026-08-04T00:00:00Z",
    resources: ["kotoba://can/graph:query"]
  } });
  assert.equal(message, [
    "kotobase.net wants you to sign in with your Ethereum account:",
    "zExample", "", "URI: https://kotobase.net", "Version: 1",
    "Chain ID: 1", "Nonce: nonce-1", "Issued At: 2026-08-04T00:00:00Z",
    "Resources:", "- kotoba://can/graph:query"
  ].join("\n"));
});
