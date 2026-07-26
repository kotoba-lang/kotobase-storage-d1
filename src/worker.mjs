import * as dagCbor from "@ipld/dag-cbor";
import { ed25519 } from "@noble/curves/ed25519";
import { base58btc } from "multiformats/bases/base58";
import {
  cacaoSiweMessage, graphCidFromName, looksLikeGraphCid
} from "../../../gftdcojp/net-kotobase/worker/js/kotobase-core.js";
import {
  headD1, basisD1, txRangeD1, listenerD1, adminD1,
  transactD1, reindexD1, qD1, pullD1, datomsD1, foldD1, viewD1
} from "../dist/kotobase-engine.js";

const TX_CAPABILITY = "kotoba://can/datom:transact";
const READ_CAPABILITY = "kotoba://can/graph:query";
const textEncoder = new TextEncoder();

function json(body, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: {
      "content-type": "application/json",
      "cache-control": "no-store",
      "x-content-type-options": "nosniff"
    }
  });
}

function edn(body, status = 200) {
  return new Response(body, {
    status,
    headers: {
      "content-type": "application/edn; charset=utf-8",
      "cache-control": "no-store",
      "x-content-type-options": "nosniff"
    }
  });
}

function requestId(request) {
  return request.headers.get("x-request-id") || crypto.randomUUID();
}

function bytesFromBase64(value) {
  const raw = atob(value);
  const bytes = new Uint8Array(raw.length);
  for (let i = 0; i < raw.length; i += 1) bytes[i] = raw.charCodeAt(i);
  return bytes;
}

function sameBytes(left, right) {
  const a = new Uint8Array(left);
  const b = new Uint8Array(right);
  if (a.length !== b.length) return false;
  return a.every((value, index) => value === b[index]);
}

function strictInstant(value) {
  if (!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$/.test(value || "")) {
    return null;
  }
  const milliseconds = Date.parse(value);
  return Number.isFinite(milliseconds) ? Math.floor(milliseconds / 1000) : null;
}

function decodeBase64(value) {
  return bytesFromBase64(value);
}

function verifyAuthenticationCacao(authorization, now) {
  try {
    const encoded = authorization.replace(/^CACAO\s+/i, "").trim();
    if (!encoded || encoded.length > 16384) {
      return { error: "invalid CACAO length" };
    }
    const cacao = dagCbor.decode(decodeBase64(encoded));
    const payload = cacao.p;
    const signature = cacao.s;
    if (!payload?.iss || !payload?.nonce || !payload?.iat ||
        !Array.isArray(payload.resources)) {
      return { error: "invalid CACAO payload" };
    }
    if (signature?.t !== "EdDSA" || !signature.s) {
      return { error: "unsupported CACAO signature" };
    }
    const issuedAt = strictInstant(payload.iat);
    const expiresAt = payload.exp ? strictInstant(payload.exp) : null;
    if (issuedAt === null || issuedAt > now + 300) {
      return { error: "invalid CACAO issued-at" };
    }
    if ((payload.exp && expiresAt === null) ||
        (expiresAt !== null && now > expiresAt)) {
      return { error: "expired CACAO" };
    }
    if (!payload.iss.startsWith("did:key:")) {
      return { error: "unsupported CACAO issuer" };
    }
    const keyBytes = base58btc.decode(payload.iss.slice("did:key:".length));
    if (keyBytes.length !== 34 || keyBytes[0] !== 0xed || keyBytes[1] !== 0x01) {
      return { error: "invalid did:key" };
    }
    const signatureBytes = decodeBase64(signature.s);
    const message = cacaoSiweMessage({ p: payload });
    if (!ed25519.verify(
      signatureBytes, textEncoder.encode(message), keyBytes.slice(2)
    )) {
      return { error: "CACAO signature verification failed" };
    }
    return {
      iss: payload.iss,
      nonce: payload.nonce,
      iat: payload.iat,
      exp: payload.exp || null,
      resources: payload.resources
    };
  } catch (error) {
    return { error: error?.message || "CACAO verification failed" };
  }
}

async function recordDecision(env, decision) {
  await env.DB.prepare(
    `INSERT OR REPLACE INTO auth_decisions
     (request_id, principal, kind, decision, reason, action, resource, created_at)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
  ).bind(
    decision.requestId, decision.principal || null, decision.kind,
    decision.decision, decision.reason || null, decision.action || null,
    decision.resource || null, Date.now()
  ).run();
}

async function authenticate(request, env) {
  const id = requestId(request);
  const authorization = request.headers.get("authorization") || "";
  const now = Math.floor(Date.now() / 1000);
  const claims = verifyAuthenticationCacao(authorization, now);
  if (claims.error) {
    await recordDecision(env, {
      requestId: id, kind: "authn", decision: "challenge",
      reason: claims.error
    });
    return { error: json({ ok: false, error: "Unauthenticated" }, 401) };
  }
  await recordDecision(env, {
    requestId: id, principal: claims.iss, kind: "authn",
    decision: "authenticated", reason: "valid-cacao"
  });
  return { id, authorization, claims, now };
}

async function authorize(authn, env, action, resource, capability) {
  const prefix = `kotobase/db/${authn.claims.iss}/`;
  const hasCapability = authn.claims.resources.includes(capability);
  const allowed = hasCapability && resource.startsWith(prefix);
  await recordDecision(env, {
    requestId: `${authn.id}:authz`, principal: authn.claims.iss,
    kind: "authz", decision: allowed ? "allow" : "deny",
    reason: allowed ? "policy-match" :
      (!hasCapability ? "missing-capability" : "tenant-scope-mismatch"),
    action, resource
  });
  return allowed;
}

async function claimNonce(authn, env) {
  await env.DB.prepare("DELETE FROM auth_nonces WHERE expires_at < ?")
    .bind(authn.now).run();
  const result = await env.DB.prepare(
    `INSERT INTO auth_nonces(principal, nonce, expires_at)
     VALUES (?, ?, ?) ON CONFLICT(principal, nonce) DO NOTHING`
  ).bind(authn.claims.iss, authn.claims.nonce, authn.now + 600).run();
  return result.meta.changes === 1;
}

async function commit(request, env, authn) {
  const body = await request.json();
  const { ref, expected = null, cid, bytes: encoded } = body;
  if (![ref, cid, encoded].every((value) => typeof value === "string")) {
    return json({ ok: false, error: "InvalidCommit" }, 400);
  }
  if (!(await authorize(authn, env, "kotobase/transact", ref, TX_CAPABILITY))) {
    return json({ ok: false, error: "Forbidden" }, 403);
  }
  if (!(await claimNonce(authn, env))) {
    return json({ ok: false, error: "Replay" }, 401);
  }

  const bytes = bytesFromBase64(encoded);
  await env.DB.prepare(
    `INSERT INTO kotobase_blocks(cid, bytes, byte_length, created_at)
     VALUES (?, ?, ?, ?) ON CONFLICT(cid) DO NOTHING`
  ).bind(cid, bytes.buffer, bytes.length, Date.now()).run();
  const stored = await env.DB.prepare(
    "SELECT bytes FROM kotobase_blocks WHERE cid = ?"
  ).bind(cid).first();
  if (!stored || !sameBytes(stored.bytes, bytes)) {
    return json({ ok: false, error: "CidCollision" }, 409);
  }

  let result;
  if (expected === null) {
    result = await env.DB.prepare(
      `INSERT INTO kotobase_refs(name, cid, revision, updated_at)
       VALUES (?, ?, 1, ?) ON CONFLICT(name) DO NOTHING`
    ).bind(ref, cid, Date.now()).run();
  } else {
    result = await env.DB.prepare(
      `UPDATE kotobase_refs
       SET cid = ?, revision = revision + 1, updated_at = ?
       WHERE name = ? AND cid = ?`
    ).bind(cid, Date.now(), ref, expected).run();
  }
  const current = await env.DB.prepare(
    "SELECT cid, revision FROM kotobase_refs WHERE name = ?"
  ).bind(ref).first();
  if (result.meta.changes !== 1) {
    return json({ ok: false, error: "CasConflict", current }, 409);
  }
  return json({ ok: true, cid: current.cid, revision: current.revision });
}

async function readRef(request, env, authn) {
  const name = new URL(request.url).searchParams.get("name") || "";
  if (!(await authorize(authn, env, "kotobase/read", name, READ_CAPABILITY))) {
    return json({ ok: false, error: "Forbidden" }, 403);
  }
  if (!(await claimNonce(authn, env))) {
    return json({ ok: false, error: "Replay" }, 401);
  }
  const ref = await env.DB.prepare(
    "SELECT cid, revision FROM kotobase_refs WHERE name = ?"
  ).bind(name).first();
  return ref ? json({ ok: true, ref }) :
    json({ ok: false, error: "NotFound" }, 404);
}

function databaseRef(request) {
  return request.headers.get("x-kotobase-ref") || "";
}

function base64url(bytes) {
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

async function sessionDigest(token) {
  const digest = await crypto.subtle.digest("SHA-256", textEncoder.encode(token));
  return base64url(new Uint8Array(digest));
}

function ednString(value) {
  return JSON.stringify(value);
}

function readEdnScalar(value) {
  if (value === null || value === undefined || value === "nil") return null;
  if (value === "true") return true;
  if (value === "false") return false;
  if (/^-?\d+$/.test(value)) return Number(value);
  if (value.startsWith("\"")) return JSON.parse(value);
  if (value.startsWith(":")) return value.slice(1);
  return value;
}

async function entityFacts(db, ref, entityEdn) {
  const result = await db.prepare(
    `SELECT a_edn, v_edn
       FROM kotobase_datoms_current
      WHERE ref_name = ? AND e_edn = ?`
  ).bind(ref, entityEdn).all();
  const facts = {};
  for (const row of result.results || []) {
    const attribute = readEdnScalar(row.a_edn);
    const key = attribute.startsWith(":") ? attribute.slice(1) : attribute;
    const value = readEdnScalar(row.v_edn);
    if (Object.prototype.hasOwnProperty.call(facts, key)) {
      facts[key] = Array.isArray(facts[key]) ?
        [...facts[key], value] : [facts[key], value];
    } else {
      facts[key] = value;
    }
  }
  return facts;
}

async function resolveOpaqueSession(request, env) {
  const id = requestId(request);
  const ref = databaseRef(request);
  const authorization = request.headers.get("authorization") || "";
  const match = authorization.match(/^Bearer\s+(\S+)$/i);
  if (!ref.startsWith("kotobase/db/") || !match || match[1].length > 4096) {
    await recordDecision(env, {
      requestId: id, kind: "authn", decision: "challenge",
      reason: "missing-session"
    });
    return json({ ok: false, error: "Unauthenticated" }, 401);
  }

  const basis = await env.DB.prepare(
    `SELECT r.cid AS canonical_head, p.head_cid AS projection_head
       FROM kotobase_refs r
       LEFT JOIN kotobase_projection p ON p.ref_name = r.name
      WHERE r.name = ?`
  ).bind(ref).first();
  if (!basis) return json({ ok: false, error: "Unauthenticated" }, 401);
  if (!basis.projection_head || basis.projection_head !== basis.canonical_head) {
    return json({ ok: false, error: "AuthenticationProjectionUnavailable" }, 503);
  }

  const digest = await sessionDigest(match[1]);
  const sessionIdentity = await env.DB.prepare(
    `SELECT e_edn
       FROM kotobase_datoms_current
      WHERE ref_name = ? AND a_edn IN (?, ?) AND v_edn = ?
      LIMIT 1`
  ).bind(
    ref,
    ":identity.session/token-digest",
    ednString(":identity.session/token-digest"),
    ednString(digest)
  ).first();
  if (!sessionIdentity) {
    await recordDecision(env, {
      requestId: id, kind: "authn", decision: "challenge",
      reason: "unknown-session"
    });
    return json({ ok: false, error: "Unauthenticated" }, 401);
  }

  const session = await entityFacts(env.DB, ref, sessionIdentity.e_edn);
  const userEdn = ednString(session["identity.session/user"]);
  const tenantEdn = ednString(session["identity.session/tenant"]);
  const user = await entityFacts(env.DB, ref, userEdn);
  const tenant = await entityFacts(env.DB, ref, tenantEdn);
  const membershipEntity = await env.DB.prepare(
    `SELECT u.e_edn
       FROM kotobase_datoms_current u
       JOIN kotobase_datoms_current t
         ON t.ref_name = u.ref_name AND t.e_edn = u.e_edn
      WHERE u.ref_name = ?
        AND u.a_edn IN (?, ?) AND u.v_edn = ?
        AND t.a_edn IN (?, ?) AND t.v_edn = ?
      LIMIT 1`
  ).bind(
    ref,
    ":identity.membership/user",
    ednString(":identity.membership/user"),
    userEdn,
    ":identity.membership/tenant",
    ednString(":identity.membership/tenant"),
    tenantEdn
  ).first();
  const membership = membershipEntity ?
    await entityFacts(env.DB, ref, membershipEntity.e_edn) : null;
  const now = Date.now();
  const active = [false, "false"].includes(session["identity.session/revoked?"]) &&
    Number(session["identity.session/expires-at"]) > now &&
    ["active", ":active"].includes(user["identity.user/status"]) &&
    membership;
  const requestedTenant = request.headers.get("x-gftd-org-id");
  const requestedApplication = request.headers.get("x-kotobase-application");
  const tenantId = tenant["identity.tenant/id"];
  const sessionApplication = session["identity.session/application"];
  if (!active || (requestedTenant && requestedTenant !== tenantId)) {
    await recordDecision(env, {
      requestId: id, principal: user["identity.user/id"] || null,
      kind: requestedTenant && requestedTenant !== tenantId ? "authz" : "authn",
      decision: requestedTenant && requestedTenant !== tenantId ? "deny" : "challenge",
      reason: requestedTenant && requestedTenant !== tenantId ?
        "tenant-scope-mismatch" : "inactive-session"
    });
    return json({
      ok: false,
      error: requestedTenant && requestedTenant !== tenantId ?
        "Forbidden" : "Unauthenticated"
    }, requestedTenant && requestedTenant !== tenantId ? 403 : 401);
  }
  if (!requestedApplication || requestedApplication !== sessionApplication) {
    await recordDecision(env, {
      requestId: id, principal: user["identity.user/id"] || null,
      kind: "authz", decision: "deny", reason: "application-audience-mismatch",
      resource: requestedApplication || null
    });
    return json({ ok: false, error: "Forbidden" }, 403);
  }

  const permissionsValue = membership["identity.membership/permissions"];
  const permissions = permissionsValue === undefined ? [] :
    (Array.isArray(permissionsValue) ? permissionsValue : [permissionsValue]);
  const sessionId = session["identity.session/id"];
  const userId = user["identity.user/id"];
  await recordDecision(env, {
    requestId: id, principal: userId, kind: "authn",
    decision: "authenticated", reason: "active-datomic-session"
  });
  return json({
    ok: true,
    context: {
      claims: {
        userId,
        sessionId,
        orgId: tenantId,
        orgRole: String(membership["identity.membership/role"] || "")
          .replace(/^:/, ""),
        orgPermissions: permissions,
        issuedAtMs: Number(session["identity.session/created-at"]),
        expiresAtMs: Number(session["identity.session/expires-at"]),
        issuer: "kotobase-storage-d1",
        authorizedParties: [sessionApplication]
      },
      targetOrgId: tenantId,
      requestId: id
    }
  });
}

// net-kotobase's own kotobase-client sends q/pull/datoms bodies keyed by a
// pre-computed content-addressed `graph` CID -- it never sends the literal
// db_name a ref name is derived from (a CID is a one-way hash, so it cannot
// be reversed back into that name). `kotobase_graph_cid_index` closes that
// gap: every successful transact records (cid-of-ref-name -> ref-name), so
// a later request bearing only the CID resolves back to the literal ref a
// prior transact already established. Reuses net-kotobase's own
// graphCidFromName/looksLikeGraphCid (same kotobase-core.js import this
// file already pulls cacaoSiweMessage from) so the CID this Worker computes
// is byte-identical to the one net-kotobase's edge derives -- no new
// algorithm, no duplication, no drift risk between the two.
async function resolveRef(env, ref) {
  if (!looksLikeGraphCid(ref)) return ref;
  const row = await env.DB.prepare(
    "SELECT ref_name FROM kotobase_graph_cid_index WHERE cid = ?"
  ).bind(ref).first();
  return row ? row.ref_name : null;
}

async function recordCidAlias(env, refName) {
  const cid = await graphCidFromName(refName);
  await env.DB.prepare(
    `INSERT INTO kotobase_graph_cid_index(cid, ref_name, created_at)
     VALUES (?, ?, ?) ON CONFLICT(cid) DO NOTHING`
  ).bind(cid, refName, Date.now()).run();
}

async function readEdnBody(request) {
  const source = await request.text();
  if (!source || source.length > 1024 * 1024) {
    throw new Error("invalid EDN body length");
  }
  return source;
}

async function datomicRequest(
  request, env, authn, action, capability, invoke, { recordAlias = false } = {},
) {
  const rawRef = databaseRef(request);
  const ref = await resolveRef(env, rawRef);
  if (ref === null) {
    // A CID nothing has ever transacted into -- matches Datomic's own
    // "never-written ref reads as empty" shape (net-kotobase's client
    // treats a 404 here as an empty result, not an error).
    return json({ ok: false, error: "UnknownGraphCid" }, 404);
  }
  if (!(await authorize(authn, env, action, ref, capability))) {
    return json({ ok: false, error: "Forbidden" }, 403);
  }
  if (!(await claimNonce(authn, env))) {
    return json({ ok: false, error: "Replay" }, 401);
  }
  try {
    const source = request.method === "GET" ? null : await readEdnBody(request);
    const result = await invoke(env.DB, ref, source);
    if (recordAlias) await recordCidAlias(env, ref);
    return edn(result);
  } catch (error) {
    console.error("Datomic request rejected", error);
    return json({ ok: false, error: "InvalidDatomicRequest" }, 400);
  }
}

export default {
  async fetch(request, env) {
    try {
      const url = new URL(request.url);
      if (request.method === "GET" && url.pathname === "/health") {
        const row = await env.DB.prepare("SELECT 1 AS ok").first();
        return json({
          ok: row?.ok === 1,
          backend: "cloudflare-d1",
          authn: "kotoba-lang/authentication:cacao",
          authz: "kotoba-lang/authorization:deny-by-default"
        });
      }
      if (request.method === "GET" && url.pathname === "/v1/session") {
        return resolveOpaqueSession(request, env);
      }

      const authn = await authenticate(request, env);
      if (authn.error) return authn.error;
      if (request.method === "POST" && url.pathname === "/v1/commit") {
        return commit(request, env, authn);
      }
      if (request.method === "GET" && url.pathname === "/v1/ref") {
        return readRef(request, env, authn);
      }
      if (request.method === "POST" && url.pathname === "/v1/transact") {
        return datomicRequest(
          request, env, authn, "datomic/transact", TX_CAPABILITY,
          (db, ref, source) => transactD1(db, ref, source),
          { recordAlias: true }
        );
      }
      if (request.method === "POST" && url.pathname === "/v1/reindex") {
        return datomicRequest(
          request, env, authn, "datomic/reindex", TX_CAPABILITY,
          (db, ref, source) => reindexD1(db, ref, source)
        );
      }
      if (request.method === "POST" && url.pathname === "/v1/fold") {
        return datomicRequest(
          request, env, authn, "datomic/fold", TX_CAPABILITY,
          (db, ref, source) => foldD1(db, ref, source)
        );
      }
      if (request.method === "POST" && url.pathname === "/v1/q") {
        return datomicRequest(
          request, env, authn, "datomic/q", READ_CAPABILITY,
          (db, ref, source) => qD1(db, ref, source)
        );
      }
      if (request.method === "POST" && url.pathname === "/v1/pull") {
        return datomicRequest(
          request, env, authn, "datomic/pull", READ_CAPABILITY,
          (db, ref, source) => pullD1(db, ref, source)
        );
      }
      if (request.method === "POST" && url.pathname === "/v1/datoms") {
        return datomicRequest(
          request, env, authn, "datomic/datoms", READ_CAPABILITY,
          (db, ref, source) => datomsD1(db, ref, source)
        );
      }
      if (request.method === "POST" && url.pathname === "/v1/view") {
        return datomicRequest(
          request, env, authn, "datomic/view", READ_CAPABILITY,
          (db, ref, source) => viewD1(db, ref, source)
        );
      }
      if (request.method === "GET" && url.pathname === "/v1/head") {
        return datomicRequest(
          request, env, authn, "datomic/head", READ_CAPABILITY,
          (db, ref) => headD1(db, ref)
        );
      }
      if (request.method === "GET" && url.pathname === "/v1/basis") {
        return datomicRequest(
          request, env, authn, "datomic/basis", READ_CAPABILITY,
          (db, ref, source) => basisD1(db, ref, source)
        );
      }
      if (request.method === "POST" && url.pathname === "/v1/tx-range") {
        return datomicRequest(
          request, env, authn, "datomic/tx-range", READ_CAPABILITY,
          (db, ref, source) => txRangeD1(db, ref, source)
        );
      }
      if (request.method === "POST" &&
          url.pathname === "/v1/listeners/poll") {
        return datomicRequest(
          request, env, authn, "datomic/listener-poll", READ_CAPABILITY,
          (db, ref, source) => listenerD1(db, ref, source)
        );
      }
      if (request.method === "POST" &&
          (url.pathname === "/v1/listeners/register" ||
           url.pathname === "/v1/listeners/ack")) {
        return datomicRequest(
          request, env, authn, "datomic/listener-admin", TX_CAPABILITY,
          (db, ref, source) => listenerD1(db, ref, source)
        );
      }
      if (request.method === "GET" && url.pathname === "/v1/admin/status") {
        return datomicRequest(
          request, env, authn, "datomic/admin", TX_CAPABILITY,
          (db, ref, source) => adminD1(db, ref, source)
        );
      }
      return json({ ok: false, error: "NotFound" }, 404);
    } catch (error) {
      console.error("verification worker failure", error);
      return json({
        ok: false,
        error: "InternalError"
      }, 500);
    }
  }
};
