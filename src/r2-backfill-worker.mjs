const encoder = new TextEncoder();

function root(env) {
  const namespace = String(env.KOTOBASE_R2_NAMESPACE || "production").trim();
  if (!/^[A-Za-z0-9._-]{1,80}$/.test(namespace)) {
    throw new Error("invalid KOTOBASE_R2_NAMESPACE");
  }
  return `kotobase/datomic/v2/${namespace}/canonical`;
}

function blockKey(env, cid) {
  return `${root(env)}/blocks/${cid}`;
}

function refKey(env, ref) {
  return `${root(env)}/refs/${encodeURIComponent(ref)}`;
}

function stateKey(env) {
  return `${root(env)}/migration/backfill-state.json`;
}

async function getState(env) {
  const object = await env.KOTOBASE_CANONICAL_R2.get(stateKey(env));
  if (!object) {
    return {
      etag: null,
      value: {
        version: 1,
        phase: "blocks",
        cycle: 1,
        lower_cutoff: -1,
        cutoff: Date.now(),
        block_cursor: null,
        ref_cursor: null,
        copied_blocks: 0,
        copied_refs: 0,
        completed_at: null
      }
    };
  }
  return { etag: object.etag, value: JSON.parse(await object.text()) };
}

async function putState(env, previousEtag, value) {
  const body = encoder.encode(JSON.stringify({ ...value, updated_at: Date.now() }));
  const result = await env.KOTOBASE_CANONICAL_R2.put(stateKey(env), body, {
    onlyIf: previousEtag
      ? { etagMatches: previousEtag }
      : { etagDoesNotMatch: "*" }
  });
  return result ? { etag: result.etag, value: JSON.parse(new TextDecoder().decode(body)) }
    : getState(env);
}

function sameBytes(left, right) {
  return left.byteLength === right.byteLength &&
    left.every((value, index) => value === right[index]);
}

function blobBytes(value) {
  if (value instanceof ArrayBuffer) return new Uint8Array(value);
  if (ArrayBuffer.isView(value)) {
    return new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
  }
  if (Array.isArray(value)) return Uint8Array.from(value);
  throw new Error("unsupported D1 BLOB representation");
}

async function putImmutable(env, cid, bytes, repairZeroObjects = false) {
  const key = blockKey(env, cid);
  if (repairZeroObjects) {
    await env.KOTOBASE_CANONICAL_R2.put(key, bytes);
    return;
  }
  const created = await env.KOTOBASE_CANONICAL_R2.put(key, bytes, {
    onlyIf: { etagDoesNotMatch: "*" }
  });
  if (created) return;
  const existing = await env.KOTOBASE_CANONICAL_R2.get(key);
  if (!existing) throw new Error("R2 immutable CID collision");
  const stored = new Uint8Array(await existing.arrayBuffer());
  if (stored.byteLength === 0 && bytes.byteLength > 0 &&
      String(env.KOTOBASE_R2_REPAIR_ZERO_OBJECTS || "0") === "1") {
    await env.KOTOBASE_CANONICAL_R2.put(key, bytes);
    const repaired = await env.KOTOBASE_CANONICAL_R2.get(key);
    if (repaired && sameBytes(new Uint8Array(await repaired.arrayBuffer()), bytes)) return;
  }
  if (!sameBytes(stored, bytes)) {
    throw new Error("R2 immutable CID collision");
  }
}

async function copyBlockPage(env, stateRecord) {
  const { value: state } = stateRecord;
  const cursorClause = state.block_cursor ? "AND cid > ?" : "";
  const sql = `SELECT cid, byte_length FROM kotobase_blocks
    WHERE created_at >= ? AND created_at <= ? ${cursorClause}
    ORDER BY cid LIMIT 64`;
  const args = state.block_cursor
    ? [state.lower_cutoff, state.cutoff, state.block_cursor]
    : [state.lower_cutoff, state.cutoff];
  const candidates = (await env.DB.prepare(sql).bind(...args).all()).results || [];
  let totalBytes = 0;
  const selected = [];
  for (const row of candidates) {
    if (selected.length > 0 && totalBytes + row.byte_length > 4194304) break;
    selected.push(row);
    totalBytes += row.byte_length;
  }
  const rows = selected.length === 0 ? [] : (await env.DB.prepare(
    `SELECT cid, bytes FROM kotobase_blocks
      WHERE cid IN (${selected.map(() => "?").join(",")}) ORDER BY cid`
  ).bind(...selected.map((row) => row.cid)).all()).results || [];
  await Promise.all(rows.map((row) => {
    return putImmutable(env, row.cid, blobBytes(row.bytes),
      state.repair === "zero-length-d1-blob-conversion");
  }));
  const next = rows.length === 0
    ? { ...state, phase: "refs", block_cursor: null, ref_cursor: null }
    : {
        ...state,
        block_cursor: rows.at(-1).cid,
        copied_blocks: state.copied_blocks + rows.length
      };
  return putState(env, stateRecord.etag, next);
}

async function publishRef(env, source) {
  const key = refKey(env, source.name);
  const body = encoder.encode(JSON.stringify({
    version: 2,
    ref: source.name,
    cid: source.cid,
    revision: source.revision,
    source_updated_at: source.updated_at,
    mirrored_at: Date.now()
  }));
  for (let attempt = 0; attempt < 8; attempt += 1) {
    const currentObject = await env.KOTOBASE_CANONICAL_R2.get(key);
    if (!currentObject) {
      if (await env.KOTOBASE_CANONICAL_R2.put(key, body, {
        onlyIf: { etagDoesNotMatch: "*" }
      })) return;
      continue;
    }
    const current = JSON.parse(await currentObject.text());
    if (Number(current.revision) >= Number(source.revision)) return;
    if (await env.KOTOBASE_CANONICAL_R2.put(key, body, {
      onlyIf: { etagMatches: currentObject.etag }
    })) return;
  }
  throw new Error("R2 ref CAS retry budget exhausted");
}

async function copyRefPage(env, stateRecord) {
  const { value: state } = stateRecord;
  const sql = state.ref_cursor
    ? `SELECT name, cid, revision, updated_at FROM kotobase_refs
        WHERE updated_at <= ? AND name > ? ORDER BY name LIMIT 64`
    : `SELECT name, cid, revision, updated_at FROM kotobase_refs
        WHERE updated_at <= ? ORDER BY name LIMIT 64`;
  const args = state.ref_cursor ? [state.cutoff, state.ref_cursor] : [state.cutoff];
  const result = await env.DB.prepare(sql).bind(...args).all();
  const rows = result.results || [];
  for (const row of rows) await publishRef(env, row);
  const next = rows.length === 0
    ? { ...state, phase: "parity", ref_cursor: null }
    : {
        ...state,
        ref_cursor: rows.at(-1).name,
        copied_refs: state.copied_refs + rows.length
      };
  return putState(env, stateRecord.etag, next);
}

async function r2BlockInventory(env) {
  let cursor;
  let count = 0;
  let bytes = 0;
  do {
    const page = await env.KOTOBASE_CANONICAL_R2.list({
      prefix: `${root(env)}/blocks/`,
      cursor
    });
    for (const object of page.objects || []) {
      count += 1;
      bytes += object.size;
    }
    cursor = page.truncated ? page.cursor : undefined;
  } while (cursor);
  return { count, bytes };
}

async function verifyParity(env, stateRecord) {
  const { value: state } = stateRecord;
  const sourceBlocks = await env.DB.prepare(
    `SELECT COUNT(*) AS count, COALESCE(SUM(byte_length), 0) AS bytes
       FROM kotobase_blocks WHERE created_at <= ?`
  ).bind(state.cutoff).first();
  const sourceRefsResult = await env.DB.prepare(
    `SELECT name, cid, revision FROM kotobase_refs
      WHERE updated_at <= ? ORDER BY name`
  ).bind(state.cutoff).all();
  const sourceRefs = sourceRefsResult.results || [];
  const r2Blocks = await r2BlockInventory(env);
  let matchingRefs = 0;
  for (const source of sourceRefs) {
    const object = await env.KOTOBASE_CANONICAL_R2.get(refKey(env, source.name));
    if (!object) continue;
    const target = JSON.parse(await object.text());
    if (target.cid === source.cid && Number(target.revision) === Number(source.revision)) {
      matchingRefs += 1;
    }
  }
  const parity = {
    checked_at: Date.now(),
    source_blocks: Number(sourceBlocks.count),
    source_block_bytes: Number(sourceBlocks.bytes),
    r2_blocks: r2Blocks.count,
    r2_block_bytes: r2Blocks.bytes,
    source_refs: sourceRefs.length,
    matching_refs: matchingRefs
  };
  parity.pass = parity.source_blocks === parity.r2_blocks &&
    parity.source_block_bytes === parity.r2_block_bytes &&
    parity.source_refs === parity.matching_refs;
  return putState(env, stateRecord.etag, {
    ...state,
    phase: parity.pass ? "complete" : "parity-failed",
    parity,
    completed_at: parity.pass ? Date.now() : null
  });
}

async function advance(env) {
  let stateRecord = await getState(env);
  if (stateRecord.value.phase === "parity-failed" &&
      stateRecord.value.parity?.r2_block_bytes === 0 &&
      String(env.KOTOBASE_R2_REPAIR_ZERO_OBJECTS || "0") === "1") {
    stateRecord = await putState(env, stateRecord.etag, {
      ...stateRecord.value,
      phase: "blocks",
      cycle: stateRecord.value.cycle + 1,
      lower_cutoff: -1,
      cutoff: Date.now(),
      block_cursor: null,
      ref_cursor: null,
      copied_blocks: 0,
      copied_refs: 0,
      parity: null,
      completed_at: null,
      repair: "zero-length-d1-blob-conversion"
    });
  }
  if (stateRecord.value.phase === "complete") {
    stateRecord = await putState(env, stateRecord.etag, {
      ...stateRecord.value,
      phase: "blocks",
      cycle: stateRecord.value.cycle + 1,
      lower_cutoff: Math.max(-1, stateRecord.value.cutoff - 1000),
      cutoff: Date.now(),
      block_cursor: null,
      ref_cursor: null,
      completed_at: null
    });
  }
  const maxPages = stateRecord.value.repair === "zero-length-d1-blob-conversion"
    ? 12 : 8;
  for (let page = 0; page < maxPages && stateRecord.value.phase !== "complete"; page += 1) {
    if (stateRecord.value.phase === "blocks") {
      stateRecord = await copyBlockPage(env, stateRecord);
    } else if (stateRecord.value.phase === "refs") {
      stateRecord = await copyRefPage(env, stateRecord);
    } else if (stateRecord.value.phase === "parity") {
      stateRecord = await verifyParity(env, stateRecord);
    } else {
      break;
    }
  }
  return stateRecord.value;
}

function publicStatus(state) {
  return {
    ok: true,
    authority: "d1",
    target: "r2-canonical-shadow",
    gc: false,
    phase: state.phase,
    cycle: state.cycle,
    cutoff: state.cutoff,
    copied_blocks: state.copied_blocks,
    copied_refs: state.copied_refs,
    completed_at: state.completed_at,
    parity: state.parity || null
  };
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (request.method !== "GET" || url.pathname !== "/health") {
      return new Response("Not Found", { status: 404 });
    }
    try {
      return Response.json(publicStatus((await getState(env)).value), {
        headers: { "cache-control": "no-store" }
      });
    } catch (_error) {
      return Response.json({ ok: false, authority: "d1", degraded: true }, {
        status: 503,
        headers: { "cache-control": "no-store" }
      });
    }
  },

  async scheduled(_event, env, ctx) {
    ctx.waitUntil(advance(env).then((state) => {
      console.log("R2 canonical backfill", JSON.stringify(publicStatus(state)));
    }));
  }
};
