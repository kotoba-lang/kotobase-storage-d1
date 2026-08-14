const SUCCESSOR_ORIGIN = "https://kotobase.net";

function json(body, status = 200, extraHeaders = {}) {
  return new Response(JSON.stringify(body), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
      "cache-control": "no-store",
      "x-content-type-options": "nosniff",
      ...extraHeaders,
    },
  });
}

export function successorOrigin(env) {
  const configured = String(env?.KOTOBASE_SUCCESSOR_URL || SUCCESSOR_ORIGIN)
    .replace(/\/+$/, "");
  if (configured !== SUCCESSOR_ORIGIN) {
    throw new Error("KOTOBASE_SUCCESSOR_URL must be https://kotobase.net");
  }
  return configured;
}

function successorHeaders(request) {
  const headers = new Headers(request.headers);
  for (const name of [
    "host",
    "cf-connecting-ip",
    "cf-ipcountry",
    "cf-ray",
    "cf-visitor",
    "cf-worker",
    "x-forwarded-for",
    "x-forwarded-host",
    "x-forwarded-proto",
    "x-real-ip",
  ]) {
    headers.delete(name);
  }
  headers.set("x-kotobase-storage-successor", "r2");
  return headers;
}

export function createHandler(fetchImpl = fetch) {
  return {
    async fetch(request, env) {
      const incoming = new URL(request.url);
      if (request.method === "GET" && incoming.pathname === "/health") {
        return json({
          ok: true,
          backend: "kotobase.net",
          storage: "r2",
          mode: "successor-proxy",
          d1: false,
        });
      }

      if (!incoming.pathname.startsWith("/xrpc/")) {
        return json(
          {
            ok: false,
            error: "D1Retired",
            successor: SUCCESSOR_ORIGIN,
            message: "Use kotobase.net XRPC; the D1 Client API is retired.",
          },
          410,
          { link: `<${SUCCESSOR_ORIGIN}>; rel="successor-version"` },
        );
      }

      const target = new URL(incoming.pathname + incoming.search, successorOrigin(env));
      if (target.host === incoming.host) {
        return json({ ok: false, error: "SuccessorLoop" }, 500);
      }

      const init = {
        method: request.method,
        headers: successorHeaders(request),
        redirect: "manual",
      };
      if (request.method !== "GET" && request.method !== "HEAD") {
        init.body = request.body;
        // Required by Node's standards-compatible Request implementation and
        // ignored by Workers; the body remains streamed and is never buffered.
        init.duplex = "half";
      }

      const upstream = await fetchImpl(new Request(target, init));
      const headers = new Headers(upstream.headers);
      headers.set("cache-control", "no-store");
      headers.set("x-kotobase-storage-successor", "r2");
      return new Response(upstream.body, {
        status: upstream.status,
        statusText: upstream.statusText,
        headers,
      });
    },
  };
}

export default createHandler();
