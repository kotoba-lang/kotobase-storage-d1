(ns kotobase.storage.d1-auth
  "Authentication, authorization, replay protection and audit for the D1
  Datomic Worker.

  This namespace owns no crypto and no wire format. CACAO envelope decode,
  base58btc did:key parsing, SIWE message reconstruction, Ed25519 signature
  verification and the temporal window all belong to `cacao.edge.verify`
  (kotoba-lang/org-chainagnostic-cacao), the single CACAO implementation for
  this fleet -- see ADR-2607268000. The allow/deny decision and its validity
  rules belong to `authorization.core`, which is deliberately not given a
  CACAO dependency; this file is the consumer that supplies the CACAO-shaped
  capability verifier it evaluates.

  What is genuinely local to D1 is the two pieces of *state* auth needs, and
  that is all this file should ever grow: the nonce table (replay) and the
  decision table (audit)."
  (:require [authorization.core :as authz]
            [authorization.model :as authz-model]
            [authorization.ports :as authz-ports]
            [cacao.edge.verify :as cacao]
            ))

(def tx-capability "kotoba://can/datom:transact")
(def read-capability "kotoba://can/graph:query")

(defn db-of
  "The D1 binding out of the Cloudflare `env` object.

  The `^js` hint is load-bearing, not decoration. `env` is an untyped host
  object, so under :advanced the property name is renamed unless :infer-externs
  can emit an extern for it, and the hint is what lets it. Measured on this
  file: plain `(.-DB env)` and `(aget env \"DB\")` and `(gobj/get env \"DB\")`
  ALL fold to the same property access and all compiled to `env.Eb`, so every
  query called `undefined.prepare` and every route returned 500.

  Two traps worth naming, since only the first is visible:
  `(.-DB env)` at least warns (\"Cannot infer target type\"); `aget` and
  `gobj/get` with a literal key silence that warning while keeping the bug, so
  the diagnostic disappears and the failure moves to runtime.

  (The `aget` calls elsewhere in this file are safe for a different reason:
  their keys name properties of objects built by compiled code, which the same
  compilation renames consistently on both sides. It is only the host-created
  `env` that needs this.)"
  [^js env]
  (.-DB env))

(defn- invoke [target method & args]
  (.apply (aget target method) target (to-array args)))

(defn- prepared [db sql params]
  (let [statement (invoke db "prepare" sql)]
    (.apply (aget statement "bind") statement (to-array params))))

(defn request-id
  "A fresh server-side id for one request's audit rows.

  Deliberately ignores any client-supplied `x-request-id`. When this was
  taken from that header, `auth_decisions.request_id` was a
  caller-chosen primary key written with INSERT OR REPLACE, so anyone could
  overwrite anyone else's decision rows by picking a colliding value -- the
  audit trail was editable by the parties it was recording. If request
  correlation with a caller's own id is wanted later, it belongs in a
  separate, non-key column."
  []
  (.randomUUID js/crypto))

(defn- record-decision! [db {:keys [request-id principal kind decision reason action resource]}]
  (-> (prepared
       db
       "INSERT INTO auth_decisions
        (request_id, principal, kind, decision, reason, action, resource, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
       [request-id (or principal nil) kind decision (or reason nil)
        (or action nil) (or resource nil) (.now js/Date)])
      (invoke "run")))

(defn authenticate
  "Verify the Authorization header. Resolves to
  {:ok? true :request-id .. :iss .. :payload .. :replay-until ..} or
  {:ok? false :status 401 :error ..}.

  Nothing is written to D1 on failure. The previous version recorded a
  `challenge` row before authentication succeeded, which let an
  unauthenticated caller drive one D1 write per request with a garbage
  header -- free write amplification against the audit table, and against
  the account's D1 budget. Rejections are observable in Worker logs, which
  is where unauthenticated traffic belongs; the audit table is for decisions
  about identified principals."
  [request env]
  (let [authorization (or (.get (.-headers request) "authorization") "")
        encoded (-> authorization (.replace #"(?i)^CACAO\s+" "") .trim)]
    (if (or (zero? (count encoded)) (> (count encoded) 16384))
      (js/Promise.resolve {:ok? false :status 401 :error "Unauthenticated"})
      (-> (cacao/verify encoded)
          (.then
           (fn [result]
             (if-not (true? (aget result "valid"))
               {:ok? false :status 401 :error "Unauthenticated"}
               (let [payload (aget result "payload")
                     id (request-id)]
                 (-> (record-decision!
                      (db-of env)
                      {:request-id id :principal (aget result "iss") :kind "authn"
                       :decision "authenticated" :reason "valid-cacao"})
                     (.then
                      (fn [_]
                        {:ok? true
                         :request-id id
                         :iss (aget result "iss")
                         :payload payload
                         :nonce (aget payload "nonce")
                         :replay-until (aget result "replayUntil")})))))))))))

(defn- resources-verifier
  "A capability verifier over the resources this CACAO actually carries.

  `authorization.adapters.cacao-policy/ICacaoCapabilityVerifier` is the shape
  authorization expects; this is its CACAO-envelope implementation. Resource
  strings are compared whole -- cacao.edge.verify has already rejected any
  containing a newline, which is what stops a single signed resource from
  being re-encoded as two (the SIWE reconstruction joins elements with
  \"\\n- \", so an embedded newline produces byte-identical signed text)."
  [payload]
  (let [resources (set (some-> (aget payload "resources") array-seq))]
    (fn [capability] (contains? resources capability))))

(defn- tenant-prefix [iss] (str "kotobase/db/" iss "/"))

(defn policy-port
  "The Worker's authorization policy as an IAuthorization port.

  Two independent conditions, both required: the CACAO carries the capability
  for this action, and the resource sits under the issuer's own tenant
  prefix. Authenticating says only that the caller holds a keypair -- it says
  nothing about what they may reach, which is why capability and tenant scope
  are evaluated separately here rather than inferred from a valid signature."
  [payload capability]
  (let [holds? (resources-verifier payload)]
    (reify authz-ports/IAuthorization
      (decide! [_ request]
        (let [principal (:authz.request/principal request)
              resource (:authz.request/resource request)
              has-capability? (holds? capability)
              in-tenant? (and (string? resource)
                              (.startsWith resource (tenant-prefix principal)))
              allowed? (and has-capability? in-tenant?)]
          (authz-model/decision
           request
           (if allowed? :allow :deny)
           {:by :d1-cacao-policy
            :reason (cond
                      allowed? nil
                      (not has-capability?) "missing-capability"
                      :else "tenant-scope-mismatch")}))))))

(defn authorize
  "Decide and audit. Resolves to {:ok? bool}."
  [authn env action resource capability]
  (let [request (authz-model/request (str (:request-id authn) ":authz")
                                    (:iss authn) action resource {})
        decision (authz/authorize (policy-port (:payload authn) capability) request)
        allowed? (= :allow (:authz.decision/decision decision))]
    (-> (record-decision!
         (db-of env)
         {:request-id (:authz.decision/request-id decision)
          :principal (:iss authn)
          :kind "authz"
          :decision (name (:authz.decision/decision decision))
          :reason (or (:authz.decision/reason decision) "policy-match")
          :action action
          :resource resource})
        (.then (fn [_] {:ok? allowed?})))))

(defn claim-nonce!
  "Claim this CACAO's nonce, or refuse it as a replay.

  Retention comes from `cacao.edge.verify/replay-until-sec` -- the last second
  the verifier will still accept this envelope -- not from a TTL chosen here.
  A store that forgets a nonce while its CACAO is still inside the verifier's
  window reopens replay for the remainder of that window: the same captured
  request verifies again and claims a fresh row. This Worker previously kept
  nonces for 600s while accepting no-exp CACAOs valid for the 7-day max-age
  fallback, so a single captured request was replayable roughly every 10
  minutes for a week.

  Sweeping only rows already past their own replay-until is therefore safe;
  it can never evict a nonce that is still defending a live CACAO."
  [authn env]
  (let [db (db-of env)
        now (cacao/now-sec)
        expires-at (:replay-until authn)]
    (-> (invoke (prepared db "DELETE FROM auth_nonces WHERE expires_at < ?" [now]) "run")
        (.then
         (fn [_]
           (invoke
            (prepared
             db
             "INSERT INTO auth_nonces(principal, nonce, expires_at)
              VALUES (?, ?, ?) ON CONFLICT(principal, nonce) DO NOTHING"
             [(:iss authn) (:nonce authn) expires-at])
            "run")))
        (.then (fn [result] (= 1 (aget (aget result "meta") "changes")))))))
