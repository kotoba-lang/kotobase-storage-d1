(ns kotobase.storage.d1-http
  "The Worker request boundary: routing, transport and nothing else.

  Replaces the previous `src/worker.mjs`, which was raw JavaScript holding a
  second hand-rolled CACAO verifier (repo rule: no new .mjs -- write nbb/cljs)
  and reaching across orgs by relative path
  (`../../../gftdcojp/net-kotobase/worker/js/kotobase-core.js`) for its SIWE
  reconstruction, an undeclared dependency that made this repo unbuildable
  from a clean clone and let another org's edit silently change which
  signatures this Worker accepts.

  Every handler is the same four steps in the same order, and none of them
  are implemented here: authenticate (cacao.edge.verify), authorize
  (authorization.core), claim the nonce (replay), then invoke the engine. The
  order matters -- authentication only establishes that the caller holds a
  keypair, which anyone can arrange, so it grants nothing on its own."
  (:require [cacao.edge.verify :as cacao]
            [kotobase.storage.d1-auth :as auth]
            [kotobase.storage.d1-worker :as worker]))

(defn- json-response [body status]
  (js/Response. (js/JSON.stringify (clj->js body))
                #js {:status status
                     :headers #js {"content-type" "application/json"
                                   "cache-control" "no-store"
                                   "x-content-type-options" "nosniff"}}))

(defn- edn-response [body]
  (js/Response. body
                #js {:status 200
                     :headers #js {"content-type" "application/edn; charset=utf-8"
                                   "cache-control" "no-store"
                                   "x-content-type-options" "nosniff"}}))

(defn- database-ref [request]
  (or (.get (.-headers request) "x-kotobase-ref") ""))

(defn- read-edn-body [request]
  (-> (.text request)
      (.then (fn [source]
               (when (or (zero? (count source)) (> (count source) (* 1024 1024)))
                 (throw (js/Error. "invalid EDN body length")))
               source))))

(defn- guarded
  "authorize -> claim nonce -> run. Shared by every authenticated route so no
  route can accidentally skip a step or reorder two of them."
  [authn env action resource capability run]
  (-> (auth/authorize authn env action resource capability)
      (.then
       (fn [{:keys [ok?]}]
         (if-not ok?
           (json-response {:ok false :error "Forbidden"} 403)
           (-> (auth/claim-nonce! authn env)
               (.then
                (fn [claimed?]
                  (if-not claimed?
                    (json-response {:ok false :error "Replay"} 401)
                    (run))))))))))

(defn- datomic-route [request env authn action capability invoke]
  (let [ref (database-ref request)]
    (guarded
     authn env action ref capability
     (fn []
       (-> (if (= "GET" (.-method request))
             (js/Promise.resolve nil)
             (read-edn-body request))
           (.then (fn [source] (invoke (auth/db-of env) (:iss authn) ref source)))
           (.then edn-response)
           (.catch (fn [error]
                     (js/console.error "Datomic request rejected" error)
                     (json-response {:ok false :error "InvalidDatomicRequest"} 400))))))))

(defn- commit-route [request env authn]
  (-> (.json request)
      (.then
       (fn [body]
         (let [ref (aget body "ref")
               cid (aget body "cid")
               encoded (aget body "bytes")
               expected (or (aget body "expected") nil)]
           (if-not (every? string? [ref cid encoded])
             (json-response {:ok false :error "InvalidCommit"} 400)
             (guarded
              authn env "kotobase/transact" ref auth/tx-capability
              (fn []
                ;; base64 is this endpoint's wire encoding, so it is decoded
                ;; here and the storage layer only ever sees bytes. Reuses
                ;; cacao's decoder, which is public for exactly this (rather
                ;; than growing yet another atob loop).
                (-> (worker/commit-block! (auth/db-of env) (:iss authn) ref expected cid
                                          (cacao/base64->bytes encoded))
                    (.then (fn [{:keys [status body]}]
                             (json-response body status))))))))))))

(defn- ref-route [request env authn]
  (let [name (or (.get (.-searchParams (js/URL. (.-url request))) "name") "")]
    (guarded
     authn env "kotobase/read" name auth/read-capability
     (fn []
       (-> (worker/read-ref (auth/db-of env) name)
           (.then (fn [ref]
                    (if ref
                      (json-response {:ok true :ref ref} 200)
                      (json-response {:ok false :error "NotFound"} 404)))))))))

(defn- health-route [env]
  (-> (worker/ping (auth/db-of env))
      (.then
       (fn [ok?]
         (json-response
          ;; Names the actual libraries in the request path. The previous
          ;; version reported "kotoba-lang/authentication:cacao" /
          ;; "kotoba-lang/authorization:deny-by-default" while importing
          ;; neither -- the first endpoint an auditor reaches was asserting an
          ;; integration that did not exist.
          {:ok ok?
           :backend "cloudflare-d1"
           :authn "kotoba-lang/org-chainagnostic-cacao:cacao.edge.verify"
           :authz "kotoba-lang/authorization:authorization.core"}
          200)))))

(defn- route [request env]
  (let [url (js/URL. (.-url request))
        path (.-pathname url)
        method (.-method request)
        get? (= "GET" method)
        post? (= "POST" method)]
    (if (and get? (= "/health" path))
      (health-route env)
      (-> (auth/authenticate request env)
          (.then
           (fn [authn]
             (if-not (:ok? authn)
               (json-response {:ok false :error (:error authn)} (:status authn))
               (cond
                 (and post? (= "/v1/commit" path)) (commit-route request env authn)
                 (and get? (= "/v1/ref" path)) (ref-route request env authn)
                 (and post? (= "/v1/transact" path))
                 (datomic-route request env authn "datomic/transact"
                                auth/tx-capability worker/transact-edn!)
                 (and post? (= "/v1/q" path))
                 (datomic-route request env authn "datomic/q"
                                auth/read-capability worker/q-edn!)
                 (and post? (= "/v1/pull" path))
                 (datomic-route request env authn "datomic/pull"
                                auth/read-capability worker/pull-edn!)
                 (and post? (= "/v1/datoms" path))
                 (datomic-route request env authn "datomic/datoms"
                                auth/read-capability worker/datoms-edn!)
                 (and get? (= "/v1/head" path))
                 (datomic-route request env authn "datomic/head"
                                auth/read-capability
                                (fn [db tenant ref _] (worker/head-edn! db tenant ref)))
                 :else (json-response {:ok false :error "NotFound"} 404)))))))))

(defn fetch-handler [request env]
  (-> (js/Promise.resolve nil)
      (.then (fn [_] (route request env)))
      (.catch (fn [error]
                (js/console.error "worker failure" error)
                (json-response {:ok false :error "InternalError"} 500)))))

(def ^:export handler #js {:fetch fetch-handler})
