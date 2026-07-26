;; End-to-end verification for the D1 Datomic Worker.
;;
;;   npx wrangler dev                        # local D1 + workerd
;;   KOTOBASE_D1_URL=http://127.0.0.1:8787 nbb scripts/verify.cljs
;;   KOTOBASE_BENCH=1 nbb scripts/verify.cljs
;;
;; nbb rather than the previous scripts/verify.mjs (repo rule: no new raw JS;
;; the .mjs also imported `@noble/curves` and reached across orgs by relative
;; path into gftdcojp/net-kotobase for its SIWE reconstruction).
;;
;; This harness mints with cacao.edge.mint and the Worker verifies with
;; cacao.edge.verify -- the same library, from the same pin. That is the point:
;; when mint and verify were separate hand-rolled implementations, a
;; divergence in SIWE line order or CBOR key order could only be discovered by
;; a client failing in production. Now a divergence cannot exist.
(require '[cacao.edge.mint :as mint]
         '[clojure.string :as str]
         '[promesa.core :as p])

(def endpoint
  (or (aget (.-env js/process) "KOTOBASE_D1_URL") "http://127.0.0.1:8787"))

(def tx-capability "kotoba://can/datom:transact")
(def read-capability "kotoba://can/graph:query")

(defn- fail! [message]
  (throw (js/Error. message)))

(defn check! [label actual expected & [context]]
  (if (= actual expected)
    (js/console.log (str "ok - " label " (" actual ")"))
    (fail! (str label ": expected " expected ", got " actual
                (when context (str " " (js/JSON.stringify (clj->js context))))))))

(defn contains-all! [label text fragments]
  (doseq [fragment fragments]
    (when-not (str/includes? text fragment)
      (fail! (str label ": missing " (pr-str fragment) " in " text))))
  (js/console.log (str "ok - " label)))

;; ---------------------------------------------------------------- principals

(defn- signer [private-key]
  (fn [msg-bytes] (.sign js/crypto.subtle "Ed25519" private-key msg-bytes)))

(defn principal
  "A fresh Ed25519 did:key holder. Anyone can make one of these, which is
  exactly why authentication alone authorizes nothing."
  []
  (p/let [kp (.generateKey js/crypto.subtle #js {:name "Ed25519"} true
                           #js ["sign" "verify"])
          raw (.exportKey js/crypto.subtle "raw" (.-publicKey kp))]
    {:did (mint/did-key-from-raw-ed25519-pub (js/Uint8Array. raw))
     :sign (signer (.-privateKey kp))}))

(defn instant [offset-sec]
  (let [d (js/Date. (+ (js/Date.now) (* 1000 offset-sec)))]
    (.setMilliseconds d 0)
    (str/replace (.toISOString d) ".000Z" "Z")))

(defn cacao
  "A signed CACAO header value. `opts` exists so the negative cases can mint
  envelopes that are correctly signed but temporally or structurally invalid --
  the interesting failures are the ones where the signature is genuine."
  ([who capability] (cacao who capability {}))
  ([{:keys [did sign]} capability
    {:keys [exp iat resources] :or {exp (instant 300) iat (instant 0)}}]
   (p/let [{:keys [cacao-b64]}
           (mint/mint did sign
                      {:domain (.-host (js/URL. endpoint))
                       :aud endpoint
                       :nonce (.randomUUID js/crypto)
                       :iat iat
                       :exp exp
                       :resources (or resources [capability])})]
     (str "CACAO " cacao-b64))))

;; ------------------------------------------------------------------ transport

(defn call
  [path {:keys [method auth body ref request-id]}]
  (p/let [headers (cond-> #js {}
                    auth (doto (aset "authorization" auth))
                    ref (doto (aset "x-kotobase-ref" ref))
                    ;; Only sent where a test deliberately probes it. The
                    ;; Worker ignores this header; it used to be the audit
                    ;; table's primary key.
                    request-id (doto (aset "x-request-id" request-id))
                    (some? body) (doto (aset "content-type"
                                             (if (string? body)
                                               "application/edn"
                                               "application/json"))))
          response (js/fetch (str endpoint path)
                             #js {:method (or method "GET")
                                  :headers headers
                                  :body (cond
                                          (nil? body) js/undefined
                                          (string? body) body
                                          :else (js/JSON.stringify (clj->js body)))})
          content-type (or (.get (.-headers response) "content-type") "")
          value (if (str/includes? content-type "application/edn")
                  (.text response)
                  (-> (.json response) (.catch (fn [_] nil))))]
    {:status (.-status response) :body value}))

(defn- body-get [body k]
  (when (and body (not (string? body))) (aget body k)))

;; ---------------------------------------------------------------------- suite

(defn run-functional [alice]
  (let [ref (str "kotobase/db/" (:did alice) "/verification")
        bytes-a (mint/bytes->base64 (.encode (js/TextEncoder.) "immutable-block-a"))
        bytes-b (mint/bytes->base64 (.encode (js/TextEncoder.) "immutable-block-b"))]
    (p/let [health (call "/health" {})
            _ (check! "D1 health" (:status health) 200)
            _ (check! "health backend" (body-get (:body health) "backend") "cloudflare-d1")
            ;; The health endpoint must name the libraries actually in the
            ;; request path. It used to claim an authentication/authorization
            ;; integration that did not exist.
            _ (contains-all! "health names its real authn library"
                             (body-get (:body health) "authn")
                             ["org-chainagnostic-cacao"])

            anon (call "/v1/commit" {:method "POST"
                                     :body {:ref ref :cid "cid-a" :bytes bytes-a}})
            _ (check! "missing authentication denied" (:status anon) 401)

            wrong-cap-auth (cacao alice read-capability)
            wrong-cap (call "/v1/commit" {:method "POST" :auth wrong-cap-auth
                                          :body {:ref ref :cid "cid-a" :bytes bytes-a}})
            _ (check! "wrong capability denied by authz" (:status wrong-cap) 403)

            cross-auth (cacao alice tx-capability)
            cross (call "/v1/commit"
                        {:method "POST" :auth cross-auth
                         :body {:ref "kotobase/db/did:key:attacker/verification"
                                :cid "cid-a" :bytes bytes-a}})
            _ (check! "cross-tenant ref denied" (:status cross) 403)

            replay-auth (cacao alice tx-capability)
            genesis (call "/v1/commit" {:method "POST" :auth replay-auth
                                        :body {:ref ref :cid "cid-a" :bytes bytes-a}})
            _ (check! "authenticated genesis commit" (:status genesis) 200 (:body genesis))
            _ (check! "genesis revision" (body-get (:body genesis) "revision") 1)

            replayed (call "/v1/commit" {:method "POST" :auth replay-auth
                                         :body {:ref ref :expected "cid-a"
                                                :cid "cid-b" :bytes bytes-b}})
            _ (check! "CACAO nonce replay denied" (:status replayed) 401)

            stale-auth (cacao alice tx-capability)
            stale (call "/v1/commit" {:method "POST" :auth stale-auth
                                      :body {:ref ref :expected "stale"
                                             :cid "cid-b" :bytes bytes-b}})
            _ (check! "stale CAS denied" (:status stale) 409)

            cas-auth (cacao alice tx-capability)
            cas (call "/v1/commit" {:method "POST" :auth cas-auth
                                    :body {:ref ref :expected "cid-a"
                                           :cid "cid-b" :bytes bytes-b}})
            _ (check! "current CAS commit" (:status cas) 200)
            _ (check! "updated revision" (body-get (:body cas) "revision") 2)

            read-auth (cacao alice read-capability)
            ref-read (call (str "/v1/ref?name=" (js/encodeURIComponent ref))
                           {:auth read-auth})
            _ (check! "authorized ref read" (:status ref-read) 200)
            _ (check! "read current CID"
                      (aget (body-get (:body ref-read) "ref") "cid") "cid-b")

            collide-auth (cacao alice tx-capability)
            collide (call "/v1/commit" {:method "POST" :auth collide-auth
                                        :body {:ref ref :expected "cid-b"
                                               :cid "cid-b" :bytes bytes-a}})
            _ (check! "same key with different bytes denied" (:status collide) 409)]
      {:ref ref})))

(defn run-hardening
  "The five gaps that existed while this Worker had its own hand-rolled CACAO
  verifier, and that cacao.edge.verify closes. Each mints a GENUINELY SIGNED
  envelope -- none of these are forgeries."
  [alice bob]
  (p/let [;; 1. No-exp CACAO. Accepted (inside the 7-day max-age fallback), but
          ;;    its nonce must be retained for that whole window. The old
          ;;    Worker swept nonces after 600s while accepting these, so one
          ;;    captured request replayed every ~10 minutes for a week.
          no-exp-ref (str "kotobase/db/" (:did alice) "/no-exp")
          no-exp-auth (cacao alice tx-capability {:exp nil})
          bytes (mint/bytes->base64 (.encode (js/TextEncoder.) "no-exp-block"))
          no-exp (call "/v1/commit" {:method "POST" :auth no-exp-auth
                                     :body {:ref no-exp-ref :cid "cid-n" :bytes bytes}})
          _ (check! "no-exp CACAO is accepted inside max-age" (:status no-exp) 200
                    (:body no-exp))
          no-exp-replay (call "/v1/commit"
                              {:method "POST" :auth no-exp-auth
                               :body {:ref no-exp-ref :expected "cid-n"
                                      :cid "cid-n2" :bytes bytes}})
          _ (check! "no-exp CACAO replay denied" (:status no-exp-replay) 401)

          ;; 2. Calendar-invalid iat. Passes a regex on shape and Date.parse
          ;;    rolls it over to Mar 2; cacao's toISOString round-trip rejects
          ;;    it. The old Worker only did the regex + Date.parse.
          bad-iat-auth (cacao alice tx-capability {:iat "2026-02-30T00:00:00Z"})
          bad-iat (call "/v1/commit" {:method "POST" :auth bad-iat-auth
                                      :body {:ref no-exp-ref :cid "cid-x" :bytes bytes}})
          _ (check! "calendar-invalid iat denied" (:status bad-iat) 401)

          ;; 3. Resource carrying an embedded "\n- ". siwe-message renders each
          ;;    resource as its own "- <r>" line, so one element containing that
          ;;    sequence reconstructs to byte-identical signed text as two
          ;;    separate elements -- a real signature over a resources shape
          ;;    that was never uniquely signed, re-encodable into two
          ;;    capabilities. The old Worker had no such check.
          smuggled-auth (cacao alice tx-capability
                               {:resources [(str read-capability "\n- " tx-capability)]})
          smuggled (call "/v1/commit" {:method "POST" :auth smuggled-auth
                                       :body {:ref no-exp-ref :cid "cid-y" :bytes bytes}})
          _ (check! "resource with embedded newline denied" (:status smuggled) 401)

          ;; 4. Client-chosen x-request-id no longer keys the audit table, so a
          ;;    caller cannot overwrite another principal's decision rows by
          ;;    colliding on it. Observable here only as "the header is
          ;;    ignored"; the row-level property is the schema's (server UUID
          ;;    primary key, plain INSERT).
          collide-id (.randomUUID js/crypto)
          id-ref (str "kotobase/db/" (:did alice) "/request-id")
          first-auth (cacao alice tx-capability)
          first-req (call "/v1/commit" {:method "POST" :auth first-auth
                                        :request-id collide-id
                                        :body {:ref id-ref :cid "cid-r" :bytes bytes}})
          _ (check! "client x-request-id is ignored (1st)" (:status first-req) 200
                    (:body first-req))
          second-auth (cacao alice tx-capability)
          second-req (call "/v1/commit" {:method "POST" :auth second-auth
                                         :request-id collide-id
                                         :body {:ref id-ref :expected "cid-r"
                                                :cid "cid-r2" :bytes bytes}})
          _ (check! "client x-request-id is ignored (2nd, same id)"
                    (:status second-req) 200 (:body second-req))

          ;; 5. Cross-tenant block key. Bob commits the SAME opaque block key
          ;;    as Alice, with different bytes, under his own ref. While
          ;;    kotobase_blocks.cid was a global primary key this returned
          ;;    CidCollision 409 -- one tenant could permanently deny a key to
          ;;    every other tenant, and (reads not re-deriving the CID) serve
          ;;    them its own bytes instead.
          bob-ref (str "kotobase/db/" (:did bob) "/verification")
          bob-bytes (mint/bytes->base64 (.encode (js/TextEncoder.) "bobs-different-bytes"))
          bob-auth (cacao bob tx-capability)
          bob-commit (call "/v1/commit" {:method "POST" :auth bob-auth
                                         :body {:ref bob-ref :cid "cid-a"
                                                :bytes bob-bytes}})
          _ (check! "another tenant may reuse a block key" (:status bob-commit) 200
                    (:body bob-commit))]
    :ok))

(defn run-datomic [alice]
  (let [ref (str "kotobase/db/" (:did alice) "/datomic-" (.randomUUID js/crypto))]
    (p/let [anon (call "/v1/q" {:method "POST" :ref ref
                                :body "{:query [:find ?e :where [?e :person/name _]]}"})
            _ (check! "Datomic q requires authentication" (:status anon) 401)

            wrong-auth (cacao alice tx-capability)
            wrong (call "/v1/q" {:method "POST" :auth wrong-auth :ref ref
                                 :body "{:query [:find ?e :where [?e :person/name _]]}"})
            _ (check! "Datomic q requires read capability" (:status wrong) 403)

            cross-auth (cacao alice read-capability)
            cross (call "/v1/q" {:method "POST" :auth cross-auth
                                 :ref "kotobase/db/did:key:attacker/datomic"
                                 :body "{:query [:find ?e :where [?e :person/name _]]}"})
            _ (check! "Datomic q enforces tenant scope" (:status cross) 403)

            tx-auth (cacao alice tx-capability)
            tx (call "/v1/transact"
                     {:method "POST" :auth tx-auth :ref ref
                      :body (str "{:tx-data [{:db/id \"alice\" :person/name \"Alice\""
                                 " :person/role \"admin\"}"
                                 " {:db/id \"bob\" :person/name \"Bob\"}]}")})
            _ (check! "Datomic transact" (:status tx) 200 (:body tx))
            _ (contains-all! "tx-report has :db-after" (:body tx) [":db-after"])

            rel-auth (cacao alice read-capability)
            rel (call "/v1/q"
                      {:method "POST" :auth rel-auth :ref ref
                       :body (str "{:query [:find ?e ?name :where"
                                  " [?e :person/role \"admin\"] [?e :person/name ?name]]}")})
            _ (check! "Datomic relation q" (:status rel) 200 (:body rel))
            _ (check! "Datomic relation result" (:body rel) "#{[\"alice\" \"Alice\"]}")

            in-auth (cacao alice read-capability)
            in-q (call "/v1/q"
                       {:method "POST" :auth in-auth :ref ref
                        :body (str "{:query [:find ?e :in $ ?role :where"
                                   " [?e :person/role ?role]] :args [\"admin\"]}")})
            _ (check! "Datomic :in q" (:status in-q) 200 (:body in-q))
            _ (check! "Datomic :in result" (:body in-q) "#{[\"alice\"]}")

            scalar-auth (cacao alice read-capability)
            scalar (call "/v1/q"
                         {:method "POST" :auth scalar-auth :ref ref
                          :body (str "{:query [:find ?name . :where"
                                     " [\"alice\" :person/name ?name]]}")})
            _ (check! "Datomic scalar q" (:status scalar) 200 (:body scalar))
            _ (check! "Datomic scalar result" (:body scalar) "\"Alice\"")

            pull-auth (cacao alice read-capability)
            pull (call "/v1/pull"
                       {:method "POST" :auth pull-auth :ref ref
                        :body "{:selector [:person/name :person/role] :eid \"alice\"}"})
            _ (check! "Datomic pull" (:status pull) 200 (:body pull))
            _ (contains-all! "Datomic pull result" (:body pull)
                             [":person/name" "Alice"])

            eavt-auth (cacao alice read-capability)
            eavt (call "/v1/datoms"
                       {:method "POST" :auth eavt-auth :ref ref
                        :body "{:index :eavt :components [\"alice\"]}"})
            _ (check! "Datomic datoms" (:status eavt) 200 (:body eavt))
            _ (contains-all! "Datomic EAVT result" (:body eavt) ["alice" ":person/name"])

            avet-auth (cacao alice read-capability)
            avet (call "/v1/datoms"
                       {:method "POST" :auth avet-auth :ref ref
                        :body "{:index :avet :components [:person/name \"Alice\"]}"})
            _ (check! "Datomic AVET datoms" (:status avet) 200 (:body avet))
            _ (contains-all! "Datomic AVET result" (:body avet) ["alice"])

            agg-auth (cacao alice read-capability)
            agg (call "/v1/q"
                      {:method "POST" :auth agg-auth :ref ref
                       :body "{:query [:find (count ?e) . :where [?e :person/name _]]}"})
            _ (check! "Datomic aggregate q" (:status agg) 200 (:body agg))
            _ (check! "Datomic aggregate result" (:body agg) "2")

            retract-auth (cacao alice tx-capability)
            retract (call "/v1/transact"
                          {:method "POST" :auth retract-auth :ref ref
                           :body "{:tx-data [[:db/retract \"bob\" :person/name \"Bob\"]]}"})
            _ (check! "Datomic retract transaction" (:status retract) 200 (:body retract))

            coll-auth (cacao alice read-capability)
            coll (call "/v1/q"
                       {:method "POST" :auth coll-auth :ref ref
                        :body "{:query [:find [?name ...] :where [_ :person/name ?name]]}"})
            _ (check! "Datomic collection q" (:status coll) 200 (:body coll))
            _ (check! "Datomic retract is visible" (:body coll) "[\"Alice\"]")

            head-auth (cacao alice read-capability)
            head (call "/v1/head" {:auth head-auth :ref ref})
            _ (check! "Datomic head" (:status head) 200 (:body head))
            _ (when-not (str/starts-with? (:body head) "\"b")
                (fail! (str "Datomic head is not a CID: " (:body head))))]
      {:datomic-ref ref})))

;; ------------------------------------------------------------------ benchmark

(defn- percentile [values fraction]
  (let [sorted (vec (sort values))]
    (nth sorted (js/Math.floor (* (dec (count sorted)) fraction)))))

(defn- timed-call [path options]
  (let [started (js/performance.now)]
    (p/let [response (call path options)]
      (assoc response :elapsed-ms (- (js/performance.now) started)))))

(defn- bench-probe [alice ref label query expected entity-count]
  (p/loop [iteration 0 samples []]
    (if (>= iteration 6)
      {:p50-ms (percentile samples 0.5)
       :min-ms (apply min samples)
       :max-ms (apply max samples)}
      (p/let [auth (cacao alice read-capability)
              measured (timed-call "/v1/q" {:method "POST" :auth auth :ref ref
                                            :body query})
              _ (check! (str "benchmark " label " " entity-count)
                        (:status measured) 200 (:body measured))
              _ (check! (str "benchmark " label " result " entity-count)
                        (:body measured) expected)]
        (p/recur (inc iteration)
                 (if (pos? iteration) (conj samples (:elapsed-ms measured)) samples))))))

(defn run-benchmark [alice]
  (let [sizes (->> (str/split (or (aget (.-env js/process) "KOTOBASE_BENCH_SIZES")
                                  "100,1000,5000")
                              #",")
                   (map #(js/parseInt % 10))
                   (filter #(and (js/Number.isInteger %) (pos? %))))]
    (p/loop [remaining sizes results []]
      (if (empty? remaining)
        (do (js/console.log
             (js/JSON.stringify
              (clj->js {:benchmark "d1-datomic-projection-v1"
                        :endpoint endpoint
                        :samplesPerQuery 5
                        :results results})
              nil 2))
            results)
        (let [entity-count (first remaining)
              ref (str "kotobase/db/" (:did alice)
                       "/benchmark-" entity-count "-" (.randomUUID js/crypto))
              tx-body (str "{:tx-data ["
                           (str/join
                            " "
                            (for [index (range entity-count)]
                              (str "{:db/id \"user-" index "\""
                                   " :bench/email \"user-" index "@example.test\""
                                   " :bench/name \"User " index "\""
                                   " :bench/role \""
                                   (if (zero? (mod index 10)) "admin" "member")
                                   "\"}")))
                           "]}")]
          (p/let [auth (cacao alice tx-capability)
                  transaction (timed-call "/v1/transact"
                                          {:method "POST" :auth auth :ref ref
                                           :body tx-body})
                  _ (check! (str "benchmark transact " entity-count)
                            (:status transaction) 200 (:body transaction))
                  point (bench-probe alice ref "point"
                                     (str "{:query [:find ?e . :where [?e :bench/email "
                                          "\"user-" (dec entity-count) "@example.test\"]]}")
                                     (str "\"user-" (dec entity-count) "\"")
                                     entity-count)
                  cnt (bench-probe alice ref "count"
                                   "{:query [:find (count ?e) . :where [?e :bench/email _]]}"
                                   (str entity-count) entity-count)
                  joined (bench-probe alice ref "join"
                                      (str "{:query [:find (count ?e) . :where"
                                           " [?e :bench/role \"admin\"] [?e :bench/name ?name]]}")
                                      (str (js/Math.ceil (/ entity-count 10)))
                                      entity-count)]
            (p/recur (rest remaining)
                     (conj results
                           {:entityCount entity-count
                            :datomCount (* entity-count 3)
                            :txRequestBytes (count tx-body)
                            :transactionMs (:elapsed-ms transaction)
                            :measurements {:point point :count cnt :join joined}}))))))))

;; ----------------------------------------------------------------------- main

(-> (p/let [alice (principal)
            bob (principal)
            functional (run-functional alice)
            _ (run-hardening alice bob)
            datomic (run-datomic alice)
            _ (js/console.log
               (js/JSON.stringify
                (clj->js {:ok true
                          :endpoint endpoint
                          :principal (:did alice)
                          :ref (:ref functional)
                          :datomicRef (:datomic-ref datomic)})
                nil 2))]
      (when (= "1" (aget (.-env js/process) "KOTOBASE_BENCH"))
        (run-benchmark alice)))
    (.catch (fn [error]
              (js/console.error (or (.-message error) error))
              (set! (.-exitCode js/process) 1))))
