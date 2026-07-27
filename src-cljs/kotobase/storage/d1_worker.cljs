(ns kotobase.storage.d1-worker
  "Direct Cloudflare D1 binding for the Promise-based Kotobase engine.

  The exported boundary accepts and returns EDN so Datomic keywords, symbols,
  sets, pull selectors, and query vectors survive the JavaScript transport."
  (:require [clojure.string :as string]
            [cljs.reader :as reader]
            [kotobase.datomic :as d]
            [kotobase.engine :as engine]
            [kotobase.storage.d1-projection :as projection]
            [kotobase.storage.core :as storage]))

(defn- invoke [target method & args]
  (.apply (aget target method) target (to-array args)))

(defn- prepared [db sql params]
  (let [statement (invoke db "prepare" sql)]
    (.apply (aget statement "bind") statement (to-array params))))

(defn- bytes-buffer [bytes]
  (let [buffer (.-buffer bytes)
        offset (.-byteOffset bytes)
        length (.-byteLength bytes)]
    (if (and (zero? offset) (= length (.-byteLength buffer)))
      buffer
      (.slice buffer offset (+ offset length)))))

(defn- same-bytes? [left right]
  (let [left (js/Uint8Array. left)
        right (js/Uint8Array. right)]
    (and (= (.-length left) (.-length right))
         (loop [index 0]
           (cond
             (= index (.-length left)) true
             (= (aget left index) (aget right index)) (recur (inc index))
             :else false)))))

;; `tenant` is the authenticated did:key the blocks belong to. `kotobase_blocks`
;; was keyed on `cid` alone while refs were per-DID, so the block store was the
;; one shared surface under an otherwise isolated tenant model: whoever wrote a
;; CID first owned it globally. The read-back below turns that into an error
;; rather than served attacker bytes, but the victim still gets a permanent
;; CidCollision on a block they legitimately own. Migration 0007 scopes the key
;; to (principal, cid); this threads the principal into every block statement.
;;
;; It is derived from ref-name rather than passed in, because authorization
;; already requires the resource to sit under `kotobase/db/<iss>/` -- deriving
;; it from the same string the authz check ran against means the two cannot
;; drift apart.
(defrecord D1Storage [db projection-plan tenant]
  storage/IBlockStore
  (-put-blocks! [_ blocks]
    (-> (js/Promise.all
         (to-array
          (map
           (fn [{:keys [cid bytes]}]
             (invoke
              (prepared
               db
               "INSERT INTO kotobase_blocks
                (principal, cid, bytes, byte_length, created_at)
                VALUES (?, ?, ?, ?, ?) ON CONFLICT(principal, cid) DO NOTHING"
               [tenant cid (bytes-buffer bytes) (.-byteLength bytes) (.now js/Date)])
              "run"))
           blocks)))
        (.then
         (fn [_]
           (js/Promise.all
            (to-array
             (map
              (fn [{:keys [cid]}]
                (invoke
                 (prepared db
                           "SELECT bytes FROM kotobase_blocks
                            WHERE principal = ? AND cid = ?"
                           [tenant cid])
                 "first"))
              blocks)))))
        (.then
         (fn [rows]
           (doseq [[{:keys [cid bytes]} row] (map vector blocks rows)]
             (when-not (and row (same-bytes? (aget row "bytes") bytes))
               (throw
                (ex-info "CID already has different bytes"
                         {:type :kotobase.storage/cid-collision
                          :cid cid}))))
           (mapv :cid blocks)))))

  (-get-blocks [_ cids]
    (-> (js/Promise.all
         (to-array
          (map
           (fn [cid]
             (-> (invoke
                  (prepared db
                            "SELECT bytes FROM kotobase_blocks
                             WHERE principal = ? AND cid = ?"
                            [tenant cid])
                  "first")
                 (.then
                  (fn [row]
                    (when row [cid (js/Uint8Array. (aget row "bytes"))])))))
           cids)))
        (.then
         (fn [entries]
           (into {} (remove nil?) (array-seq entries))))))

  storage/IRefStore
  (-read-ref [_ name]
    (-> (invoke
         (prepared db
                   "SELECT cid, revision FROM kotobase_refs WHERE name = ?"
                   [name])
         "first")
        (.then
         (fn [row]
           (when row
             {:cid (aget row "cid")
              :version (aget row "revision")})))))

  (-compare-and-set-ref! [this name expected next]
    (let [publish
          (if projection-plan
            (projection/projected-cas!
             db name expected next projection-plan)
            (let [statement
                  (if (nil? expected)
                    (prepared
                     db
                     "INSERT INTO kotobase_refs(name, cid, revision, updated_at)
                      VALUES (?, ?, 1, ?) ON CONFLICT(name) DO NOTHING"
                     [name next (.now js/Date)])
                    (prepared
                     db
                     "UPDATE kotobase_refs
                      SET cid = ?, revision = revision + 1, updated_at = ?
                      WHERE name = ? AND cid = ?"
                     [next (.now js/Date) name expected]))]
              (-> (invoke statement "run")
                  (.then
                   (fn [result]
                     {:published?
                      (= 1 (aget (aget result "meta") "changes"))
                      :current next})))))]
      (-> publish
          (.then
           (fn [{:keys [published?]}]
             (if published?
               {:published? true :current next :version nil}
               (-> (storage/-read-ref this name)
                   (.then
                    (fn [current]
                      {:published? false
                       :current (:cid current)
                       :version (:version current)})))))))))

  storage/IBackendCapabilities
  (-capabilities [_]
    #{:immutable-blocks :cid-addressed-read :conditional-ref
      :linearizable-ref :batch-get :batch-put :cloudflare-d1}))

(defn- ref-principal
  "The did:key a ref belongs to. Ref names are `kotobase/db/<iss>/<name>`, and
  authorization rejects anything outside that prefix, so this is exactly the
  principal the request was authorized as. Returns \"\" for a malformed name,
  which no authenticated principal can ever equal, so such a request reads and
  writes nothing rather than falling back to the shared surface."
  [ref-name]
  (let [parts (string/split (str ref-name) #"/")]
    (if (and (= "kotobase" (nth parts 0 nil))
             (= "db" (nth parts 1 nil))
             (seq (nth parts 2 nil)))
      (nth parts 2)
      "")))

(defn- database
  ([db ref-name] (database db ref-name nil))
  ([db ref-name projection-plan]
  (engine/open
   {:storage (->D1Storage db projection-plan (ref-principal ref-name))
    :ref-name ref-name
    :encrypt-fn #(js/Promise.resolve %)
    :decrypt-fn #(js/Promise.resolve %)
    :blind-fn #(js/Promise.resolve (pr-str %))
    :visible? (constantly true)})))

(defn- read-edn [source]
  (reader/read-string source))

(defn- edn-promise [value]
  (-> (js/Promise.resolve value)
      (.then pr-str)))

(defn ^:export head-edn! [db ref-name]
  (edn-promise (engine/head (database db ref-name))))

(defn ^:export transact-edn! [db ref-name source]
  (let [request (read-edn source)]
    (-> (projection/requires-advanced-preparation! db ref-name request)
        (.then
         (fn [tuple-sensitive?]
           (if (or tuple-sensitive? (d/advanced-transaction? request))
             (d/prepare-transaction (database db ref-name) request)
             (js/Promise.resolve
              (d/prepare-basic-transaction request)))))
        (.then
         (fn [prepared]
           (-> (projection/prepare-transaction!
                db ref-name (:request prepared))
               (.then
                (fn [{projected-request :request plan :plan}]
                  {:prepared (assoc prepared :request projected-request)
                   :plan plan})))))
        (.then
         (fn [{:keys [prepared plan]}]
           (d/transact-prepared (database db ref-name plan) prepared)))
        (.then pr-str))))

(defn ^:export reindex-edn! [db ref-name _source]
  (let [canonical (database db ref-name)]
    (-> (engine/head canonical)
        (.then
         (fn [head]
           (when-not head
             (throw (ex-info "Cannot reindex a missing database ref"
                             {:type :kotobase.datomic/missing-ref
                              :ref ref-name})))
           (-> (d/datoms canonical {:index :eavt})
               (.then
                (fn [datoms]
                  (projection/rebuild-projection!
                   db ref-name head datoms))))))
        (.then pr-str))))

(defn- hydrate-q [canonical query args]
  (edn-promise (apply d/q query canonical (or args []))))

(defn- hydrate-pull [canonical selector eid]
  (edn-promise (d/pull canonical selector eid)))

(defn- hydrate-datoms [canonical options]
  (edn-promise (d/datoms canonical options)))

(defn- with-fresh-projection!
  "When the SQL projection is stale, rebuild once from the canonical CID head
  then retry the fast path. Unsupported queries skip rebuild and hydrate."
  [db ref-name reason retry-fast hydrate]
  (if (not= reason :stale-projection)
    (hydrate)
    (let [canonical (database db ref-name)]
      (-> (engine/head canonical)
          (.then
           (fn [head]
             (if-not head
               (hydrate)
               (-> (d/datoms canonical {:index :eavt})
                   (.then
                    (fn [datoms]
                      (projection/rebuild-projection!
                       db ref-name head datoms)))
                   (.then (fn [_] (retry-fast)))
                   (.then
                    (fn [{:keys [used? value]}]
                      (if used?
                        (pr-str value)
                        (hydrate))))))))))))

(defn ^:export q-edn! [db ref-name source]
  (let [{:keys [query args as-of since history]} (read-edn source)
        canonical (database db ref-name)]
    (if (or (some? as-of) (some? since) history)
      (let [view (cond
                   (some? as-of) (d/as-of (d/db canonical) as-of)
                   (some? since) (d/since (d/db canonical) since)
                   history (d/history (d/db canonical)))]
        (edn-promise (apply d/q query view (or args []))))
      (-> (projection/fast-q! db ref-name query (or args []))
          (.then
           (fn [{:keys [used? value reason]}]
             (if used?
               (pr-str value)
               (with-fresh-projection!
                 db ref-name reason
                 #(projection/fast-q! db ref-name query (or args []))
                 #(hydrate-q canonical query args)))))))))

(defn ^:export pull-edn! [db ref-name source]
  (let [{:keys [selector eid as-of since history]} (read-edn source)
        canonical (database db ref-name)]
    (if (or (some? as-of) (some? since) history)
      (let [view (cond
                   (some? as-of) (d/as-of (d/db canonical) as-of)
                   (some? since) (d/since (d/db canonical) since)
                   history (d/history (d/db canonical)))]
        (edn-promise (d/pull view selector eid)))
      (-> (projection/fast-pull! db ref-name selector eid)
          (.then
           (fn [{:keys [used? value reason]}]
             (if used?
               (pr-str value)
               (with-fresh-projection!
                 db ref-name reason
                 #(projection/fast-pull! db ref-name selector eid)
                 #(hydrate-pull canonical selector eid)))))))))

(defn ^:export datoms-edn! [db ref-name source]
  (let [{:keys [as-of since history] :as options} (read-edn source)
        datom-options (dissoc options :as-of :since :history)
        canonical (database db ref-name)]
    (if (or (some? as-of) (some? since) history)
      (let [view (cond
                   (some? as-of) (d/as-of (d/db canonical) as-of)
                   (some? since) (d/since (d/db canonical) since)
                   history (d/history (d/db canonical)))]
        (edn-promise (d/datoms view datom-options)))
      (-> (projection/fast-datoms! db ref-name datom-options)
          (.then
           (fn [{:keys [used? value reason]}]
             (if used?
               (pr-str value)
               (with-fresh-projection!
                 db ref-name reason
                 #(projection/fast-datoms! db ref-name datom-options)
                 #(hydrate-datoms canonical datom-options)))))))))
(defn ^:export fold-edn! [db ref-name source]
  (let [opts (read-edn source)]
    (edn-promise (d/fold (database db ref-name) opts))))

(defn ^:export view-edn! [db ref-name source]
  (let [{:keys [view]} (read-edn source)]
    (edn-promise (d/view (database db ref-name) view))))

(defn ^:export basis-edn! [db ref-name _source]
  (let [snapshot (d/db (database db ref-name))]
    (-> (js/Promise.all
         #js [(d/basis-cid snapshot) (d/basis-t snapshot)])
        (.then
         (fn [results]
           (pr-str {:basis-cid (aget results 0)
                    :basis-t (aget results 1)}))))))

(defn- outbox-row [row]
  (merge {:t (aget row "t")
          :tx-cid (aget row "tx_cid")
          :created-at (aget row "created_at")}
         (reader/read-string (aget row "payload_edn"))))

(defn- tx-range! [db ref-name start end limit]
  (-> (invoke
       (prepared
        db
        "SELECT t, tx_cid, payload_edn, created_at
         FROM kotobase_tx_outbox
         WHERE ref_name = ? AND t >= ? AND (? IS NULL OR t < ?)
         ORDER BY t LIMIT ?"
        [ref-name (or start 0) end end
         (min 1000 (max 1 (or limit 100)))])
       "all")
      (.then
       (fn [result]
         (mapv outbox-row
               (array-seq (aget result "results")))))))

(defn ^:export tx-range-edn! [db ref-name source]
  (let [{:keys [start end limit]} (read-edn source)]
    (-> (tx-range! db ref-name start end limit)
        (.then pr-str))))

(defn- valid-consumer! [consumer]
  (when-not (and (string? consumer)
                 (re-matches #"[A-Za-z0-9._:/-]{1,200}" consumer))
    (throw (ex-info "Invalid durable listener consumer"
                    {:type :kotobase.datomic/invalid-consumer})))
  consumer)

(defn ^:export listener-edn! [db ref-name source]
  (let [{:keys [op consumer since t limit]} (read-edn source)
        consumer (valid-consumer! consumer)
        now (.now js/Date)]
    (->
     (case op
       :register
       (-> (invoke
            (prepared
             db
             "INSERT INTO kotobase_listener_cursor
                (ref_name, consumer, next_t, updated_at)
              VALUES (?, ?, ?, ?)
              ON CONFLICT(ref_name, consumer) DO NOTHING"
             [ref-name consumer (or since 0) now])
            "run")
           (.then
            (fn [_]
              {:consumer consumer :registered true
               :next-t (or since 0)})))

       :poll
       (-> (invoke
            (prepared
             db
             "SELECT next_t FROM kotobase_listener_cursor
              WHERE ref_name = ? AND consumer = ?"
             [ref-name consumer])
            "first")
           (.then
            (fn [cursor]
              (when-not cursor
                (throw (ex-info "Durable listener is not registered"
                                {:type :kotobase.datomic/listener-not-found
                                 :consumer consumer})))
              (let [next-t (aget cursor "next_t")]
                (-> (tx-range! db ref-name next-t nil limit)
                    (.then
                     (fn [transactions]
                       {:consumer consumer
                        :next-t next-t
                        :transactions transactions})))))))

       :ack
       (do
         (when-not (and (number? t) (js/Number.isSafeInteger t)
                        (not (neg? t)))
           (throw (ex-info "Listener ack requires a non-negative integer :t"
                           {:type :kotobase.datomic/invalid-listener-ack})))
         (-> (invoke
              (prepared
               db
               "UPDATE kotobase_listener_cursor
                SET next_t = MAX(next_t, ?), updated_at = ?
                WHERE ref_name = ? AND consumer = ?"
               [(inc t) now ref-name consumer])
              "run")
             (.then
              (fn [result]
                (when-not (= 1 (aget (aget result "meta") "changes"))
                  (throw (ex-info "Durable listener is not registered"
                                  {:type :kotobase.datomic/listener-not-found
                                   :consumer consumer})))
                {:consumer consumer :acked-through t :next-t (inc t)}))))

       (js/Promise.reject
        (ex-info "Unknown durable listener operation"
                 {:type :kotobase.datomic/invalid-listener-operation
                  :op op})))
     (.then pr-str))))

(defn ^:export admin-edn! [db ref-name _source]
  (-> (invoke
       (prepared
        db
        "SELECT r.cid, r.revision, r.updated_at,
                p.head_cid AS projection_head,
                CASE WHEN p.head_cid = r.cid THEN 1 ELSE 0 END AS projected,
                (SELECT COUNT(*) FROM kotobase_datoms_current d
                 WHERE d.ref_name = r.name) AS datoms,
                (SELECT COUNT(*) FROM kotobase_datom_history h
                 WHERE h.ref_name = r.name) AS history_datoms,
                (SELECT COUNT(*) FROM kotobase_schema s
                 WHERE s.ref_name = r.name) AS schema_attributes,
                (SELECT COUNT(*) FROM kotobase_tx_outbox o
                 WHERE o.ref_name = r.name) AS transactions,
                (SELECT COUNT(*) FROM kotobase_listener_cursor c
                 WHERE c.ref_name = r.name) AS listeners,
                (SELECT MAX(t) FROM kotobase_tx_outbox o
                 WHERE o.ref_name = r.name) AS latest_t
         FROM kotobase_refs r
         LEFT JOIN kotobase_projection p ON p.ref_name = r.name
         WHERE r.name = ?"
        [ref-name])
       "first")
      (.then
       (fn [row]
         (pr-str
          (if row
            {:ref ref-name
             :basis-cid (aget row "cid")
             :revision (aget row "revision")
             :updated-at (aget row "updated_at")
             :projection-head (aget row "projection_head")
             :projected? (= 1 (aget row "projected"))
             :datoms (aget row "datoms")
             :history-datoms (aget row "history_datoms")
             :schema-attributes (aget row "schema_attributes")
             :transactions (aget row "transactions")
             :listeners (aget row "listeners")
             :latest-t (aget row "latest_t")}
            {:ref ref-name :missing? true}))))))
