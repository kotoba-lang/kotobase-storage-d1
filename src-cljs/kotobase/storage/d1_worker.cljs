(ns kotobase.storage.d1-worker
  "Direct Cloudflare D1 binding for the Promise-based Kotobase engine.

  The exported boundary accepts and returns EDN so Datomic keywords, symbols,
  sets, pull selectors, and query vectors survive the JavaScript transport."
  (:require [cljs.reader :as reader]
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

;; `tenant` is the authenticated did:key the blocks belong to. It is threaded
;; in from the request boundary rather than parsed back out of `ref-name`,
;; because the authenticated issuer is the authority on ownership and the ref
;; name is merely required to agree with it (see d1-auth/policy-port). Block
;; keys are scoped by it: `cid` alone was global while refs were per-DID, so
;; any caller could squat a CID another tenant's graph would later read
;; (migration 0004).
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
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(principal, cid) DO NOTHING"
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

(defn ping
  "Liveness of the D1 binding itself."
  [db]
  (-> (invoke (prepared db "SELECT 1 AS ok" []) "first")
      (.then (fn [row] (= 1 (some-> row (aget "ok")))))))

(defn read-ref
  "Current cid/revision of a ref, or nil."
  [db name]
  (-> (invoke (prepared db "SELECT cid, revision FROM kotobase_refs WHERE name = ?" [name])
              "first")
      (.then (fn [row]
               (when row {:cid (aget row "cid") :revision (aget row "revision")})))))

(defn commit-block!
  "The raw block-put + ref-CAS endpoint, moved out of the old worker.mjs.

  Note what this is and is not: `cid` here is an opaque, caller-chosen key,
  not a verified content address -- the verification suite commits under
  literal \"cid-a\"/\"cid-b\". Content-addressing proper is the engine's job on
  the transact path, where CIDs are derived from the bytes rather than
  supplied alongside them. The collision check below therefore means only
  \"this key already holds different bytes\", which is why the key had to
  become tenant-scoped (migration 0004): while it was global, first writer
  won for every tenant at once.

  Resolves to {:status :body} -- transport is the caller's business."
  [db tenant ref expected cid bytes]
  (-> (invoke
       (prepared
        db
        "INSERT INTO kotobase_blocks (principal, cid, bytes, byte_length, created_at)
         VALUES (?, ?, ?, ?, ?) ON CONFLICT(principal, cid) DO NOTHING"
        [tenant cid (bytes-buffer bytes) (.-byteLength bytes) (.now js/Date)])
       "run")
      (.then
       (fn [_]
         (invoke (prepared db
                           "SELECT bytes FROM kotobase_blocks
                            WHERE principal = ? AND cid = ?"
                           [tenant cid])
                 "first")))
      (.then
       (fn [stored]
         (if-not (and stored (same-bytes? (aget stored "bytes") bytes))
           {:status 409 :body {:ok false :error "CidCollision"}}
           (-> (invoke
                (if (nil? expected)
                  (prepared db
                            "INSERT INTO kotobase_refs(name, cid, revision, updated_at)
                             VALUES (?, ?, 1, ?) ON CONFLICT(name) DO NOTHING"
                            [ref cid (.now js/Date)])
                  (prepared db
                            "UPDATE kotobase_refs
                             SET cid = ?, revision = revision + 1, updated_at = ?
                             WHERE name = ? AND cid = ?"
                            [cid (.now js/Date) ref expected]))
                "run")
               (.then
                (fn [result]
                  (-> (read-ref db ref)
                      (.then
                       (fn [current]
                         (if (= 1 (aget (aget result "meta") "changes"))
                           {:status 200
                            :body {:ok true
                                   :cid (:cid current)
                                   :revision (:revision current)}}
                           {:status 409
                            :body {:ok false :error "CasConflict"
                                   :current current}}))))))))))))

(defn- database
  ([db tenant ref-name] (database db tenant ref-name nil))
  ([db tenant ref-name projection-plan]
  (engine/open
   {:storage (->D1Storage db projection-plan tenant)
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

(defn ^:export head-edn! [db tenant ref-name]
  (edn-promise (engine/head (database db tenant ref-name))))

(defn ^:export transact-edn! [db tenant ref-name source]
  (let [request (read-edn source)
        plan (projection/transaction-plan request)]
    (edn-promise
     (d/transact (database db tenant ref-name plan) request))))

(defn ^:export q-edn! [db tenant ref-name source]
  (let [{:keys [query args]} (read-edn source)]
    (-> (projection/fast-q! db ref-name query (or args []))
        (.then
         (fn [{:keys [used? value]}]
           (if used?
             (pr-str value)
             (edn-promise
              (apply d/q query (database db tenant ref-name) (or args [])))))))))

(defn ^:export pull-edn! [db tenant ref-name source]
  (let [{:keys [selector eid]} (read-edn source)]
    (-> (projection/fast-pull! db ref-name selector eid)
        (.then
         (fn [{:keys [used? value]}]
           (if used?
             (pr-str value)
             (edn-promise
              (d/pull (database db tenant ref-name) selector eid))))))))

(defn ^:export datoms-edn! [db tenant ref-name source]
  (let [options (read-edn source)]
    (-> (projection/fast-datoms! db ref-name options)
        (.then
         (fn [{:keys [used? value]}]
           (if used?
             (pr-str value)
             (edn-promise
              (d/datoms (database db tenant ref-name) options))))))))
