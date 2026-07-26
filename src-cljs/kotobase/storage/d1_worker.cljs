(ns kotobase.storage.d1-worker
  "Direct Cloudflare D1 binding for the Promise-based Kotobase engine.

  The exported boundary accepts and returns EDN so Datomic keywords, symbols,
  sets, pull selectors, and query vectors survive the JavaScript transport."
  (:require [cljs.reader :as reader]
            [kotobase.datomic :as d]
            [kotobase.engine :as engine]
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

(defrecord D1Storage [db]
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
                (cid, bytes, byte_length, created_at)
                VALUES (?, ?, ?, ?) ON CONFLICT(cid) DO NOTHING"
               [cid (bytes-buffer bytes) (.-byteLength bytes) (.now js/Date)])
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
                           "SELECT bytes FROM kotobase_blocks WHERE cid = ?"
                           [cid])
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
                            "SELECT bytes FROM kotobase_blocks WHERE cid = ?"
                            [cid])
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
             (if (= 1 (aget (aget result "meta") "changes"))
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

(defn- database [db ref-name]
  (engine/open
   {:storage (->D1Storage db)
    :ref-name ref-name
    :encrypt-fn #(js/Promise.resolve %)
    :decrypt-fn #(js/Promise.resolve %)
    :blind-fn #(js/Promise.resolve (pr-str %))
    :visible? (constantly true)}))

(defn- read-edn [source]
  (reader/read-string source))

(defn- edn-promise [value]
  (-> (js/Promise.resolve value)
      (.then pr-str)))

(defn ^:export head-edn! [db ref-name]
  (edn-promise (engine/head (database db ref-name))))

(defn ^:export transact-edn! [db ref-name source]
  (let [request (read-edn source)]
    (edn-promise (d/transact (database db ref-name) request))))

(defn ^:export q-edn! [db ref-name source]
  (let [{:keys [query args]} (read-edn source)]
    (edn-promise
     (apply d/q query (database db ref-name) (or args [])))))

(defn ^:export pull-edn! [db ref-name source]
  (let [{:keys [selector eid]} (read-edn source)]
    (edn-promise
     (d/pull (database db ref-name) selector eid))))

(defn ^:export datoms-edn! [db ref-name source]
  (let [options (read-edn source)]
    (edn-promise
     (d/datoms (database db ref-name) options))))
