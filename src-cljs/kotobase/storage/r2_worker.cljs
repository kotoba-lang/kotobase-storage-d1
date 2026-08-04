(ns kotobase.storage.r2-worker
  "Datomic-compatible canonical engine over a Cloudflare R2 head and blocks.

  D1 remains a rebuildable query projection and rollback mirror. The mutable
  authority in this namespace is the R2 ref object's ETag CAS."
  (:require [cljs.reader :as reader]
            [goog.object :as gobj]
            [kotobase.datomic :as d]
            [kotobase.engine :as engine]
            [kotobase.storage.core :as storage]
            [kotobase.storage.d1-projection :as projection]))

(defn- invoke [target method & args]
  (.apply (gobj/get target method) target (to-array args)))

(defn- clean-namespace [value]
  (let [value (or (not-empty (str value)) "production")]
    (when-not (re-matches #"[A-Za-z0-9._-]{1,80}" value)
      (throw (ex-info "invalid R2 namespace"
                      {:type :kotobase.storage/invalid-namespace})))
    value))

(defn- prefix [namespace]
  (str "kotobase/datomic/v2/" (clean-namespace namespace) "/canonical"))

(defn- block-key [namespace cid]
  (str (prefix namespace) "/blocks/" cid))

(defn- ref-key [namespace ref-name]
  (str (prefix namespace) "/refs/" (js/encodeURIComponent ref-name)))

(defn- bytes= [left right]
  (let [left (js/Uint8Array. left)
        right (js/Uint8Array. right)]
    (and (= (.-byteLength left) (.-byteLength right))
         (loop [index 0]
           (cond
             (= index (.-byteLength left)) true
             (= (aget left index) (aget right index)) (recur (inc index))
             :else false)))))

(defn- object-bytes [object]
  (-> (invoke object "arrayBuffer")
      (.then #(js/Uint8Array. %))))

(defn- object-ref [object]
  (-> (invoke object "text")
      (.then js/JSON.parse)
      (.then
       (fn [value]
         {:cid (gobj/get value "cid")
          :revision (gobj/get value "revision")
          :version (gobj/get object "etag")}))))

(defn- prepared [db sql params]
  (let [statement (invoke db "prepare" sql)]
    (.apply (gobj/get statement "bind") statement (to-array params))))

(defn- bytes-buffer [bytes]
  (let [buffer (.-buffer bytes)
        offset (.-byteOffset bytes)
        length (.-byteLength bytes)]
    (if (and (zero? offset) (= length (.-byteLength buffer)))
      buffer
      (.slice buffer offset (+ offset length)))))

(defn- mirror-block! [db cid bytes]
  (-> (invoke
       (prepared db
                 "INSERT INTO kotobase_blocks
                    (cid, bytes, byte_length, created_at)
                  VALUES (?, ?, ?, ?) ON CONFLICT(cid) DO NOTHING"
                 [cid (bytes-buffer bytes) (.-byteLength bytes) (.now js/Date)])
       "run")
      (.then
       (fn [_]
         (invoke (prepared db "SELECT bytes FROM kotobase_blocks WHERE cid = ?"
                           [cid])
                 "first")))
      (.then
       (fn [row]
         (when-not (and row (bytes= (gobj/get row "bytes") bytes))
           (throw (ex-info "D1 rollback mirror CID collision"
                           {:type :kotobase.storage/cid-collision
                            :cid cid})))
         cid))))

(defrecord R2Storage [db bucket namespace]
  storage/IBlockStore
  (-put-blocks! [_ blocks]
    (-> (js/Promise.all
         (clj->js
          (mapv
           (fn [{:keys [cid bytes]}]
             (-> (js/Promise.all
                  #js [(mirror-block! db cid bytes)
                       (invoke bucket "put" (block-key namespace cid) bytes
                               #js {:onlyIf #js {:etagDoesNotMatch "*"}})])
                 (.then
                  (fn [results]
                    (let [created (aget results 1)]
                    (if created
                      cid
                      (-> (invoke bucket "get" (block-key namespace cid))
                          (.then
                           (fn [existing]
                             (when-not existing
                               (throw (ex-info "R2 immutable block disappeared"
                                               {:type :kotobase.storage/block-missing
                                                :cid cid})))
                             (-> (object-bytes existing)
                                 (.then
                                  (fn [stored]
                                    (when-not (bytes= stored bytes)
                                      (throw
                                       (ex-info "R2 immutable CID collision"
                                                {:type :kotobase.storage/cid-collision
                                                 :cid cid})))
                                    cid))))))))))))
           blocks)))
        (.then (fn [_] (mapv :cid blocks)))))

  (-get-blocks [_ cids]
    (-> (js/Promise.all
         (clj->js
          (mapv
           (fn [cid]
             (-> (invoke bucket "get" (block-key namespace cid))
                 (.then
                  (fn [object]
                    (when object
                      (-> (object-bytes object)
                          (.then (fn [bytes] [cid bytes]))))))))
           cids)))
        (.then
         (fn [entries]
           (->> (array-seq entries)
                (remove nil?)
                (into {}))))))

  storage/IRefStore
  (-read-ref [_ name]
    (-> (invoke bucket "get" (ref-key namespace name))
        (.then (fn [object] (when object (object-ref object))))))

  (-compare-and-set-ref! [this name expected next]
    (-> (storage/-read-ref this name)
        (.then
         (fn [current]
           (if (not= expected (:cid current))
             {:published? false :current (:cid current)
              :version (:version current)}
             (let [revision (inc (or (:revision current) 0))
                   body (.encode
                         (js/TextEncoder.)
                         (js/JSON.stringify
                          #js {:version 2 :ref name :cid next
                               :revision revision
                               :source_updated_at (.now js/Date)
                               :mirrored_at (.now js/Date)}))
                   options
                   #js {:onlyIf
                        (if current
                          #js {:etagMatches (:version current)}
                          #js {:etagDoesNotMatch "*"})}]
               (-> (invoke bucket "put" (ref-key namespace name) body options)
                   (.then
                    (fn [written]
                      (if written
                        {:published? true :current next
                         :version (gobj/get written "etag")}
                        (-> (storage/-read-ref this name)
                            (.then
                             (fn [winner]
                               {:published? false :current (:cid winner)
                                :version (:version winner)})))))))))))))

  storage/IBackendCapabilities
  (-capabilities [_]
    (conj storage/required-capabilities :linearizable-ref)))

(defn- database [db bucket namespace ref-name]
  (engine/open
   {:storage (->R2Storage db bucket namespace)
    :ref-name ref-name
    :encrypt-fn #(js/Promise.resolve %)
    :decrypt-fn #(js/Promise.resolve %)
    :blind-fn #(js/Promise.resolve (pr-str %))
    :visible? (constantly true)}))

(defn- read-edn [source] (reader/read-string source))
(defn- edn-promise [value] (-> (js/Promise.resolve value) (.then pr-str)))

(defn ^:export head-edn! [db bucket namespace ref-name]
  (edn-promise (engine/head (database db bucket namespace ref-name))))

(defn ^:export basis-edn! [db bucket namespace ref-name _source]
  (let [snapshot (d/db (database db bucket namespace ref-name))]
    (-> (js/Promise.all #js [(d/basis-cid snapshot) (d/basis-t snapshot)])
        (.then
         (fn [values]
           (pr-str {:basis-cid (aget values 0) :basis-t (aget values 1)}))))))

(defn ^:export q-edn! [db bucket namespace ref-name source]
  (let [{:keys [query args as-of since history]} (read-edn source)
        canonical (database db bucket namespace ref-name)
        view (cond
               (some? as-of) (d/as-of (d/db canonical) as-of)
               (some? since) (d/since (d/db canonical) since)
               history (d/history (d/db canonical))
               :else canonical)]
    (edn-promise (apply d/q query view (or args [])))))

(defn ^:export pull-edn! [db bucket namespace ref-name source]
  (let [{:keys [selector eid as-of since history]} (read-edn source)
        canonical (database db bucket namespace ref-name)
        view (cond
               (some? as-of) (d/as-of (d/db canonical) as-of)
               (some? since) (d/since (d/db canonical) since)
               history (d/history (d/db canonical))
               :else canonical)]
    (edn-promise (d/pull view selector eid))))

(defn ^:export datoms-edn! [db bucket namespace ref-name source]
  (let [{:keys [as-of since history] :as options} (read-edn source)
        canonical (database db bucket namespace ref-name)
        view (cond
               (some? as-of) (d/as-of (d/db canonical) as-of)
               (some? since) (d/since (d/db canonical) since)
               history (d/history (d/db canonical))
               :else canonical)]
    (edn-promise (d/datoms view (dissoc options :as-of :since :history)))))

(defn ^:export fold-edn! [db bucket namespace ref-name source]
  (edn-promise (d/fold (database db bucket namespace ref-name) (read-edn source))))

(defn ^:export view-edn! [db bucket namespace ref-name source]
  (edn-promise (d/view (database db bucket namespace ref-name)
                       (:view (read-edn source)))))

(defn ^:export parity! [db bucket namespace ref-name]
  (let [canonical (database db bucket namespace ref-name)]
    (-> (engine/head canonical)
        (.then
         (fn [head]
           (-> (d/datoms canonical {:index :eavt})
               (.then
                (fn [datoms]
                  #js {:head head :datomCount (count datoms)}))))))))

(defn ^:export transact-edn! [db bucket namespace ref-name source]
  (let [request (read-edn source)
        canonical (database db bucket namespace ref-name)]
    (-> (projection/requires-advanced-preparation! db ref-name request)
        (.then
         (fn [advanced?]
           (if (or advanced? (d/advanced-transaction? request))
             (d/prepare-transaction canonical request)
             (js/Promise.resolve (d/prepare-basic-transaction request)))))
        (.then
         (fn [prepared]
           (-> (projection/prepare-transaction! db ref-name (:request prepared))
               (.then
                (fn [{projected-request :request plan :plan}]
                  {:prepared (assoc prepared :request projected-request)
                   :plan plan})))))
        (.then
         (fn [{:keys [prepared plan]}]
           (-> (d/transact-prepared canonical prepared)
               (.then
                (fn [report]
                  (-> (projection/projected-cas!
                       db ref-name (:db-before report) (:db-after report) plan)
                      ;; R2 already won its CAS. A projection outage must not
                      ;; turn the committed write into a client retry.
                      (.catch (fn [_] nil))
                      (.then (fn [_] report))))))))
        (.then pr-str))))
