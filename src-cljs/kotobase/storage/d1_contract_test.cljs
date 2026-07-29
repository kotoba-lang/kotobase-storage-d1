(ns kotobase.storage.d1-contract-test
  "Runs the shared storage contract against a REAL D1 database.

  Until now this repo had no test directory at all. Its one verification
  script talks to a deployed endpoint over HTTP, which exercises the API
  surface but never the storage contract -- so `:linearizable-ref`, which
  this backend has declared since it was written, had never been checked
  against anything.

  That matters more here than for the other providers in the family,
  because this is the backend holding live kotobase.net traffic, and
  because D1 has no interactive transactions: a CAS written as read-then-
  write across two calls would be lost-update prone and would look
  perfectly correct to any single-writer test. Reading the code says it is
  not written that way -- the plain path is one conditional
  `UPDATE ... WHERE name = ? AND cid = ?`, and the projection path carries
  the same guard on every statement inside one `batch()`. This turns that
  reading into a measurement.

  Miniflare rather than a mock: its D1 is real SQLite with real `batch()`
  transaction semantics, and a hand-written stub would be a stub of the one
  thing under test."
  (:require ["miniflare" :refer [Miniflare]]
            ["node:fs" :as fs]
            ["node:path" :as path]
            [clojure.string :as string]
            [kotobase.storage.async-contract :as contract]
            [kotobase.storage.core :as storage]
            [kotobase.storage.d1-worker :as d1]))

(def ^:private failures (atom 0))

(defn- expect [ok? message]
  (if ok?
    (println (str "ok  - " message))
    (do (js/console.error (str "FAIL: " message)) (swap! failures inc))))

;; ── schema ───────────────────────────────────────────────────────────────────

(defn- statements
  "Split a migration into executable statements.

   `D1Database.exec` splits on newlines, which mangles every multi-line
   `CREATE TABLE` in this repo's migrations, so the statements are prepared
   individually instead."
  [sql]
  (->> (string/split-lines sql)
       (remove #(string/starts-with? (string/trim %) "--"))
       (string/join "\n")
       (#(string/split % #";"))
       (map string/trim)
       (remove string/blank?)))

(defn- migrate! [^js db dir]
  (let [files (sort (js->clj (.readdirSync fs dir)))]
    (reduce
     (fn [p file]
       (.then p (fn [_]
                  (let [sql (.readFileSync fs (.join path dir file) "utf8")]
                    (.batch db (to-array (map #(.prepare db %) (statements sql))))))))
     (js/Promise.resolve nil)
     files)))

;; ── does this harness have teeth on D1? ──────────────────────────────────────

(defrecord Toctou [inner]
  ;; The same real D1 underneath, with the CAS rewritten as read, compare,
  ;; then write in a later turn -- which is what the implementation would
  ;; be if the guard were lifted out of the SQL and evaluated in
  ;; JavaScript. D1 has no interactive transactions, so this is the shape
  ;; the code is one refactor away from, and it passes every sequential
  ;; check. If the race cannot catch it here, on this backend, against this
  ;; database, then a green run above means nothing.
  storage/IBlockStore
  (-put-blocks! [_ blocks] (storage/-put-blocks! inner blocks))
  (-get-blocks [_ cids] (storage/-get-blocks inner cids))

  storage/IRefStore
  (-read-ref [_ name] (storage/-read-ref inner name))
  (-compare-and-set-ref! [this name expected next]
    (-> (storage/-read-ref this name)
        (.then (fn [current]
                 (if (not= expected (:cid current))
                   {:published? false :current (:cid current)
                    :version (:version current)}
                   ;; Decided. Now write with no guard at all.
                   (-> (storage/-compare-and-set-ref!
                        inner name (:cid current) next)
                       (.then (fn [_] {:published? true :current next}))))))))

  storage/IBackendCapabilities
  (-capabilities [_] (storage/-capabilities inner)))

(defn- prove-the-race-can-fail
  "Needs its OWN database. `kotobase_blocks` is tenant-scoped (migration
   0007) but `kotobase_refs` is keyed by name alone, and the contract uses
   bare `main`/`race` names -- so a second run against the same D1 finds
   the first run's refs and dies on \"a missing ref must read as nil\"
   before reaching the race. Not a product problem: real ref names embed
   the tenant via `storage/scoped-ref`. It is a test-isolation problem, and
   it is exactly the kind that would otherwise be mistaken for the oracle
   working."
  [db]
  (-> (contract/verify (->Toctou (d1/->D1Storage db nil "toctou")))
      (.then (fn [result]
               (expect false
                       (str "a read-then-write CAS over real D1 was ACCEPTED: "
                            (pr-str result)))))
      (.catch (fn [error]
                (expect (some? (re-find #"concurrent writers all published"
                                        (or (.-message error) "")))
                        (str "a read-then-write CAS over real D1 is rejected by "
                             "the race -- got: " (.-message error)))))))

;; ── the property the API tests cannot see ────────────────────────────────────

(defn- concurrent-transact-guard
  "Beyond the shared race: D1's own projection path, contended.

   `projected-cas!` runs a whole projection delta and the ref CAS in one
   `batch()`, every statement carrying the `expected` guard. If that guard
   were evaluated anywhere but inside the transaction, two writers would
   both publish AND both write their projection rows -- leaving the
   projection describing a state no head ever had. Worth its own check,
   because the shared contract only looks at the ref."
  [db]
  (let [backend (d1/->D1Storage db nil "")
        head "cid-guard-genesis"]
    (-> (storage/-compare-and-set-ref! backend "guard" nil head)
        (.then (fn [_]
                 (js/Promise.all
                  (to-array
                   (map #(storage/-compare-and-set-ref!
                          backend "guard" head (str "cid-guard-" %))
                        (range 6))))))
        (.then (fn [results]
                 (let [results (vec (array-seq results))
                       winners (filterv :published? results)]
                   (expect (= 1 (count winners))
                           (str "6 contended writers on one D1 ref: "
                                (count winners) " published (must be 1)"))
                   (storage/-read-ref backend "guard"))))
        (.then (fn [head-now]
                 (expect (string/starts-with? (or (:cid head-now) "") "cid-guard-")
                         "and the surviving head is one of the proposals"))))))

(defn -main [& _]
  (let [mf (Miniflare. #js {:modules true
                            :script "export default {};"
                            :d1Databases #js {:DB "kotobase-contract-test"
                                              :ORACLE "kotobase-contract-oracle"}})]
    (-> (js/Promise.all #js [(.getD1Database mf "DB") (.getD1Database mf "ORACLE")])
        (.then (fn [[db oracle-db]]
                 (-> (js/Promise.all #js [(migrate! db "migrations")
                                          (migrate! oracle-db "migrations")])
                     (.then (fn [_]
                              (println "ok  - migrations applied to a real D1 (miniflare/SQLite)")
                              ;; nil projection-plan: the plain ref path.
                              ;; The projection path is exercised by
                              ;; concurrent-transact-guard below.
                              (contract/verify (d1/->D1Storage db nil ""))))
                     (.then (fn [result]
                              (println (str "D1 contract: " (pr-str result)))
                              (expect (= :linearizable-ref (:profile result))
                                      "D1 declares linearizable")
                              (expect (= :verified (:concurrency result))
                                      "and the suite actually raced it, rather than skipping")))
                     (.then (fn [_] (concurrent-transact-guard db)))
                     (.then (fn [_] (prove-the-race-can-fail oracle-db))))))
        (.catch (fn [error]
                  (js/console.error (str "FAIL: " (.-message error)))
                  (when-let [data (ex-data error)] (js/console.error (pr-str data)))
                  (swap! failures inc)))
        (.then (fn [_]
                 (-> (.dispose mf)
                     (.then (fn [_]
                              (if (zero? @failures)
                                (println "kotobase-storage-d1: all green")
                                (println (str "kotobase-storage-d1: " @failures
                                              " FAILURE(S) above")))
                              (.exit js/process (if (zero? @failures) 0 1))))))))))
