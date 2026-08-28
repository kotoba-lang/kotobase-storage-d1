(ns kotobase.storage.ipfs-contract-test
  "Qualification for the ADR-2608281000 Decision 2 wiring: blocks on IPFS,
  refs on D1, composed via `kotobase.storage.core/compose`, run against a
  REAL D1 (miniflare) exactly the way `d1-contract-test`/`r2-contract-test`
  run against a real D1/R2.

  The block half is an in-memory fake client (the same shape
  `kotobase-storage-ipfs`'s own `test/run.cljs` uses for `block-client`) --
  this test's job is to prove the COMPOSITION (D1Storage reused purely for
  its ref half; `kotobase.storage.ipfs`'s adapter as the block half) behaves
  as one linearizable-ref backend against a real database, not to exercise
  `kotobase.storage.ipfs-kubo`'s HTTP transport (covered by that library's
  own `ipfs_kubo_test.cljs`)."
  (:require ["miniflare" :refer [Miniflare]]
            [kotobase.storage.async-contract :as contract]
            [kotobase.storage.core :as storage]
            [kotobase.storage.d1-worker :as d1]
            [kotobase.storage.ipfs :as ipfs]))

(defn- fake-ipfs-client []
  (let [blocks (atom {})]
    {:put-block! (fn [cid bytes] (swap! blocks assoc cid bytes) (js/Promise.resolve cid))
     :get-block (fn [cid] (js/Promise.resolve (get @blocks cid)))}))

(defn -main [& _]
  (let [mf (Miniflare. #js {:modules true
                            :script "export default {};"
                            :d1Databases #js {:DB "ipfs-authority-contract"}})]
    (-> (.getD1Database mf "DB")
        (.then
         (fn [db]
           (-> (.exec db
                      "CREATE TABLE kotobase_refs (name TEXT PRIMARY KEY, cid TEXT NOT NULL, revision INTEGER NOT NULL DEFAULT 1, updated_at INTEGER NOT NULL);")
               (.then
                (fn [_]
                  (contract/verify
                   (storage/compose
                    {:blocks (ipfs/open {:client (fake-ipfs-client)})
                     ;; Reused purely for its IRefStore half -- compose never
                     ;; calls a `:refs` value's block methods, so the tenant
                     ;; D1Storage would otherwise scope kotobase_blocks
                     ;; writes with is irrelevant here (nil, same as
                     ;; ipfs-worker.cljs's `database`).
                     :refs (d1/->D1Storage db nil nil)})))))))
        (.then
         (fn [result]
           (when-not (and (= :linearizable-ref (:profile result))
                          (= :verified (:concurrency result)))
             (throw (js/Error. (str "unexpected IPFS-authority contract result "
                                    (pr-str result)))))
           (println (str "IPFS authority (blocks on IPFS, refs on D1) contract: "
                        (pr-str result)))))
        (.catch
         (fn [error]
           (js/console.error (or (.-stack error) (.-message error)))
           (set! (.-exitCode js/process) 1)))
        (.then (fn [_] (.dispose mf))))))
