(ns kotobase.storage.r2-contract-test
  (:require ["miniflare" :refer [Miniflare]]
            [kotobase.storage.async-contract :as contract]
            [kotobase.storage.r2-worker :as r2]))

(defn -main [& _]
  (let [mf (Miniflare. #js {:modules true
                            :script "export default {};"
                            :d1Databases #js {:DB "r2-rollback-mirror"}
                            :r2Buckets #js {:R2 "r2-authority-contract"}})]
    (-> (js/Promise.all #js [(.getD1Database mf "DB")
                             (.getR2Bucket mf "R2")])
        (.then
         (fn [[db bucket]]
           (-> (.exec db
                      "CREATE TABLE kotobase_blocks (cid TEXT PRIMARY KEY, bytes BLOB NOT NULL, byte_length INTEGER NOT NULL DEFAULT 0, created_at INTEGER NOT NULL DEFAULT 0);")
               (.then (fn [_]
                        (contract/verify
                         (r2/->R2Storage db bucket "contract")))))))
        (.then
         (fn [result]
           (when-not (and (= :linearizable-ref (:profile result))
                          (= :verified (:concurrency result)))
             (throw (js/Error. (str "unexpected R2 contract result "
                                    (pr-str result)))))
           (println (str "R2 authority contract: " (pr-str result)))))
        (.catch
         (fn [error]
           (js/console.error (or (.-stack error) (.-message error)))
           (set! (.-exitCode js/process) 1)))
        (.then (fn [_] (.dispose mf))))))
