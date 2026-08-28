(ns kotobase.storage.ipfs-worker
  "Datomic-compatible canonical engine over a real, independently operated
  IPFS network as the BLOCK plane. D1 remains the ref authority (its own
  conditional write on `kotobase_refs`) -- this namespace only swaps WHERE
  immutable blocks live, never who arbitrates the mutable head.

  ## Why this file exists (ADR-2608281000 Decision 2)

  `kotobase.storage.ipfs` (kotoba-lang/kotobase-storage-ipfs) already
  implements `IBlockStore` over an injected client, and
  `kotobase.storage.ipfs-kubo` (same repo) is a real client for an
  independently operated Kubo node. Neither was wired into this Worker's
  provider selection. This file is that wiring, composed the way the
  library's own README prescribes:

    (storage/compose {:blocks (ipfs adapter) :refs (a ref store)})

  Blocks go to IPFS; refs stay on `kotobase.storage.d1-worker/->D1Storage`,
  reused here for ITS OWN conditional-write implementation only -- `compose`
  never calls a `:refs` value's block methods, so the tenant it is
  constructed with is irrelevant (D1Storage's `-read-ref`/
  `-compare-and-set-ref!` never reference it) and is left `nil`.

  ## Deliberately narrower than `d1-worker`/`r2-worker`

  Both siblings also maintain a D1 SQL projection (`kotobase_datoms_current`
  etc.) for a zero-basis-read fast read path, and `r2-worker` additionally
  mirrors every block into D1 for rollback safety. This namespace does
  neither: every read and write goes through the canonical engine directly.
  That is strictly correct (the projection is an optimization, never a
  correctness dependency -- ADR-2608148200's siblings treat it the same
  way for `as-of`/`since`/`history` reads already), just without the fast
  path's speed. This is the FIRST cut of an explicitly non-default,
  opt-in provider; folding in the projection fast path is a follow-up, not
  a correctness gap in what ships here.

  ## Not usable without operator-supplied Kubo config

  There is no default Kubo node baked in anywhere in this file or
  `kotobase.storage.ipfs-kubo`. `worker.mjs` only reaches this namespace
  when BOTH `KOTOBASE_AUTHORITY=ipfs` AND the Kubo endpoint bindings are
  present; see its `ipfsAuthority`/`ipfsConfig`."
  (:require [cljs.reader :as reader]
            [goog.object :as gobj]
            [kotobase.datomic :as d]
            [kotobase.engine :as engine]
            [kotobase.storage.core :as storage]
            [kotobase.storage.d1-worker :as d1]
            [kotobase.storage.ipfs :as ipfs]
            [kotobase.storage.ipfs-kubo :as ipfs-kubo]))

(defn- ipfs-config->clj
  "`config` is the plain JS object `worker.mjs` builds from env bindings:
  `{apiUrl, gatewayUrl, token}` (`token` optional). `goog.object/get`, not
  `.-`, deliberately -- `config` is built in worker.mjs, which this
  `:advanced`-optimized module does not compile together with, so a raw
  dot-property access is eligible for Closure's property renaming and would
  silently read `undefined` (the same hazard `d1-worker`/`r2-worker` avoid
  with `aget`/`gobj/get` on every externally constructed object they read)."
  [config]
  {:api-url (gobj/get config "apiUrl")
   :gateway-url (gobj/get config "gatewayUrl")
   :token (gobj/get config "token")})

(defn- database [db ipfs-config ref-name]
  (engine/open
   {:storage (storage/compose
              {:blocks (ipfs/open {:client (ipfs-kubo/open (ipfs-config->clj ipfs-config))})
               :refs (d1/->D1Storage db nil nil)})
    :ref-name ref-name
    :encrypt-fn #(js/Promise.resolve %)
    :decrypt-fn #(js/Promise.resolve %)
    :blind-fn #(js/Promise.resolve (pr-str %))
    :visible? (constantly true)}))

(defn- read-edn [source] (reader/read-string source))
(defn- edn-promise [value] (-> (js/Promise.resolve value) (.then pr-str)))

(defn ^:export head-edn! [db ipfs-config ref-name]
  (edn-promise (engine/head (database db ipfs-config ref-name))))

(defn ^:export basis-edn! [db ipfs-config ref-name _source]
  (let [snapshot (d/db (database db ipfs-config ref-name))]
    (-> (js/Promise.all #js [(d/basis-cid snapshot) (d/basis-t snapshot)])
        (.then
         (fn [values]
           (pr-str {:basis-cid (aget values 0) :basis-t (aget values 1)}))))))

(defn- as-view [canonical as-of since history]
  (cond
    (some? as-of) (d/as-of (d/db canonical) as-of)
    (some? since) (d/since (d/db canonical) since)
    history (d/history (d/db canonical))
    :else canonical))

(defn ^:export q-edn! [db ipfs-config ref-name source]
  (let [{:keys [query args as-of since history]} (read-edn source)
        canonical (database db ipfs-config ref-name)
        view (as-view canonical as-of since history)]
    (edn-promise (apply d/q query view (or args [])))))

(defn ^:export pull-edn! [db ipfs-config ref-name source]
  (let [{:keys [selector eid as-of since history]} (read-edn source)
        canonical (database db ipfs-config ref-name)
        view (as-view canonical as-of since history)]
    (edn-promise (d/pull view selector eid))))

(defn ^:export datoms-edn! [db ipfs-config ref-name source]
  (let [{:keys [as-of since history] :as options} (read-edn source)
        canonical (database db ipfs-config ref-name)
        view (as-view canonical as-of since history)]
    (edn-promise (d/datoms view (dissoc options :as-of :since :history)))))

(defn ^:export fold-edn! [db ipfs-config ref-name source]
  (edn-promise (d/fold (database db ipfs-config ref-name) (read-edn source))))

(defn ^:export view-edn! [db ipfs-config ref-name source]
  (edn-promise (d/view (database db ipfs-config ref-name) (:view (read-edn source)))))

(defn ^:export transact-edn! [db ipfs-config ref-name source]
  (let [request (read-edn source)
        canonical (database db ipfs-config ref-name)]
    (-> (if (d/advanced-transaction? request)
          (d/prepare-transaction canonical request)
          (js/Promise.resolve (d/prepare-basic-transaction request)))
        (.then (fn [prepared] (d/transact-prepared canonical prepared)))
        (.then pr-str))))
