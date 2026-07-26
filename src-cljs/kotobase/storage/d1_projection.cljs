(ns kotobase.storage.d1-projection
  "Transactional SQLite projection and SQL fast path for Datomic-shaped D1.

  Immutable IPLD blocks remain authoritative.  A projection is readable only
  when its published head equals kotobase_refs.cid; any unsupported or lagging
  projection therefore falls back to the canonical hydrate/query engine."
  (:require [cljs.reader :as reader]
            [clojure.string :as str]
            [kotobase.datomic :as d]))

(defn- invoke [target method & args]
  (.apply (aget target method) target (to-array args)))

(defn- prepared [db sql params]
  (let [statement (invoke db "prepare" sql)]
    (.apply (aget statement "bind") statement (to-array params))))

(defn- keyword-wire-value [value]
  (let [value-name (name value)]
    (if (.startsWith value-name "_")
      (str "_:"
           (when-let [value-namespace (namespace value)]
             (str value-namespace "/"))
           (subs value-name 1))
      (str value))))

(defn- wire-value [value]
  (cond
    (keyword? value) (keyword-wire-value value)
    (vector? value) (mapv wire-value value)
    (list? value) (apply list (map wire-value value))
    (set? value) (into #{} (map wire-value) value)
    (map? value) (into {}
                       (map (fn [[key item]]
                              [(wire-value key) (wire-value item)]))
                       value)
    :else value))

(defn- encoded [value]
  (pr-str (wire-value value)))

(defn- reverse-attribute [attribute]
  (when (and (keyword? attribute) (.startsWith (name attribute) "_"))
    (keyword (namespace attribute) (subs (name attribute) 1))))

(defn- values-of [value]
  (if (and (coll? value) (not (map? value)))
    value
    [value]))

(defn- entity-map-ops [entity]
  (let [entity-id (:db/id entity)]
    (mapcat
     (fn [[attribute value]]
       (when-not (= attribute :db/id)
         (map
          (fn [item]
            (if-let [forward (reverse-attribute attribute)]
              {:kind :add :e item :a forward :v entity-id}
              {:kind :add :e entity-id :a attribute :v item}))
          (values-of value))))
     entity)))

(defn- tx-item-ops [item]
  (cond
    (and (map? item) (contains? item :db/id))
    (entity-map-ops item)

    (and (vector? item) (= :db/add (first item)) (= 4 (count item)))
    [{:kind :add :e (nth item 1) :a (nth item 2) :v (nth item 3)}]

    (and (vector? item) (= :db/retract (first item)) (= 4 (count item)))
    [{:kind :retract :e (nth item 1) :a (nth item 2) :v (nth item 3)}]

    (and (vector? item) (= :db/retractEntity (first item)) (= 2 (count item)))
    [{:kind :retract-entity :e (nth item 1)}]

    :else []))

(defn- remove-entity-exact [exact entity]
  (into {}
        (remove (fn [[[candidate _ _] _]] (= candidate entity)))
        exact))

(defn transaction-plan
  "Reduce transaction order to its effective projection delta.

  retractEntity clears earlier operations for the entity while later adds are
  retained.  The canonical engine remains responsible for validating the
  complete transaction grammar."
  [request]
  (let [tx-data (if (map? request) (:tx-data request) request)
        state
        (reduce
         (fn [{:keys [exact entities] :as state} operation]
           (let [{:keys [kind e a v]} operation
                 e (encoded e)]
             (case kind
               :retract-entity
               (assoc state
                      :entities (conj entities e)
                      :exact (remove-entity-exact exact e))

               (:add :retract)
               (let [a (encoded a)
                     v (encoded v)]
                 (assoc state :exact
                        (assoc exact [e a v]
                               {:kind kind :e e :a a :v v})))

               state)))
         {:exact {} :entities #{}}
         (mapcat tx-item-ops tx-data))
        exact-ops (vals (:exact state))
        entity-ops (map (fn [entity]
                          {:kind :retract-entity :e entity})
                        (:entities state))
        operations (vec (concat entity-ops exact-ops))
        history (mapv (fn [ordinal operation]
                        (assoc operation
                               :ordinal ordinal
                               :added (if (= :add (:kind operation)) 1 0)))
                      (range)
                      operations)]
    {:operations operations
     :history history
     :recompute-all? (boolean (seq entity-ops))
     :attributes (into #{} (keep :a) exact-ops)}))

(defn- json-source [value]
  (js/JSON.stringify (clj->js value)))

(defn- guard [expected]
  (if (nil? expected)
    {:sql "NOT EXISTS
           (SELECT 1 FROM kotobase_refs r WHERE r.name = ?)
           AND NOT EXISTS
           (SELECT 1 FROM kotobase_projection p WHERE p.ref_name = ?)"
     :params nil}
    {:sql "EXISTS
           (SELECT 1 FROM kotobase_refs r
            WHERE r.name = ? AND r.cid = ?)
           AND EXISTS
           (SELECT 1 FROM kotobase_projection p
            WHERE p.ref_name = ? AND p.head_cid = ?)"
     :params expected}))

(defn- guard-params [name expected]
  (if (nil? expected)
    [name name]
    [name expected name expected]))

(defn- projection-statements
  [db name expected next {:keys [operations history recompute-all? attributes]}]
  (let [{guard-sql :sql} (guard expected)
        guard-values (guard-params name expected)
        operations-json (json-source operations)
        history-json (json-source history)
        attributes-json (json-source (vec attributes))
        now (.now js/Date)
        statements
        [(prepared
          db
          (str
           "DELETE FROM kotobase_datoms_current
            WHERE ref_name = ?
              AND e_edn IN
                  (SELECT json_extract(value, '$.e')
                   FROM json_each(?)
                   WHERE json_extract(value, '$.kind') = 'retract-entity')
              AND " guard-sql)
          (into [name operations-json] guard-values))

         (prepared
          db
          (str
           "DELETE FROM kotobase_datoms_current
            WHERE ref_name = ?
              AND EXISTS
                  (SELECT 1 FROM json_each(?) op
                   WHERE json_extract(op.value, '$.kind') = 'retract'
                     AND json_extract(op.value, '$.e') = e_edn
                     AND json_extract(op.value, '$.a') = a_edn
                     AND json_extract(op.value, '$.v') = v_edn)
              AND " guard-sql)
          (into [name operations-json] guard-values))

         (prepared
          db
          (str
           "INSERT INTO kotobase_datoms_current
              (ref_name, e_edn, a_edn, v_edn, tx_cid)
            SELECT ?, json_extract(value, '$.e'),
                   json_extract(value, '$.a'),
                   json_extract(value, '$.v'), ?
            FROM json_each(?)
            WHERE json_extract(value, '$.kind') = 'add'
              AND " guard-sql "
            ON CONFLICT(ref_name, e_edn, a_edn, v_edn) DO NOTHING")
          (into [name next operations-json] guard-values))

         (prepared
          db
          (str
           "INSERT INTO kotobase_datom_history
              (ref_name, tx_cid, ordinal, added, e_edn, a_edn, v_edn)
            SELECT ?, ?, json_extract(value, '$.ordinal'),
                   json_extract(value, '$.added'),
                   json_extract(value, '$.e'),
                   json_extract(value, '$.a'),
                   json_extract(value, '$.v')
            FROM json_each(?)
            WHERE " guard-sql)
          (into [name next history-json] guard-values))]
        statements
        (if recompute-all?
          (conj
           statements
           (prepared
            db
            (str "DELETE FROM kotobase_attribute_stats
                  WHERE ref_name = ? AND " guard-sql)
            (into [name] guard-values)))
          (conj
           statements
           (prepared
            db
            (str
             "DELETE FROM kotobase_attribute_stats
              WHERE ref_name = ?
                AND a_edn IN (SELECT value FROM json_each(?))
                AND " guard-sql)
            (into [name attributes-json] guard-values))))
        stats-filter (if recompute-all?
                       ""
                       "AND a_edn IN (SELECT value FROM json_each(?))")
        stats-params (if recompute-all?
                       (into [name next name] guard-values)
                       (into [name next name attributes-json] guard-values))
        statements
        (conj
         statements
         (prepared
          db
          (str
           "INSERT INTO kotobase_attribute_stats
              (ref_name, a_edn, datom_count, basis_cid)
            SELECT ?, a_edn, COUNT(*), ?
            FROM kotobase_datoms_current
            WHERE ref_name = ? " stats-filter "
              AND " guard-sql "
            GROUP BY a_edn")
          stats-params))
        projection-statement
        (if (nil? expected)
          (prepared
           db
           (str
            "INSERT INTO kotobase_projection(ref_name, head_cid, updated_at)
             SELECT ?, ?, ?
             WHERE " guard-sql "
             ON CONFLICT(ref_name) DO NOTHING")
           (into [name next now] guard-values))
          (prepared
           db
           "UPDATE kotobase_projection
            SET head_cid = ?, updated_at = ?
            WHERE ref_name = ? AND head_cid = ?
              AND EXISTS
                  (SELECT 1 FROM kotobase_refs r
                   WHERE r.name = ? AND r.cid = ?)"
           [next now name expected name expected]))
        ref-statement
        (if (nil? expected)
          (prepared
           db
           "INSERT INTO kotobase_refs(name, cid, revision, updated_at)
            VALUES (?, ?, 1, ?) ON CONFLICT(name) DO NOTHING"
           [name next now])
          (prepared
           db
           "UPDATE kotobase_refs
            SET cid = ?, revision = revision + 1, updated_at = ?
            WHERE name = ? AND cid = ?"
           [next now name expected]))]
    (conj statements projection-statement ref-statement)))

(defn projected-cas!
  "Atomically publish projection delta and mutable ref.

  The last result is always the ref CAS, so callers retain the IRefStore
  compare-and-set contract."
  [db name expected next plan]
  (-> (invoke db "batch"
              (to-array
               (projection-statements db name expected next plan)))
      (.then
       (fn [results]
         (let [last-result (aget results (dec (.-length results)))
               changes (aget (aget last-result "meta") "changes")]
           {:published? (= 1 changes)
            :current (when (= 1 changes) next)})))))

(defn- datalog-var? [value]
  (and (symbol? value) (.startsWith (name value) "?")))

(defn- input-bindings [query args]
  (let [specs (remove #{'$} (:in query))]
    (when (and (= (count specs) (count args))
               (every? datalog-var? specs))
      (zipmap specs (map wire-value args)))))

(defn- aggregate-count? [find-element]
  (and (seq? find-element)
       (= 'count (first find-element))
       (= 2 (count find-element))
       (datalog-var? (second find-element))))

(defn- compile-sql [ref-name query args]
  (let [{compiled :query shape :shape named? :named?} (d/compile-query query)
        where (:where compiled)
        inputs (input-bindings compiled args)]
    (when (and inputs
               (seq where)
               (every? #(and (vector? %) (= 3 (count %))) where))
      (let [columns ["e_edn" "a_edn" "v_edn"]
            bindings (atom {})
            conditions (atom [])
            params (atom [])
            aliases (mapv #(str "d" %) (range (count where)))]
        (doseq [[alias clause] (map vector aliases where)]
          (swap! conditions conj (str alias ".ref_name = ?"))
          (swap! params conj ref-name)
          (doseq [[column term] (map vector columns clause)]
            (let [qualified (str alias "." column)]
              (cond
                (= term '_) nil
                (contains? inputs term)
                (do (swap! conditions conj (str qualified " = ?"))
                    (swap! params conj (encoded (get inputs term))))
                (datalog-var? term)
                (if-let [previous (get @bindings term)]
                  (swap! conditions conj (str qualified " = " previous))
                  (swap! bindings assoc term qualified))
                :else
                (do (swap! conditions conj (str qualified " = ?"))
                    (swap! params conj (encoded term)))))))
        (let [find-spec (:find compiled)
              count-only? (and (= 1 (count find-spec))
                               (aggregate-count? (first find-spec)))
              plain? (every? datalog-var? find-spec)]
          (when (or count-only? plain?)
            (let [selects
                  (if count-only?
                    [(str "COUNT(DISTINCT "
                          (get @bindings (second (first find-spec)))
                          ") AS c0")]
                    (mapv (fn [index variable]
                            (str (get @bindings variable)
                                 " AS c" index))
                          (range)
                          find-spec))]
              (when (every? some? selects)
                {:sql (str "SELECT DISTINCT " (str/join ", " selects)
                           " FROM kotobase_datoms_current "
                           (str/join ", kotobase_datoms_current " aliases)
                           " WHERE " (str/join " AND " @conditions))
                 :params @params
                 :shape shape
                 :named? named?
                 :query compiled
                 :count-only? count-only?
                 :column-count (count selects)}))))))))

(defn- projection-current? [db ref-name]
  (-> (invoke
       (prepared
        db
        "SELECT 1 AS current
         FROM kotobase_refs r
         JOIN kotobase_projection p
           ON p.ref_name = r.name AND p.head_cid = r.cid
         WHERE r.name = ?"
        [ref-name])
       "first")
      (.then boolean)))

(defn- row-values [row {:keys [column-count count-only?]}]
  (mapv
   (fn [index]
     (let [value (aget row (str "c" index))]
       (if count-only? value (reader/read-string value))))
   (range column-count)))

(defn- shape-result [rows {:keys [shape named? query]}]
  (let [relation (into #{} rows)]
    (if named?
      (let [[kind labels]
            (cond
              (seq (:keys query)) [:keys (:keys query)]
              (seq (:strs query)) [:strs (:strs query)]
              :else [:syms (:syms query)])]
        (mapv
         (fn [tuple]
           (into {}
                 (map (fn [label value]
                        [(case kind
                           :keys (keyword (name label))
                           :strs (name label)
                           :syms (symbol (name label)))
                         value])
                      labels tuple)))
         relation))
      (case shape
        :scalar (ffirst relation)
        :collection (mapv first relation)
        :tuple (first relation)
        relation))))

(defn fast-q!
  "Return {:used? true :value x} for the SQL-compatible subset, otherwise
  {:used? false}.  A stale/missing projection is never queried."
  [db ref-name query args]
  (if-let [plan (compile-sql ref-name query args)]
    (-> (projection-current? db ref-name)
        (.then
         (fn [current?]
           (if-not current?
             {:used? false}
             (-> (invoke (prepared db (:sql plan) (:params plan)) "all")
                 (.then
                  (fn [result]
                    (let [rows (mapv #(row-values % plan)
                                     (array-seq (aget result "results")))]
                      {:used? true
                       :value (shape-result rows plan)}))))))))
    (js/Promise.resolve {:used? false})))

(defn- flat-pull-selector? [selector]
  (and (vector? selector)
       (every?
        (fn [attribute]
          (or (= '* attribute)
              (and (keyword? attribute)
                   (not (.startsWith (name attribute) "_")))
              (and (string? attribute)
                   (not (.startsWith attribute "_")))))
        selector)))

(defn fast-pull!
  "Indexed EAVT pull for wildcard and flat forward attributes."
  [db ref-name selector eid]
  (if-not (flat-pull-selector? selector)
    (js/Promise.resolve {:used? false})
    (-> (projection-current? db ref-name)
        (.then
         (fn [current?]
           (if-not current?
             {:used? false}
             (let [wildcard? (some #{'*} selector)
                   attributes (mapv encoded (remove #{'*} selector))
                   sql (str
                        "SELECT a_edn, v_edn
                         FROM kotobase_datoms_current
                         WHERE ref_name = ? AND e_edn = ?"
                        (when-not wildcard?
                          " AND a_edn IN
                              (SELECT value FROM json_each(?))")
                        " ORDER BY a_edn, v_edn")
                   params (cond-> [ref-name (encoded eid)]
                            (not wildcard?)
                            (conj (json-source attributes)))]
               (-> (invoke (prepared db sql params) "all")
                   (.then
                    (fn [result]
                      {:used? true
                       :value
                       (reduce
                        (fn [pulled row]
                          (let [attribute
                                (reader/read-string (aget row "a_edn"))
                                value
                                (reader/read-string (aget row "v_edn"))]
                            (update pulled attribute
                                    (fnil conj #{}) value)))
                        {}
                        (array-seq (aget result "results")))}))))))))))

(def ^:private index-columns
  {:eavt ["e_edn" "a_edn" "v_edn"]
   :aevt ["a_edn" "e_edn" "v_edn"]
   :avet ["a_edn" "v_edn" "e_edn"]
   :vaet ["v_edn" "a_edn" "e_edn"]})

(defn fast-datoms!
  "Indexed current-basis datom access for EAVT, AEVT, AVET, and VAET."
  [db ref-name {:keys [index components limit]}]
  (let [index (or index :eavt)
        columns (get index-columns index)]
    (if (or (nil? columns)
            (> (count components) (count columns))
            (and limit (not (pos-int? limit))))
      (js/Promise.resolve {:used? false})
      (-> (projection-current? db ref-name)
          (.then
           (fn [current?]
             (if-not current?
               {:used? false}
               (let [component-conditions
                     (mapv #(str % " = ?")
                           (take (count components) columns))
                     conditions
                     (into ["ref_name = ?"] component-conditions)
                     sql
                     (str
                      "SELECT e_edn, a_edn, v_edn, tx_cid
                       FROM kotobase_datoms_current
                       WHERE " (str/join " AND " conditions) "
                       ORDER BY " (str/join ", " columns)
                      (when limit " LIMIT ?"))
                     params
                     (cond-> (into [ref-name] (map encoded components))
                       limit (conj limit))]
                 (-> (invoke (prepared db sql params) "all")
                     (.then
                      (fn [result]
                        {:used? true
                         :value
                         (mapv
                          (fn [row]
                            {:e (reader/read-string (aget row "e_edn"))
                             :a (reader/read-string (aget row "a_edn"))
                             :v_edn (aget row "v_edn")
                             :tx (aget row "tx_cid")
                             :added true})
                          (array-seq (aget result "results")))})))))))))))
