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

(defn- encoded-value
  "Encode the value as kotobase-peer stores it today.

  The peer validates typed values before persistence, then preserves its
  established string-valued arrangement format. Keeping the projection in
  that same representation makes reindex and fallback queries identical."
  [value]
  (encoded (if (string? value) value (str value))))

(defn- reverse-attribute [attribute]
  (when (and (keyword? attribute) (.startsWith (name attribute) "_"))
    (keyword (namespace attribute) (subs (name attribute) 1))))

(def ^:private schema-vector-attributes
  #{:db/tupleAttrs :db/tupleTypes :db/fn})

(defn- values-of [attribute value]
  (if (and (coll? value) (not (map? value))
           (not (contains? schema-vector-attributes attribute)))
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
          (values-of attribute value))))
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

(def ^:private supported-value-types
  #{:db.type/string :db.type/boolean :db.type/long :db.type/double
    :db.type/keyword :db.type/ref :db.type/instant :db.type/uuid
    :db.type/symbol :db.type/tuple :db.type/fn})

(declare transaction-plan json-source)

(defn- schema-definition [entity]
  (when (and (map? entity)
             (contains? entity :db/ident)
             (contains? entity :db/valueType))
    (let [attribute (:db/ident entity)
          value-type (:db/valueType entity)
          cardinality (:db/cardinality entity)
          unique-kind (:db/unique entity)
          tuple-attrs (:db/tupleAttrs entity)
          tuple-types (:db/tupleTypes entity)
          tuple-type (:db/tupleType entity)]
      (when-not (keyword? attribute)
        (throw (ex-info "Schema :db/ident must be a keyword"
                        {:type :kotobase.datomic/invalid-schema
                         :attribute attribute})))
      (when-not (contains? supported-value-types value-type)
        (throw (ex-info "Unsupported Datomic :db/valueType"
                        {:type :kotobase.datomic/invalid-schema
                         :attribute attribute
                         :value-type value-type})))
      (when-not (contains? #{:db.cardinality/one :db.cardinality/many}
                           cardinality)
        (throw (ex-info "Invalid Datomic :db/cardinality"
                        {:type :kotobase.datomic/invalid-schema
                         :attribute attribute
                         :cardinality cardinality})))
      (when-not (or (nil? unique-kind)
                    (contains? #{:db.unique/identity :db.unique/value}
                               unique-kind))
        (throw (ex-info "Invalid Datomic :db/unique"
                        {:type :kotobase.datomic/invalid-schema
                         :attribute attribute
                         :unique unique-kind})))
      (when (= :db.type/tuple value-type)
        (let [definitions (remove nil? [tuple-attrs tuple-types tuple-type])]
          (when-not (= 1 (count definitions))
            (throw (ex-info "Tuple schema requires exactly one tuple definition"
                            {:type :kotobase.datomic/invalid-schema
                             :attribute attribute})))
          (when (and tuple-attrs
                     (not (and (vector? tuple-attrs)
                               (<= 2 (count tuple-attrs) 8)
                               (every? keyword? tuple-attrs))))
            (throw (ex-info "Invalid :db/tupleAttrs"
                            {:type :kotobase.datomic/invalid-schema
                             :attribute attribute})))
          (when (and tuple-types
                     (not (and (vector? tuple-types)
                               (<= 2 (count tuple-types) 8)
                               (every? supported-value-types tuple-types))))
            (throw (ex-info "Invalid :db/tupleTypes"
                            {:type :kotobase.datomic/invalid-schema
                             :attribute attribute})))))
      {:a attribute
       :a-edn (encoded attribute)
       :value-type value-type
       :cardinality cardinality
       :unique-kind unique-kind
       :tuple-attrs tuple-attrs
       :tuple-types tuple-types
       :tuple-type tuple-type})))

(defn- transaction-schema-definitions [request]
  (let [requested (if (map? request) (:tx-data request) request)
        schema-attributes
        #{:db/ident :db/valueType :db/cardinality :db/unique
          :db/tupleAttrs :db/tupleTypes :db/tupleType}
        entities
        (reduce
         (fn [result {:keys [kind e a v]}]
           (if (and (= :add kind) (contains? schema-attributes a))
             (assoc-in result [e a] v)
             result))
         {}
         (mapcat tx-item-ops requested))]
    (vec (keep (comp schema-definition val) entities))))

(defn- value-type? [value-type value]
  (and
   (some? value)
   (case value-type
     :db.type/string (string? value)
     :db.type/boolean (boolean? value)
     :db.type/long (and (number? value) (js/Number.isSafeInteger value))
     :db.type/double (number? value)
     :db.type/keyword (keyword? value)
     :db.type/ref (or (string? value) (keyword? value)
                      (and (number? value) (js/Number.isSafeInteger value)))
     :db.type/instant (or (inst? value) (instance? js/Date value))
     :db.type/uuid (uuid? value)
     :db.type/symbol (symbol? value)
     :db.type/fn (or (map? value) (string? value))
     :db.type/tuple (and (vector? value) (<= 2 (count value) 8))
     false)))

(defn- schema-row [row]
  {:a-edn (aget row "a_edn")
   :value-type (reader/read-string (aget row "value_type"))
   :cardinality (reader/read-string (aget row "cardinality"))
   :unique-kind (some-> (aget row "unique_kind") reader/read-string)
   :tuple-attrs (some-> (aget row "tuple_attrs_edn") reader/read-string)
   :tuple-types (some-> (aget row "tuple_types_edn") reader/read-string)
   :tuple-type (some-> (aget row "tuple_type_edn") reader/read-string)})

(defn- load-schema! [db ref-name]
  (-> (invoke
       (prepared
        db
        "SELECT a_edn, value_type, cardinality, unique_kind,
                tuple_attrs_edn, tuple_types_edn, tuple_type_edn
         FROM kotobase_schema WHERE ref_name = ?"
        [ref-name])
       "all")
      (.then
       (fn [result]
         (into {}
               (map (fn [row]
                      (let [schema (schema-row row)]
                        [(:a-edn schema) schema])))
               (array-seq (aget result "results")))))))

(defn requires-advanced-preparation!
  "Return a Promise indicating whether REQUEST touches a composite tuple.

  D1's zero-read fast path remains available for unrelated transactions;
  tuple constituents use the canonical preparation path so derived tuple
  datoms and identity upserts stay backend-independent."
  [db ref-name request]
  (let [requested (if (map? request) (:tx-data request) request)
        inline-composite?
        (boolean
         (some #(and (map? %) (seq (:db/tupleAttrs %))) requested))
        touched
        (into #{}
              (keep (fn [{:keys [a]}] (some-> a encoded)))
              (mapcat tx-item-ops requested))]
    (if inline-composite?
      (js/Promise.resolve true)
      (-> (load-schema! db ref-name)
          (.then
           (fn [schemas]
             (boolean
              (some
               (fn [[_ definition]]
                 (some touched
                       (map encoded (:tuple-attrs definition))))
               schemas))))))))

(defn- current-head! [db ref-name]
  (-> (invoke
       (prepared
        db
        "SELECT r.cid,
                CASE WHEN p.head_cid = r.cid THEN 1 ELSE 0 END AS projected
         FROM kotobase_refs r
         LEFT JOIN kotobase_projection p ON p.ref_name = r.name
         WHERE r.name = ?"
        [ref-name])
       "first")
      (.then
       (fn [row]
         {:head (some-> row (aget "cid"))
          :projected? (and row (= 1 (aget row "projected")))}))))

(defn- selected-current-datoms!
  [db ref-name pair-keys unique-keys schema-attributes]
  (let [pair-json (json-source pair-keys)
        unique-json (json-source unique-keys)
        schema-json (json-source schema-attributes)]
    (-> (invoke
         (prepared
          db
          "SELECT e_edn, a_edn, v_edn
           FROM kotobase_datoms_current d
           WHERE d.ref_name = ?
             AND (
               EXISTS (
                 SELECT 1 FROM json_each(?) x
                 WHERE json_extract(x.value, '$.e') = d.e_edn
                   AND json_extract(x.value, '$.a') = d.a_edn)
               OR EXISTS (
                 SELECT 1 FROM json_each(?) x
                 WHERE json_extract(x.value, '$.a') = d.a_edn
                   AND json_extract(x.value, '$.v') = d.v_edn)
               OR d.a_edn IN (SELECT value FROM json_each(?))
             )"
          [ref-name pair-json unique-json schema-json])
         "all")
        (.then
         (fn [result]
           (mapv
            (fn [row]
              {:e (aget row "e_edn")
               :a (aget row "a_edn")
               :v (aget row "v_edn")})
            (array-seq (aget result "results"))))))))

(defn- validate-schema-install! [definition rows]
  (let [attribute (:a-edn definition)
        matching (filter #(= attribute (:a %)) rows)]
    ;; kotobase-peer's established arrangement stores non-ref values as
    ;; strings after validating their original type. Historical datoms
    ;; therefore cannot prove whether "42" originated as a long or a string.
    ;; Enforce valueType on every new write; validate only the recoverable
    ;; cardinality/uniqueness invariants during install/reindex.
    (when (= :db.cardinality/one (:cardinality definition))
      (when (some (fn [[_ values]] (> (count values) 1))
                  (group-by :e matching))
        (throw (ex-info "Existing values violate :db.cardinality/one"
                        {:type :kotobase.datomic/schema-conflict
                         :attribute (:a definition)}))))
    (when (:unique-kind definition)
      (when (some (fn [[_ values]]
                    (> (count (into #{} (map :e) values)) 1))
                  (group-by :v matching))
        (throw (ex-info "Existing values violate :db/unique"
                        {:type :kotobase.datomic/schema-conflict
                         :attribute (:a definition)}))))))

(defn- enrich-transaction
  [request schemas current-rows definitions validated-head]
  (let [requested (if (map? request) (:tx-data request) request)
        operations (vec (mapcat tx-item-ops requested))
        current-by-pair
        (reduce (fn [result {:keys [e a v]}]
                  (update result [e a] (fnil conj #{}) v))
                {}
                current-rows)
        current-unique
        (reduce (fn [result {:keys [e a v]}]
                  (assoc result [a v] e))
                {}
                current-rows)
        state
        (reduce
         (fn [{:keys [tx-data pair-values unique-values]} operation]
           (let [{:keys [kind e a v]} operation
                 e-edn (encoded e)
                 a-edn (when a (encoded a))
                 v-edn (when (some? v) (encoded-value v))
                 schema (get schemas a-edn)]
             (when (and (= :add kind) schema
                        (not (value-type? (:value-type schema) v)))
               (throw (ex-info "Value violates Datomic :db/valueType"
                               {:type :kotobase.datomic/value-type
                                :entity e
                                :attribute a
                                :value v
                                :value-type (:value-type schema)})))
             (case kind
               :retract-entity
               {:tx-data (conj tx-data [:db/retractEntity e])
                :pair-values
                (into {}
                      (remove (fn [[[candidate _] _]]
                                (= candidate e-edn)))
                      pair-values)
                :unique-values
                (into {}
                      (remove (fn [[_ owner]] (= owner e-edn)))
                      unique-values)}

               :retract
               {:tx-data (conj tx-data [:db/retract e a v])
                :pair-values (update pair-values [e-edn a-edn] disj v-edn)
                :unique-values (if (= e-edn (get unique-values [a-edn v-edn]))
                                 (dissoc unique-values [a-edn v-edn])
                                 unique-values)}

               :add
               (let [one? (= :db.cardinality/one (:cardinality schema))
                     old-values (if one?
                                  (get pair-values [e-edn a-edn] #{})
                                  #{})
                     replacements (remove #{v-edn} old-values)
                     tx-data
                     (into tx-data
                           (map (fn [old]
                                  [:db/retract e a (reader/read-string old)]))
                           replacements)
                     owner (when (:unique-kind schema)
                             (get unique-values [a-edn v-edn]))]
                 (when (and owner (not= owner e-edn))
                   (throw (ex-info "Value violates Datomic :db/unique"
                                   {:type :kotobase.datomic/unique-conflict
                                    :entity e
                                    :attribute a
                                    :value v})))
                 {:tx-data (conj tx-data [:db/add e a v])
                  :pair-values
                  (assoc pair-values [e-edn a-edn]
                         (if one?
                           #{v-edn}
                           (conj (get pair-values [e-edn a-edn] #{}) v-edn)))
                  :unique-values
                  (if (:unique-kind schema)
                    (assoc unique-values [a-edn v-edn] e-edn)
                    unique-values)})

               {:tx-data tx-data
                :pair-values pair-values
                :unique-values unique-values})))
         {:tx-data []
          :pair-values current-by-pair
          :unique-values current-unique}
         operations)
        enriched-request
        (if (map? request)
          (assoc request :tx-data (:tx-data state))
          (:tx-data state))]
    {:request enriched-request
     :plan (assoc (transaction-plan enriched-request)
                  :validated-head validated-head
                  :schema-upserts definitions
                  :unique-attributes
                  (into
                   (into #{} (keep (fn [definition]
                                     (when (:unique-kind definition)
                                       (:a-edn definition))))
                         definitions)
                   (keep
                    (fn [{:keys [a]}]
                      (let [a-edn (some-> a encoded)]
                        (when (:unique-kind (get schemas a-edn))
                          a-edn))))
                   (mapcat tx-item-ops
                           (if (map? request) (:tx-data request) request))))}))

(defn prepare-transaction!
  "Validate and normalize a transaction against the current schema projection.

  Cardinality-one adds are expanded with the canonical retractions required to
  keep the immutable block chain and SQL projection byte-for-byte equivalent."
  [db ref-name request]
  (let [definitions (transaction-schema-definitions request)]
    (-> (js/Promise.all
         #js [(current-head! db ref-name)
              (load-schema! db ref-name)])
        (.then
         (fn [results]
           (let [{:keys [head projected?]} (aget results 0)
                 existing (aget results 1)
                 definitions-by-attribute
                 (into {} (map (juxt :a-edn identity)) definitions)
                 schemas (merge existing definitions-by-attribute)]
             (when (and head (seq existing) (not projected?))
               (throw
                (ex-info "Schema projection is stale; run /v1/reindex"
                         {:type :kotobase.datomic/reindex-required
                          :ref ref-name
                          :head head})))
             (if (empty? schemas)
               ;; Preserve the established schemaless bulk path: no current
               ;; datom lookup, no second operation normalization pass.
               {:request request
                :plan (assoc (transaction-plan request)
                             :validated-head head
                             :schema-upserts []
                             :unique-attributes #{})}
               (let [operations
                     (vec
                      (mapcat tx-item-ops
                              (if (map? request) (:tx-data request) request)))
                     schema-ops
                     (filter #(get schemas (some-> (:a %) encoded)) operations)
                     pair-keys
                     (into []
                           (keep
                            (fn [{:keys [e a]}]
                              (let [a-edn (encoded a)
                                    schema (get schemas a-edn)]
                                (when (= :db.cardinality/one
                                         (:cardinality schema))
                                  {:e (encoded e) :a a-edn}))))
                           schema-ops)
                     unique-keys
                     (into []
                           (keep
                            (fn [{:keys [a v kind]}]
                              (let [a-edn (encoded a)
                                    schema (get schemas a-edn)]
                                (when (and (= :add kind)
                                           (:unique-kind schema))
                                  {:a a-edn :v (encoded-value v)}))))
                           schema-ops)
                     schema-attributes (mapv :a-edn definitions)]
                 (-> (selected-current-datoms!
                      db ref-name pair-keys unique-keys schema-attributes)
                     (.then
                      (fn [rows]
                        (doseq [definition definitions]
                          (when-let [installed
                                     (get existing (:a-edn definition))]
                            (when-not
                             (= (select-keys installed
                                             [:value-type :cardinality
                                              :unique-kind :tuple-attrs
                                              :tuple-types :tuple-type])
                                (select-keys definition
                                             [:value-type :cardinality
                                              :unique-kind :tuple-attrs
                                              :tuple-types :tuple-type]))
                              (throw
                               (ex-info "Installed Datomic schema is immutable"
                                        {:type :kotobase.datomic/schema-conflict
                                         :attribute (:a definition)}))))
                          (validate-schema-install! definition rows))
                        (enrich-transaction request schemas rows definitions
                                            head))))))))))))

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
                     v (encoded-value v)]
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
  [db name expected next
   {:keys [operations history recompute-all? attributes schema-upserts
           unique-attributes]}]
  (let [{guard-sql :sql} (guard expected)
        guard-values (guard-params name expected)
        operations-json (json-source operations)
        history-json (json-source history)
        outbox-edn (pr-str {:data history})
        attributes-json (json-source (vec attributes))
        schema-json
        (json-source
         (mapv (fn [{:keys [a-edn value-type cardinality unique-kind
                            tuple-attrs tuple-types tuple-type]}]
                 {:a a-edn
                  :valueType (str value-type)
                  :cardinality (str cardinality)
                  :uniqueKind (some-> unique-kind str)
                  :tupleAttrs (some-> tuple-attrs pr-str)
                  :tupleTypes (some-> tuple-types pr-str)
                  :tupleType (some-> tuple-type pr-str)})
               schema-upserts))
        unique-attributes-json (json-source (vec unique-attributes))
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
        (if (seq schema-upserts)
          (conj
           statements
           (prepared
            db
            (str
             "INSERT INTO kotobase_schema
                (ref_name, a_edn, value_type, cardinality, unique_kind,
                 basis_cid, tuple_attrs_edn, tuple_types_edn, tuple_type_edn)
              SELECT ?, json_extract(value, '$.a'),
                     json_extract(value, '$.valueType'),
                     json_extract(value, '$.cardinality'),
                     json_extract(value, '$.uniqueKind'), ?,
                     json_extract(value, '$.tupleAttrs'),
                     json_extract(value, '$.tupleTypes'),
                     json_extract(value, '$.tupleType')
              FROM json_each(?)
              WHERE " guard-sql "
              ON CONFLICT(ref_name, a_edn) DO UPDATE SET
                basis_cid = excluded.basis_cid,
                tuple_attrs_edn = excluded.tuple_attrs_edn,
                tuple_types_edn = excluded.tuple_types_edn,
                tuple_type_edn = excluded.tuple_type_edn")
            (into [name next schema-json] guard-values)))
          statements)
        statements
        (if (seq unique-attributes)
          (into
           statements
           [(prepared
             db
             (str
              "DELETE FROM kotobase_unique_values
               WHERE ref_name = ?
                 AND a_edn IN (SELECT value FROM json_each(?))
                 AND " guard-sql)
             (into [name unique-attributes-json] guard-values))
            (prepared
             db
             (str
              "INSERT INTO kotobase_unique_values
                 (ref_name, a_edn, v_edn, e_edn, basis_cid)
               SELECT d.ref_name, d.a_edn, d.v_edn, d.e_edn, ?
               FROM kotobase_datoms_current d
               JOIN kotobase_schema s
                 ON s.ref_name = d.ref_name AND s.a_edn = d.a_edn
               WHERE d.ref_name = ?
                 AND s.unique_kind IS NOT NULL
                 AND d.a_edn IN (SELECT value FROM json_each(?))
                 AND " guard-sql)
             (into [next name unique-attributes-json] guard-values))])
          statements)
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
        outbox-statement
        (if (nil? expected)
          (prepared
           db
           (str
            "INSERT INTO kotobase_tx_outbox
               (ref_name, t, tx_cid, payload_edn, created_at)
             SELECT ?, 0, ?, ?, ?
             WHERE " guard-sql "
             ON CONFLICT(ref_name, t) DO NOTHING")
           (into [name next outbox-edn now] guard-values))
          (prepared
           db
           (str
            "INSERT INTO kotobase_tx_outbox
               (ref_name, t, tx_cid, payload_edn, created_at)
             SELECT ?, r.revision, ?, ?, ?
             FROM kotobase_refs r
             WHERE r.name = ? AND r.cid = ? AND " guard-sql "
             ON CONFLICT(ref_name, t) DO NOTHING")
           (into [name next outbox-edn now name expected] guard-values)))
        statements (conj statements outbox-statement)
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
  (if (and (contains? plan :validated-head)
           (not= expected (:validated-head plan)))
    (js/Promise.reject
     (ex-info "Schema basis changed; retry transaction validation"
              {:type :kotobase.datomic/schema-basis-changed
               :validated (:validated-head plan)
               :current expected}))
    (-> (invoke db "batch"
                (to-array
                 (projection-statements db name expected next plan)))
        (.then
         (fn [results]
           (let [last-result (aget results (dec (.-length results)))
                 changes (aget (aget last-result "meta") "changes")]
             {:published? (= 1 changes)
              :current (when (= 1 changes) next)}))))))

(defn- wire-keyword [value]
  (if (and (string? value) (.startsWith value ":"))
    (reader/read-string value)
    value))

(defn- schema-wire-value [value]
  (let [value
        (if (string? value)
          (try (reader/read-string value)
               (catch :default _ value))
          value)]
    (cond
      (keyword? value) value
      (vector? value) (mapv schema-wire-value value)
      :else (wire-keyword value))))

(defn- schema-definitions-from-datoms [datoms]
  (let [entities
        (reduce
         (fn [result {:keys [e a v_edn]}]
           (assoc-in result [e a]
                     (schema-wire-value (reader/read-string v_edn))))
         {}
         datoms)]
    (into
     []
     (keep
      (fn [[_ attributes]]
        (when (and (get attributes ":db/ident")
                   (get attributes ":db/valueType"))
          (let [ident (get attributes ":db/ident")]
            (schema-definition
             {:db/ident ident
              :db/valueType (get attributes ":db/valueType")
              :db/cardinality (get attributes ":db/cardinality")
              :db/unique (get attributes ":db/unique")
              :db/tupleAttrs (get attributes ":db/tupleAttrs")
              :db/tupleTypes (get attributes ":db/tupleTypes")
              :db/tupleType (get attributes ":db/tupleType")}))))
      entities))))

(defn rebuild-projection!
  "Atomically rebuild the current SQL projection from hydrated canonical datoms.

  The final publication is conditional on the ref still naming `head`; a
  concurrent transaction therefore leaves the old projection readable and the
  caller can retry from the new canonical basis."
  [db ref-name head datoms]
  (let [rows
        (mapv
         (fn [{:keys [e a v_edn]}]
           {:e (encoded e) :a (encoded a) :v v_edn})
         datoms)
        definitions (schema-definitions-from-datoms datoms)
        validation-rows (mapv #(select-keys % [:e :a :v]) rows)
        _ (doseq [definition definitions]
            (validate-schema-install! definition validation-rows))
        row-chunks (partition-all 500 rows)
        schema-json
        (json-source
         (mapv (fn [{:keys [a-edn value-type cardinality unique-kind
                            tuple-attrs tuple-types tuple-type]}]
                 {:a a-edn
                  :valueType (str value-type)
                  :cardinality (str cardinality)
                  :uniqueKind (some-> unique-kind str)
                  :tupleAttrs (some-> tuple-attrs pr-str)
                  :tupleTypes (some-> tuple-types pr-str)
                  :tupleType (some-> tuple-type pr-str)})
               definitions))
        now (.now js/Date)
        ref-guard
        "EXISTS (SELECT 1 FROM kotobase_refs
                 WHERE name = ? AND cid = ?)"
        statements
        [(prepared db
                   (str "DELETE FROM kotobase_datoms_current
                         WHERE ref_name = ? AND " ref-guard)
                   [ref-name ref-name head])
         (prepared db
                   (str "DELETE FROM kotobase_attribute_stats
                         WHERE ref_name = ? AND " ref-guard)
                   [ref-name ref-name head])
         (prepared db
                   (str "DELETE FROM kotobase_unique_values
                         WHERE ref_name = ? AND " ref-guard)
                   [ref-name ref-name head])
         (prepared db
                   (str "DELETE FROM kotobase_schema
                         WHERE ref_name = ? AND " ref-guard)
                   [ref-name ref-name head])]
        statements
        (into
         statements
         (map
          (fn [chunk]
            (prepared
             db
             (str
              "INSERT INTO kotobase_datoms_current
                 (ref_name, e_edn, a_edn, v_edn, tx_cid)
               SELECT ?, json_extract(value, '$.e'),
                      json_extract(value, '$.a'),
                      json_extract(value, '$.v'), ?
               FROM json_each(?)
               WHERE " ref-guard)
             [ref-name head (json-source (vec chunk)) ref-name head]))
          row-chunks))
        statements
        (cond-> statements
          (seq definitions)
          (conj
           (prepared
            db
            (str
             "INSERT INTO kotobase_schema
                (ref_name, a_edn, value_type, cardinality, unique_kind,
                 basis_cid, tuple_attrs_edn, tuple_types_edn, tuple_type_edn)
              SELECT ?, json_extract(value, '$.a'),
                     json_extract(value, '$.valueType'),
                     json_extract(value, '$.cardinality'),
                     json_extract(value, '$.uniqueKind'), ?,
                     json_extract(value, '$.tupleAttrs'),
                     json_extract(value, '$.tupleTypes'),
                     json_extract(value, '$.tupleType')
              FROM json_each(?)
              WHERE " ref-guard)
            [ref-name head schema-json ref-name head])))
        statements
        (into
         statements
         [(prepared
           db
           (str
            "INSERT INTO kotobase_attribute_stats
               (ref_name, a_edn, datom_count, basis_cid)
             SELECT ?, a_edn, COUNT(*), ?
             FROM kotobase_datoms_current
             WHERE ref_name = ? AND " ref-guard "
             GROUP BY a_edn")
           [ref-name head ref-name ref-name head])
          (prepared
           db
           (str
            "INSERT INTO kotobase_unique_values
               (ref_name, a_edn, v_edn, e_edn, basis_cid)
             SELECT d.ref_name, d.a_edn, d.v_edn, d.e_edn, ?
             FROM kotobase_datoms_current d
             JOIN kotobase_schema s
               ON s.ref_name = d.ref_name AND s.a_edn = d.a_edn
             WHERE d.ref_name = ? AND s.unique_kind IS NOT NULL
               AND " ref-guard)
           [head ref-name ref-name head])
          (prepared
           db
           (str
            "INSERT INTO kotobase_projection(ref_name, head_cid, updated_at)
             SELECT ?, ?, ? WHERE " ref-guard "
             ON CONFLICT(ref_name) DO UPDATE SET
               head_cid = excluded.head_cid,
               updated_at = excluded.updated_at")
           [ref-name head now ref-name head])])]
    (-> (invoke db "batch" (to-array statements))
        (.then
         (fn [results]
           (let [last-result (aget results (dec (.-length results)))
                 changes (aget (aget last-result "meta") "changes")]
             (if (= 1 changes)
               {:reindexed? true
                :head head
                :datom-count (count rows)
                :schema-count (count definitions)}
               {:reindexed? false
                :head head
                :reason :head-changed})))))))

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
                    (swap! params conj
                           ((if (= column "v_edn") encoded-value encoded)
                            (get inputs term))))
                (datalog-var? term)
                (if-let [previous (get @bindings term)]
                  (swap! conditions conj (str qualified " = " previous))
                  (swap! bindings assoc term qualified))
                :else
                (do (swap! conditions conj (str qualified " = ?"))
                    (swap! params conj
                           ((if (= column "v_edn") encoded-value encoded)
                            term)))))))
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
                     (cond-> (into [ref-name]
                                   (map (fn [column component]
                                          ((if (= column "v_edn")
                                             encoded-value
                                             encoded)
                                           component))
                                        columns components))
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
