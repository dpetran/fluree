(ns fluree.db.query.fql-parser
  (:require [fluree.db.util.core :as util :refer [try* catch*]]
            [fluree.db.dbproto :as dbproto]
            [clojure.string :as str]
            [fluree.db.spec :as spec]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.schema :as schema-util]
            [fluree.db.constants :as const]
            [fluree.json-ld :as json-ld]))

#?(:clj (set! *warn-on-reflection* true))

(defn where-clause-valid?
  "Checks to see if the where clause has ' = ', ' > ', ' < ', ' <= ', or ' >= ', and returns true if yes"
  [where-clause]
  (and (string? where-clause)
       (re-find #"^.* (=|>|<|>=|<=|not=) .*$" where-clause)))

(defn parse-where-spec
  "Returns a where clause spec based on a string where caluse.
  The spec can be applied to a specific db to coerce predicate names
  to predicate ids, validate types, and ultimately generate a filtering function."
  ([where]
   (parse-where-spec where nil))
  ([where default-collection]
   (when-not (where-clause-valid? where) (throw (ex-info (str "Invalid where clause: " where)
                                                         {:status 400 :error :db/invalid-query})))
   (let [where-type (cond
                      (and (re-matches #".+ AND .+" where) (not (re-matches #".+ OR .+" where)))
                      :and

                      (and (re-matches #".+ OR .+" where) (not (re-matches #".+ AND .+" where)))
                      :or

                      ;; single clause.. use 'or' for now as it is lazy (but otherwise shouldn't matter)
                      (and (not (re-matches #".+ OR .+" where)) (not (re-matches #".+ AND .+" where)))
                      :or

                      :else
                      (throw (ex-info (str "Where clause can have either AND or OR operations, but not both currently: " where)
                                      {:status 400 :error :db/invalid-query})))
         args       (case where-type
                      :and (str/split where #" AND ")
                      :or (str/split where #" OR "))
         statements (reduce
                      (fn [acc arg]
                        (let [arg       (str/trim arg)
                              [_ ^String pred-name ^String op ^String match] (re-find #"^([^\s=><].+)[\s]+:?(=|>|<|>=|<=|not=)[\s]+:?(.+)$" arg)
                              pred-name (cond
                                          (schema-util/reverse-ref? pred-name false)
                                          (throw (ex-info (str "Reverse references cannot be used in a where clause. Provided: " pred-name)
                                                          {:status 400 :error :db/invalid-query}))

                                          (str/includes? pred-name "/")
                                          pred-name

                                          default-collection
                                          (str default-collection "/" pred-name)

                                          :else
                                          (throw (ex-info (str "Only full namespaced predicate
                                         names can be used in a where clause. This can be
                                         provided in a from clause. Provided: " pred-name)
                                                          {:status 400 :error :db/invalid-query})))
                              op*       (case op
                                          "=" =
                                          ">" >
                                          ">=" >=
                                          "<" <
                                          "<=" <=
                                          "not=" not=)
                              match*    (if-let [match-str (or (re-find #"'(.*)'$" match) (re-find #"\"(.*)\"$" match))]
                                          (second match-str)
                                          ;; must be numeric
                                          ;; TODO - we should look up predicate type and do conversion according to it
                                          (try*
                                            (cond
                                              (= "true" match)
                                              true

                                              (= "false" match)
                                              false

                                              (str/includes? match ".")
                                              #?(:clj  (Double/parseDouble match)
                                                 :cljs (js/parseFloat match))

                                              :else
                                              #?(:clj  (Long/parseLong match)
                                                 :cljs (js/parseInt match)))
                                            (catch* _
                                                    (throw (ex-info (str "Invalid where clause in argument: " arg)
                                                                    {:status 400
                                                                     :error  :db/invalid-query})))))]
                          (conj acc [pred-name op* match*])))
                      [] args)]
     [where-type statements])))

(defn conform-where
  "Conforms a where spec, using provided db, to use predicate ids and coerces where match values.
  If an predicate does not exist, it drops it."
  [db [where-type statements]]
  (let [statements* (->> statements
                         (map
                           (fn [[pred-name op o]]
                             (let [pid (dbproto/-p-prop db :id pred-name)]
                               (when pid
                                 (let [pred-type (dbproto/-p-prop db :type pid)
                                       o*        (spec/type-check o pred-type)]
                                   [pid op o*])))))
                         (remove nil?)
                         (not-empty))]
    (when statements*
      [where-type statements*])))


(defn parse-where
  "Parses a where clause and coerces the predicates "
  ([db where]
   (parse-where db where nil))
  ([db where default-collection]
   (let [where-spec (parse-where-spec where default-collection)]
     (conform-where db where-spec))))


(defn- to-select-map
  "Takes a sequential select statement and turns it into a map."
  [select]
  (reduce
    (fn [acc x]
      (let [kv (cond
                 (sequential? x) [(util/keyword->str (first x))
                                  (as-> (second x) v*
                                        (if (coll? v*) (to-select-map v*) v*))]

                 ;; must be a map within a sequence...
                 (map? x)
                 (let [values (mapv (fn [x-item]
                                      (let [key-x  (-> x-item
                                                       key
                                                       util/keyword->str)
                                            val-x  (val x-item)
                                            val-x' (cond
                                                     (= "_orderBy" key-x)
                                                     (if (coll? val-x)
                                                       {:order     (first val-x)
                                                        :predicate (second val-x)}
                                                       {:order     "ASC"
                                                        :predicate val-x})

                                                     (coll? val-x)
                                                     (to-select-map val-x)

                                                     :else val-x)]
                                        [key-x val-x'])) x)]
                   (into [] (flatten values)))

                 :else [x nil])]

        (apply assoc acc kv)))
    {} select))

;; TODO - check :limit below and default setting
(defn parse
  "Parses select statement into our own select format.
  Has no dependency on a database, or a given schema
  at this point.

  Select spec has the following keys:
  :name  - name of the predicate
  :namespace? - if the predicate 'name' has a namespace included
  :wildcard? - true if wildcard query
  :id? - true if  _id was explicitly requested
  :reverse? - if this predicate is a reverse lookup. 'name' will be the non-reverse named predicate
  :as - The result key name to use in the results map for this predicate
  :recur - number of recur iterations, if specified
  :limit - limit the number of results, if specified (100 default)
  :offset - offset the number of results, if specified (0 default)
  :select - sub-selection for this predicate - only applicable to 'ref' predicates
  :compact - If we are to remove namespaces (or names from reverse refs)
  :context - parsed context if supplied with original query
  "
  [select context opts]
  (let [select           (if (coll? select) (to-select-map select) select)
        default-compact? (:compact opts)]
    (reduce-kv
      (fn [acc k v]
        (let [pred     (cond (keyword? k)
                             (util/keyword->str k)

                             (symbol? k)
                             (str k)

                             :else
                             k)
              v'       (if (sequential? v) (to-select-map v) v)
              compact? (if (contains? v' "_compact")
                         (get v' "_compact")
                         default-compact?)]
          (cond
            (= "*" pred) (assoc acc :wildcard? true
                                    :compact? compact?
                                    :context context)
            (= "_id" pred) (assoc acc :id? true)
            :else
            (let [_          (when (and v' (not (map? v)))
                               (throw (ex-info (str "Invalid select spec: " select)
                                               {:status 400
                                                :db     :db/invalid-query})))

                  sub-select (some-> (dissoc v' "_limit" "_offset" "_as" "_recur" "_component" "_orderBy" "_compact")
                                     (not-empty)
                                     (parse context opts))
                  namespace? (str/includes? pred "/")
                  reversed   (or (get-in context [pred :reverse])
                                 (when (str/includes? pred "/_")
                                   (str/replace pred "/_" "/")))
                  pred'      (or reversed
                                 (json-ld/expand-iri pred context))
                  as         (cond
                               (contains? v' "_as")
                               (get v' "_as")

                               (and default-compact? reversed)
                               (re-find #"^[^/]+" pred)

                               (and default-compact? namespace?)
                               (second (re-find #"/(.+)" pred)) ;; just capture everything after the '/'

                               :else
                               k)
                  spec       {:name             pred'
                              :wildcard?        (:wildcard? sub-select)
                              :id?              (:id? sub-select)
                              :namespace?       namespace?
                              :reverse?         (some? reversed)
                              :componentFollow? (get v' "_component")
                              :compact?         compact?    ;; remove namespace from result if same as _collection
                              :limit            (get v' "_limit" 100)
                              :offset           (get v' "_offset" 0)
                              :as               as
                              :recur            (get v' "_recur") ;; holds max depth of recursion
                              :recur-depth      0           ;; updated with recursion depth while processing
                              :recur-seen       #{}
                              :orderBy          (get v' "_orderBy")
                              :select           (:select sub-select)
                              :context          context}]
              (cond
                reversed
                (assoc-in acc [:select :reverse pred'] spec)

                namespace?
                (assoc-in acc [:select :pred-id pred'] spec)

                :else
                (assoc-in acc [:select :ns-lookup pred'] spec))))))
      {} select)))


(defn p->pred-config
  [db p context compact?]
  (let [name (dbproto/-p-prop db :name p)
        iri  (dbproto/-p-prop db :iri p)]
    {:p          p
     :limit      nil
     :name       name                                       ;; TODO: With :as being the primary name used, how is this used if at all any longer?
     :as         (cond
                   (and compact? name)                      ;; TODO - legacy, favor using a @context to shorten pred names, look to remove
                   (second (re-find #"/(.+)" name))

                   (= const/$iri p)
                   (or (some #(when (= "@id" (:id (val %)))
                                (key %)) context)
                       "@id")

                   (= const/$rdf:type p)
                   (or (some #(when (= "@type" (:id (val %)))
                                (key %)) context)
                       "@type")

                   iri
                   (if context
                     (json-ld/compact iri context)
                     iri)

                   :else
                   (or name (str p)))
     :multi?     (dbproto/-p-prop db :multi p)
     :component? (dbproto/-p-prop db :component p)
     :tag?       (= :tag (dbproto/-p-prop db :type p))
     :ref?       (dbproto/-p-prop db :ref? p)}))


(defn- build-predicate-map
  "For a flake selection, build out parts of the
  base set of predicates so we don't need to look them up
  each time... like multi, component, etc."
  [db pred-name context]
  (when-let [p (dbproto/-p-prop db :id pred-name)]
    (p->pred-config db p context false)))


(defn ns-lookup-pred-spec
  "Given an predicate spec produced by the parsed select statement,
  when an predicate does not have a namespace we will default it to
  utilize the namespace of the subject.

  This fills out the predicate spec that couldn't be done earlier because
  we did not know the collection."
  [db collection-id {:keys [context] :as ns-lookup-spec-map}]
  (let [collection-name (dbproto/-c-prop db :name collection-id)]
    (reduce-kv
      (fn [acc k v]
        (let [pred (str collection-name "/" k)]
          (if-let [p-map (or (build-predicate-map db pred context)
                             (build-predicate-map db k context))]
            (assoc acc (:p p-map) (merge p-map v))
            acc)))
      nil ns-lookup-spec-map)))


(defn- parse-db*
  [db sub-select context]
  (loop [[[k v] & r] sub-select
         acc {}]
    (if-not k
      acc
      ;; if pred-id or reverse, substitute predicate name keys with predicate id keys
      (let [v'   (if-not (#{:pred-id :reverse} k)
                   v
                   (loop [[[k* v*] & r*] v
                          acc* {}]
                     (if-not k*
                       acc*
                       (let [p-map (build-predicate-map db k* context)
                             acc** (if p-map
                                     (assoc acc* (:p p-map) (merge p-map v*))
                                     acc*)]
                         (recur r* acc**)))))
            v''  (loop [[[k* v*] & r*] v'
                        acc* {}]
                   (if-not k*
                     acc*
                     (let [acc** (if (:select v*)
                                   (assoc acc* k* (assoc v* :select (parse-db* db (:select v*) context)))
                                   (assoc acc* k* v*))]
                       (recur r* acc**))))
            acc' (assoc acc k v'')]
        (recur r acc')))))


;; TODO - look at caching below for speed... reset at any schema versioning
;; select statement caching, keeps cache for current schema version, then resets when schema is updated.
(def select-cache (atom {:version 0}))

(defn reset-select-cache!
  []
  (reset! select-cache {:version 0}))

(defn parse-db
  "Parses, but leverages a specific db to convert predicate
   names that it can into more complete select statement maps.

   Caches results based on database version."
  [db select context opts]
  (let [schema-version (schema-util/version db)
        cache-key      [schema-version select context (dissoc opts :fuel)]]
    ;; when schema is at a newer version, reset cache (version is 't' and negative, so decreases with newer)
    (when (< schema-version (:version @select-cache))
      (reset! select-cache {:version schema-version}))
    (or (get @select-cache cache-key)                       ;; :fuel is a volatile! and will be different every time, exclude from cache lookup
        (let [select-smt  (parse select context opts)
              select-smt* (assoc select-smt :select (parse-db* db (:select select-smt) context))]
          (swap! select-cache assoc cache-key select-smt*)
          select-smt*))))


(comment


  (-> @select-cache)
  (reset-select-cache!))

