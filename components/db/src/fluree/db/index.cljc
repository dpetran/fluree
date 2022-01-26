(ns fluree.db.index
  (:refer-clojure :exclude [resolve])
  (:require [fluree.db.flake :as flake]
            #?(:clj  [clojure.core.async :refer [go <!] :as async]
               :cljs [cljs.core.async :refer [go <!] :as async])
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.log :as log]))

#?(:clj (set! *warn-on-reflection* true))

(def default-comparators
  "Map of default index comparators for the five index types"
  {:spot flake/cmp-flakes-spot
   :psot flake/cmp-flakes-psot
   :post flake/cmp-flakes-post
   :opst flake/cmp-flakes-opst
   :tspo flake/cmp-flakes-block})

(def types
  "The five possible index orderings based on the subject, predicate, object,
  and transaction flake attributes"
  (-> default-comparators keys set))

(defn leaf?
  "Returns `true` if `node` is a map for a leaf node"
  [node]
  (-> node :leaf true?))

(defn branch?
  "Returns `true` if `node` is a map for branch node"
  [node]
  (-> node :leaf false?))

(defprotocol Resolver
  (resolve [r node]
    "Populate index branch and leaf node maps with either their child node
     attributes or the flakes the store, respectively."))

(defn resolved?
  "Returns `true` if the data associated with the index node map `node` is fully
  resolved from storage"
  [node]
  (cond
    (leaf? node)   (not (nil? (:flakes node)))
    (branch? node) (not (nil? (:children node)))))

(defn lookup
  [branch flake]
  (when (and (branch? branch)
             (resolved? branch))
    (let [{:keys [children]} branch]
      (-> children
          (flake/nearest <= flake)
          (or (first children))
          val))))

(defn lookup-leaf
  [r branch flake]
  (go-try
   (when (and (branch? branch)
              (resolved? branch))
     (loop [child (lookup branch flake)]
       (if (leaf? child)
         child
         (recur (<? (resolve r child))))))))


(defn empty-leaf
  "Returns a blank leaf node map for the provided `network`, `dbid`, and index
  comparator `cmp`."
  [network dbid cmp]
  {:comparator cmp
   :network network
   :dbid dbid
   :id :empty
   :leaf true
   :first flake/maximum
   :rhs nil
   :size 0
   :block 0
   :t 0
   :leftmost? true})

(defn child-entry
  [{:keys [first] :as node}]
  [first node])

(defn child-map
  "Returns avl sorted map whose keys are the first flakes of the index node
  sequence `child-nodes`, and whose values are the corresponding nodes from
  `child-nodes`."
  [cmp & child-nodes]
  (->> child-nodes
       (mapcat child-entry)
       (apply flake/sorted-map-by cmp)))

(defn empty-branch
  "Returns a blank branch node which contains a single empty leaf node for the
  provided `network`, `dbid`, and index comparator `cmp`."
  [network dbid cmp]
  (let [child-node (empty-leaf network dbid cmp)
        children   (child-map cmp child-node)]
    {:comparator cmp
     :network network
     :dbid dbid
     :id :empty
     :leaf false
     :first flake/maximum
     :rhs nil
     :children children
     :size 0
     :block 0
     :t 0
     :leftmost? true}))

(defn after-t?
  "Returns `true` if `flake` has a transaction value after the provided `t`"
  [t flake]
  (-> flake flake/t (< t)))

(defn filter-after
  "Returns a sequence containing only flakes from the flake set `flakes` with
  transaction values after the provided `t`."
  [t flakes]
  (filter (partial after-t? t) flakes))

(defn flakes-through
  "Returns an avl-subset of the avl-set `flakes` with transaction values on or
  before the provided `t`."
  [t flakes]
  (->> flakes
       (filter-after t)
       (flake/disj-all flakes)))

(defn stale-by
  "Returns a sequence of flakes from the sorted set `flakes` that are out of date
  by the transaction `t` because `flakes` contains another flake with the same
  subject and predicate and a transaction value later than that flake but on or
  before `t`."
  [t flakes]
  (->> flakes
       (flakes-through t)
       (group-by (juxt flake/s flake/p flake/o))
       vals
       (mapcat (fn [flakes]
                 (let [sorted-flakes (sort-by flake/t flakes)
                       last-flake    (first sorted-flakes)]
                   (if (flake/op last-flake)
                     (rest sorted-flakes)
                     sorted-flakes))))))

(defn t-range
  "Returns a sorted set of flakes that are not out of date between the
  transactions `from-t` and `to-t`."
  [from-t to-t flakes]
  (let [stale-flakes (stale-by from-t flakes)
        subsequent   (filter-after to-t flakes)
        out-of-range (concat stale-flakes subsequent)]
    (flake/disj-all flakes out-of-range)))

(defn novelty-subrange
  [{:keys [rhs leftmost?], first-flake :first, :as node} through-t novelty]
  (let [subrange (cond
                   ;; standard case.. both left and right boundaries
                   (and rhs (not leftmost?))
                   (flake/subrange novelty > first-flake <= rhs)

                   ;; right only boundary
                   (and rhs leftmost?)
                   (flake/subrange novelty <= rhs)

                   ;; left only boundary
                   (and (nil? rhs) (not leftmost?))
                   (flake/subrange novelty > first-flake)

                   ;; no boundary
                   (and (nil? rhs) leftmost?)
                   novelty)]
    (flakes-through through-t subrange)))

(defn add-flakes
  [leaf flakes]
  (-> leaf
      (update :flakes flake/conj-all flakes)
      (update :size (fn [size]
                      (->> flakes
                           (map flake/size-flake)
                           (reduce + size))))))

(defn rem-flakes
  [leaf flakes]
  (-> leaf
      (update :flakes flake/disj-all flakes)
      (update :size (fn [size]
                      (->> flakes
                           (map flake/size-flake)
                           (reduce - size))))))

(defn at-t
  "Find the value of `leaf` at transaction `t` by adding new flakes from
  `idx-novelty` to `leaf` if `t` is newer than `leaf`, or removing flakes later
  than `t` from `leaf` if `t` is older than `leaf`."
  [{:keys [rhs leftmost? flakes], leaf-t :t, :as leaf} t idx-novelty]
  (if (= leaf-t t)
    leaf
    (cond-> leaf
      (> leaf-t t)
      (add-flakes (novelty-subrange leaf t idx-novelty))

      (< leaf-t t)
      (rem-flakes (filter-after t flakes))

      true
      (assoc :t t))))
