(ns fluree.db.interface.index
  (:refer-clojure :exclude [resolve])
  (:require [fluree.db.index :as index-impl]))

(def Resolver index-impl/Resolver)

(def types index-impl/types)

(defn leaf?
  [node]
  (index-impl/leaf? node))

(defn branch?
  [node]
  (index-impl/branch? node))

(defn at-t
  [leaf t idx-novelty]
  (index-impl/at-t leaf t idx-novelty))

(defn novelty-subrange
  [node through-t novelty]
  (index-impl/novelty-subrange node through-t novelty))

(defn resolve
  [r node]
  (index-impl/resolve r node))

(defn resolved?
  [node]
  (index-impl/resolved? node))

(defn child-map
  [cmp & child-nodes]
  (apply index-impl/child-map cmp child-nodes))
