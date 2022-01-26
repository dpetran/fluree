(ns fluree.db.interface.flake
  (:refer-clojure :exclude [sorted-set-by])
  (:require [fluree.db.flake :as flake-impl]))

(defn s
  [f]
  (flake-impl/s f))

(defn p
  [f]
  (flake-impl/p f))

(defn o
  [f]
  (flake-impl/o f))

(defn t
  [f]
  (flake-impl/t f))

(defn op
  [f]
  (flake-impl/op f))

(defn m
  [f]
  (flake-impl/m f))

(defn sid->cid
  [sid]
  (flake-impl/sid->cid sid))

(defn ->sid
  [cid n]
  (flake-impl/->sid cid n))

(defn sid->i
  [sid]
  (flake-impl/sid->i sid))

(defn ->Flake
  [s p o t op m]
  (flake-impl/->Flake s p o t op m))

(defn new-flake
  [& parts]
  (apply flake-impl/new-flake parts))

(defn parts->Flake
  ([[s p o t op m :as parts]]
   (flake-impl/parts->Flake  parts))
  ([[s p o t op m :as parts] default-tx]
   (flake-impl/parts->Flake parts default-tx))
  ([[s p o t op m :as parts] default-tx default-op]
   (flake-impl/parts->Flake parts default-tx default-op)))

(defn sorted-set-by
  [comparator & flakes]
  (apply flake-impl/sorted-set-by comparator flakes))

(defn cmp-flakes-block
  [f1 f2]
  (flake-impl/cmp-flakes-block f1 f2))

(defn cmp-flakes-spot
  [f1 f2]
  (flake-impl/cmp-flakes-spot f1 f2))

(defn disj-all
  [ss to-remove]
  (flake-impl/disj-all ss to-remove))

(defn subrange
  ([ss test flake]
   (flake-impl/subrange ss test flake))
  ([ss test start-flake end-test end-flake]
   (flake-impl/subrange ss test start-flake end-test end-flake)))

(defn size-flake
  [f]
  (flake-impl/size-flake f))

(defn size-bytes
  [flakes]
  (flake-impl/size-bytes flakes))

(defn flip-flake
  ([flake]
   (flake-impl/flip-flake flake))
  ([flake tx]
   (flake-impl/flip-flake flake tx)))

(defn min-subject-id
  [cid]
  (flake-impl/min-subject-id cid))

(defn max-subject-id
  [cid]
  (flake-impl/max-subject-id cid))
