(ns fluree.db.interface.transact
  (:require [fluree.db.util.tx :as tx-impl]))

(defn create-new-db-tx
  [tx-map]
  (tx-impl/create-new-db-tx tx-map))

(defn gen-tx-hash
  ([tx-flakes]
   (tx-impl/gen-tx-hash tx-flakes))
  ([tx-flakes sorted?]
   (tx-impl/gen-tx-hash tx-flakes sorted?)))

(defn make-candidate-db
  [db]
  (tx-impl/make-candidate-db db))

(defn validate-command
  [cmd-data]
  (tx-impl/validate-command cmd-data))

(defn generate-merkle-root
  [& hashes]
  (apply tx-impl/generate-merkle-root hashes))
