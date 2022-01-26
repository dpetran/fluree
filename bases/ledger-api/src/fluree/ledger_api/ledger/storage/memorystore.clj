(ns fluree.ledger-api.ledger.storage.memorystore
  (:require [fluree.db.interface.async :as fdb.async]))

(set! *warn-on-reflection* true)

(def memory-store (atom {}))

(defn connection-storage-read
  "Default function for connection storage."
  [key]
  (fdb.async/go-try (get @memory-store key)))


(defn connection-storage-write
  "Default function for connection storage writing."
  [key val]
  (fdb.async/go-try (if (nil? val)
            (swap! memory-store dissoc key)
            (swap! memory-store assoc key val))
          true))


(defn close
  "Resets memory store."
  []
  (reset! memory-store {})
  true)
