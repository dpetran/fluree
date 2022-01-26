(ns fluree.db.interface.event-bus
  (:require [fluree.db.event-bus :as event-bus-impl]))

(defn publish
  [event-type dbv data]
  (event-bus-impl/publish event-type dbv data))

(defn reset-sub
  []
  (event-bus-impl/reset-sub))

(defn subscribe-db
  [dbv c]
  (event-bus-impl/subscribe-db dbv c))

(defn unsubscribe-db
  [dbv c]
  (event-bus-impl/unsubscribe-db dbv c))
