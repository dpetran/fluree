(ns fluree.db.interface.connection
  (:require [fluree.db.connection :as connection]
            [fluree.db.conn-events :as conn-events]))

(defn connect
  [servers conn-opts]
  (connection/connect servers conn-opts))

(defn process-events
  [conn msg]
  (conn-events/process-events conn msg))
