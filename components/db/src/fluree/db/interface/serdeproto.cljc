(ns fluree.db.interface.serdeproto
  (:require [fluree.db.serde.protocol :as serdeproto]))

#_(def StorageSerializer serdeproto/StorageSerializer)

(defprotocol StorageSerializer
  (-serialize-block [this block] "Serializes block")
  (-deserialize-block [this block] "Deserializes block")
  (-serialize-db-root [this db-root] "Serializes the database index root.")
  (-deserialize-db-root [this db-root] "Deserializes the database index root.")
  (-serialize-branch [this branch] "Serializes a branch.")
  (-deserialize-branch [this branch] "Deserializes a branch.")
  (-serialize-leaf [this leaf] "Serializes a leaf.")
  (-deserialize-leaf [this leaf] "Deserializes a leaf.")
  (-serialize-garbage [this garbage] "Serializes database garbage for later cleanup.")
  (-deserialize-garbage [this garbage] "Deserializes database garbage.")
  (-serialize-db-pointer [this pointer] "Serializes a database pointer")
  (-deserialize-db-pointer [this pointer] "Deserializes a database pointer"))
