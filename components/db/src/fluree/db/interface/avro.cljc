(ns fluree.db.interface.avro
  (:require [fluree.db.serde.avro :as avro]))

(defn serialize-block
  [block-data]
  (avro/serialize-block block-data))

(def map->Serializer avro/map->Serializer)
