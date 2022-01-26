(ns fluree.db.interface.schema
  (:require [fluree.db.query.schema :as schema-impl]
            [fluree.db.util.schema :as schema-util]))

(defn schema-map
  [db]
  (schema-impl/schema-map db))

(defn get-language-change
  "Returns the language being added, if any. Else returns nil."
  [flakes]
  (schema-util/get-language-change flakes))
