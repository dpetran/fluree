(ns fluree.db.interface.query
  (:require [fluree.db.query.fql :as fql]))

(defn flakes->res
  [db cache fuel max-fuel base-select-spec flakes]
  (fql/flakes->res db cache fuel max-fuel base-select-spec flakes))
