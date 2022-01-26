(ns fluree.db.interface.spec
  (:require [fluree.db.spec :as spec-impl]
            [fluree.db.graphdb :as graphdb]))

(defn type-check
  [x p-type]
  (spec-impl/type-check x p-type))

(defn validate-ledger-ident
  [ledger]
  (graphdb/validate-ledger-ident ledger))
