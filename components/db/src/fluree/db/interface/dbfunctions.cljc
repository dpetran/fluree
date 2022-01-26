(ns fluree.db.interface.dbfunctions
  (:require [fluree.db.dbfunctions.core :as dbfun-impl]))

(defn tx-fn?
  [value]
  (dbfun-impl/tx-fn? value))

(defn execute-tx-fn
  [db auth-id credits s p o fuel block-instant]
  (dbfun-impl/execute-tx-fn db auth-id credits s p o fuel block-instant))

(defn combine-fns
  [fn-str-coll]
  (dbfun-impl/combine-fns fn-str-coll))

(defn parse-fn
  ([db fn-str type]
   (dbfun-impl/parse-fn db fn-str type))
  ([db fn-str type params]
   (dbfun-impl/parse-fn db fn-str type params)))
