(ns fluree.db.interface.session
  (:require [fluree.db.session :as session]))

(defn indexing-promise-ch
  [session]
  (session/indexing-promise-ch session))

(defn acquire-indexing-lock!
  [session pc]
  (session/acquire-indexing-lock! session pc))

(defn release-indexing-lock!
  [session block]
  (session/release-indexing-lock! session block))

(defn clear-db!
  [session]
  (session/clear-db! session))

(defn reload-db!
  [session]
  (session/reload-db! session))

(defn current-db
  [session]
  (session/current-db session))

(defn resolve-ledger
  [conn ledger]
  (session/resolve-ledger conn ledger))

(defn session
  ([conn ledger]
   (session/session conn ledger))
  ([conn ledger opts]
   (session/session conn ledger opts)))

(defn db
  [conn ledger opts]
  (session/db conn ledger opts))

(defn blank-db
  [conn ledger]
  (session/blank-db conn ledger))

(defn close
  ([session]
   (session/close session))
  ([network dbid]
   (session/close network dbid)))
