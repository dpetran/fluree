(ns fluree.ledger-api.ledger.memorydb
  (:require [clojure.core.async :as async]
            [fluree.ledger-api.ledger.bootstrap :as bootstrap]
            [fluree.db.interface.async :as fdb.async]
            [fluree.db.interface.dbproto :as fdb.dbproto]
            [fluree.db.interface.flake :as fdb.flake]
            [fluree.db.interface.schema :as fdb.schema]
            [fluree.db.interface.index :as fdb.index]))

(set! *warn-on-reflection* true)

;; One-off in-memory dbs, eventually move to fluree/db repository so local in-memory dbs can be launched
;; inside application servers, web browsers, ?? - to maintain local state but have all of the other benefits

;; Put here mostly for quickly testing, it will make sense to move most tests to utilize this format.

;; For now, requires bootstrap and transact namespaces, which are only in fluree/ledger

(defrecord FakeConnection [transactor?]
  fdb.index/Resolver
  (resolve [this node]
    (let [out (async/chan)]
      (async/put! out node)
      out)))

(defn fake-conn
  "Returns a fake connection object that is suitable for use with the memorydb if
  no other conn is available."
  []
  (map->FakeConnection {:transactor? false}))

(defn new-db
  "Creates a local, in-memory but bootstrapped db (primarily for testing)."
  ([conn ledger] (new-db conn ledger nil))
  ([conn ledger bootstrap-opts]
   (let [pc (async/promise-chan)]
     (async/go
       (let [block-data   (bootstrap/bootstrap-memory-db conn ledger bootstrap-opts)
             db-no-schema (:db block-data)
             schema       (fdb.async/<? (fdb.schema/schema-map db-no-schema))]
         (async/put! pc (assoc db-no-schema :schema schema))))
     pc)))


(defn transact-flakes
  "Transacts a series of preformatted flakes into the in-memory db."
  [db flakes]
  (let [block (inc (:block db))]
    (fdb.dbproto/-with (async/<!! db) block flakes)))


(defn transact-tuples
  "Transacts tuples which includes s, p, o and optionally op.
  If op is not explicitly false, it is assumed to be true.

  Does zero validation that tuples are accurate"
  [db tuples]
  (let [db*    (if (fdb.async/channel? db)
                 (async/<!! db)
                 db)
        t      (dec (:t db*))
        flakes (->> tuples
                    (map (fn [[s p o op]]
                           (fdb.flake/->Flake s p o t (if (false? op) false true) nil))))]
    (transact-flakes db* flakes)))
