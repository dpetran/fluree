(ns fluree.db.interface.api
  (:require [fluree.db.api :as fdb]))

(defn transact-async
  [conn dbid txn]
  (fdb/transact-async conn dbid txn))

(defn db
  [conn dbid]
  (fdb/db conn dbid))

(defn query-async
  [db q]
  (fdb/query-async db q))

(defn multi-query-async
  [sources multi-query-map]
  (fdb/multi-query-async sources multi-query-map))

(defn block-query-async
  [conn ledger query-map]
  (fdb/block-query-async conn ledger query-map))

(defn block-range-with-txn-async
  [conn ledger block-map]
  (fdb/block-range-with-txn-async conn ledger block-map))

(defn history-query-async
  [sources query-map]
  (fdb/history-query-async sources query-map))

(defn graphql-async
  [conn db-name query-map]
  (fdb/graphql-async conn db-name query-map))

(defn sparql-async
  ([db sparql-str]
   (fdb/sparql-async db sparql-str))
  ([db sparql-str opts]
   (fdb/sparql-async db sparql-str opts)))

(defn sql-async
  ([db sql-str]
   (fdb/sql-async db sql-str))
  ([db sql-str opts]
   (fdb/sql-async db sql-str opts)))

(defn query-with-async
  [sources param]
  (fdb/query-with-async sources param))

(defn resolve-block-range
  [db query-map]
  (fdb/resolve-block-range db query-map))

(defn get-history-pattern
  [history]
  (fdb/get-history-pattern history))

(defn block-range
  ([db start]
   (fdb/block-range db start))
  ([db start end]
   (fdb/block-range db start end))
  ([db start end opts]
   (fdb/block-range db start end opts)))

(defn new-ledger
  ([conn ledger]
   (fdb/new-ledger conn ledger))
  ([conn ledger opts]
   (fdb/new-ledger conn ledger opts)))

(defn ledger-info
  [conn ledger]
  (fdb/ledger-info conn ledger))

(defn ledger-info-async
  [conn ledger]
  (fdb/ledger-info-async conn ledger))

(defn ledger-list
  [conn]
  (fdb/ledger-list conn))

(defn subid-async
  [db ident]
  (fdb/subid-async db ident))

(defn submit-command-async
  [conn command]
  (fdb/submit-command-async conn command))

(defn monitor-tx-async
  [conn ledger tid timeout-ms]
  (fdb/monitor-tx-async conn ledger tid timeout-ms))

(defn tx->command
  ([ledger txn private-key]
   (fdb/tx->command ledger txn private-key))
  ([ledger txn private-key opts]
   (fdb/tx->command ledger txn private-key opts)))
