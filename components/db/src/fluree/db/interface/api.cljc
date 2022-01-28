(ns fluree.db.interface.api
  (:require [fluree.db.api :as fdb]))

(defn connect
  [ledger-servers opts]
  (fdb/connect ledger-servers opts))

(defn transact-async
  "Submits a transaction for a ledger and a transaction. Returns a core async channel
  that will eventually have either the result of the tx, the txid (if :txid-only option used), or
  an exception due to an invalid transaction or if the timeout occurs prior to a response.

  Will locally sign a final transaction command if a private key is provided via :private-key
  in the options, otherwise will submit the transaction to the connected ledger and request signature,
  provided the ledger group has a default private key available for signing.

  Options (opts) is a map with the following possible keys:
  - private-key - The private key to use for signing. If not present, a default
                  private key will attempt to be used from the connection, if available.
  - auth        - The auth id for the auth record being used. The private key must
                  correspond to this auth record, or an authority of this auth record.
  - expire      - When this transaction should expire if not yet attempted.
                  Defaults to 5 minutes.
  - nonce       - Any long/64-bit integer value that will make this transaction unique.
                  By default epoch milliseconds is used.
  - deps        - List of one or more txids that must be successfully processed before
                  this tx is processed. If any fail, this tx will fail. (not yet implemented)
  - txid-only   - Boolean (default of false). If true, will not wait for a response to the tx,
                  but instead return with the txid once it is successfully persisted by the
                  transactors. The txid can be used to look up/monitor the response at a later time.
  - timeout     - will respond with an exception if timeout reached before response available."
  ([conn ledger txn] (fdb/transact-async conn ledger txn))
  ([conn ledger txn opts] (fdb/transact-async conn ledger txn opts)))

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
