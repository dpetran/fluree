(ns fluree.db.interface.storage
  (:refer-clojure :exclude [read])
  (:require [fluree.db.storage.core :as storage]))

(defn ledger-block-key
  [network dbid block]
  (storage/ledger-block-key network dbid block))

(defn ledger-block-file-path
  [network ledger-id block]
  (storage/ledger-block-file-path network ledger-id block))

(defn ledger-garbage-key
  [network ledger-key block]
  (storage/ledger-garbage-key network ledger-key block))

(defn ledger-root-key
  [network ledger-id block]
  (storage/ledger-root-key network ledger-id block))

(defn read-branch
  [conn key]
  (storage/read-branch conn key))

(defn read-garbage
  [conn network dbid block]
  (storage/read-garbage conn network dbid block))

(defn read-db-root
  [conn network dbid block]
  (storage/read-db-root conn network dbid block))

(defn read-block
  [conn network ledger-id block]
  (storage/read-block conn network ledger-id block))

(defn read-block-version
  [conn network ledger-id block version]
  (storage/read-block-version conn network ledger-id block version))

(defn read
  [s k]
  (storage/read s k))

(defn write-leaf
  [conn network dbid idx-type leaf]
  (storage/write-leaf conn network dbid idx-type leaf))

(defn write-branch
  [conn network dbid idx-type branch]
  (storage/write-branch conn network dbid idx-type branch))

(defn write-branch-data
  [conn key data]
  (storage/write-branch-data conn key data))

(defn write-block
  [conn network dbid idx-type block-data]
  (storage/write-block conn network dbid block-data))

(defn write-block-version
  [conn network dbid idx-type block-data version]
  (storage/write-block-version conn network dbid block-data version))

(defn write-db-root
  ([db]
   (storage/write-db-root db))
  ([db custom-ecount]
   (storage/write-db-root db custom-ecount)))

(defn write-garbage
  [db garbage]
  (storage/write-garbage db garbage))

(defn exists?
  [s k]
  (storage/exists? s k))

(defn rename
  [s old-key new-key]
  (storage/rename s old-key new-key))

(defn reify-db
  [conn network dbid blank-db index]
  (storage/reify-db conn network dbid blank-db index))
