(ns fluree.ledger-api.ledger.delete
  (:require [fluree.db.interface.async :as fdb.async]
            [fluree.db.interface.index :as fdb.index]
            [fluree.db.interface.session :as fdb.session]
            [fluree.db.interface.storage :as fdb.storage]
            [fluree.ledger-api.ledger.garbage-collect :as gc]
            [fluree.ledger-api.ledger.txgroup.txgroup-proto :as txproto]))

(set! *warn-on-reflection* true)

;; for deleting a current db

(defn delete-all-index-children
  "From any branch index, delete all children.
   If children are branches, recursively deletes them."
  [conn idx-branch]
  (fdb.async/go-try
    (let [idx      (fdb.async/<? (fdb.index/resolve conn idx-branch))
          children (vals (:children idx))
          leaf?    (:leaf (first children))]
      (doseq [child children]
        (if leaf?
          (do
            ;; delete leaf
            (fdb.async/<? (gc/delete-file-raft conn (:id child))))
          (fdb.async/<? (delete-all-index-children conn child))))
      ;; now delete the main branch called once children are all gone
      (fdb.async/<? (gc/delete-file-raft conn (:id idx-branch))))))


(defn delete-db-indexes
  "Deletes all keys for all four indexes for a db."
  [conn network dbid idx-point]
  (fdb.async/go-try
    (let [session  (fdb.session/session conn (str network "/" dbid))
          blank-db (:blank-db session)
          db       (fdb.async/<? (fdb.storage/reify-db conn network dbid blank-db idx-point))]
      (doseq [idx fdb.index/types]
        (fdb.async/<? (delete-all-index-children conn (get db idx)))))))

(defn all-versions
  [conn storage-block-key]
  (fdb.async/go-try
    (loop [n        1
           versions []]
      (let [version-key (str storage-block-key "--v" n)]
        (if (fdb.async/<? (fdb.storage/exists? conn version-key))
          (recur (inc n) (conj versions version-key))
          versions)))))

(defn delete-all-blocks
  "Deletes blocks and versions of blocks."
  [conn network dbid block]
  (fdb.async/go-try
    (doseq [block (range 1 (inc block))]
      (let [block-key (fdb.storage/ledger-block-file-path network dbid block)
            versions  (fdb.async/<? (all-versions conn block-key))
            to-delete (conj versions block-key)]
        (doseq [file to-delete]
          (fdb.async/<? (gc/delete-file-raft conn file)))))))

(defn delete-lucene-indexes
  "Deletes the full-text (lucene) indexes for a ledger."
  [conn network dbid]
  (fdb.async/go-try
    (when-let [indexer (-> conn :full-text/indexer :process)]
      (let [db (fdb.async/<? (fdb.session/db conn (str network "/" dbid) nil))]
        (fdb.async/<? (indexer {:action :forget, :db db}))))))

(defn process
  "Deletes a current DB, deletes block files."
  [conn network dbid]
  (fdb.async/go-try
    ;; mark status as deleting, so nothing new will get a handle on this db
    (txproto/update-ledger-status (:group conn) network dbid "deleting")
    (let [group     (:group conn)
          dbinfo    (txproto/ledger-info group network dbid)
          block     (:block dbinfo)
          idx-point (:index dbinfo)]

      ;; do a full garbage collection first. If nothing exists to gc, will throw
      (gc/process conn network dbid)

      ;; delete full-text indexes
      (fdb.async/<? (delete-lucene-indexes conn network dbid))

      ;; need to delete all index segments for the current index.
      (fdb.async/<? (delete-db-indexes conn network dbid idx-point))

      ;; need to explicitly do a garbage collection of the 'current' node
      (fdb.async/<? (gc/process-index conn network dbid idx-point))

      ;; delete all blocks
      (fdb.async/<? (delete-all-blocks conn network dbid block))

      ;;; remove current index from raft db status
      (txproto/remove-current-index group network dbid)

      ;; mark status as archived
      (txproto/remove-ledger group network dbid)

      ;; all done!
      true)))
