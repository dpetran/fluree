(ns fluree.ledger-api.ledger.garbage-collect
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [fluree.db.interface.storage :as fdb.storage]
            [fluree.db.interface.async :as fdb.async]
            [fluree.ledger-api.ledger.txgroup.txgroup-proto :as txproto]))

(set! *warn-on-reflection* true)

;; takes care of garbage collection for a db.

(defn delete-file-raft
  "Deletes a file from the RAFT network based on the file key."
  [conn key]
  (fdb.async/go-try
    (let [group (:group conn)]
      (fdb.async/<? (txproto/storage-write-async group key nil)))))


(defn process-index
  "Garbage collections a specific index point."
  [conn network dbid idx-point]
  (fdb.async/go-try
    (let [group        (:group conn)
          garbage-keys (:garbage (fdb.async/<? (fdb.storage/read-garbage conn network dbid idx-point)))]
      (log/info "Garbage collecting index point " idx-point " for ledger " network "/" dbid ".")
      ;; delete index point first so it won't show up in dbinfo
      (txproto/remove-index-point group network dbid idx-point)
      ;; remove db-root
      (fdb.async/<? (delete-file-raft conn (fdb.storage/ledger-root-key network dbid idx-point)))
      ;; remove all index segments that were garbage collected
      (doseq [k garbage-keys]
        (fdb.async/<? (delete-file-raft conn k)))
      ;; remove garbage file
      (fdb.async/<? (delete-file-raft conn (fdb.storage/ledger-garbage-key network dbid idx-point)))
      (log/info "Finished garbage collecting index point " idx-point " for ledger " network "/" dbid "."))))


(defn process
  "Collects garbage (deletes indexes) for any index point(s) between from-block and to-block.
  If from and to blocks are not specified, collects all index points except for current one."
  ([conn network dbid]
   (let [dbinfo     (txproto/ledger-info (:group conn) network dbid)
         idx-points (-> (into #{} (keys (:indexes dbinfo)))
                        ;; remove current index point
                        (disj (:index dbinfo)))]
     (if (empty? idx-points)
       false                                                ;; nothing to garbage collect
       (process conn network dbid (apply min idx-points) (apply max idx-points)))))
  ([conn network dbid from-block to-block]
   (fdb.async/go-try
     (let [[from-block to-block] (if (> from-block to-block) ;; make sure from-block is smallest number
                                   [to-block from-block]
                                   [from-block to-block])
           dbinfo                (txproto/ledger-info (:group conn) network dbid)
           index-points          (-> (into #{} (keys (:indexes dbinfo)))
                                     (disj (:index dbinfo))) ;; remove current index point
           filtered-index-points (->> index-points
                                      (filter #(<= from-block % to-block))
                                      ;; do smallest indexes first
                                      (sort))]
       (log/info "Garbage collecting ledger " network "/" dbid " for index points: " filtered-index-points)
       (doseq [idx-point filtered-index-points]
         (fdb.async/<? (process-index conn network dbid idx-point)))
       (log/info "Done garbage collecting ledger " network "/" dbid " for index points: " filtered-index-points)
       true))))
