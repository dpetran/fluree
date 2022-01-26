(ns fluree.ledger-api.ledger.upgrade
  (:require [clojure.tools.logging :as log]
            [clojure.string :as str]
            [fluree.db.interface.api :as fdb.api]
            [fluree.db.interface.async :as fdb.async]
            [fluree.db.interface.constants :as fdb.const]
            [fluree.db.interface.flake :as fdb.flake]
            [fluree.db.interface.index :as fdb.index]
            [fluree.db.interface.query-range :as fdb.query-range]
            [fluree.db.interface.storage :as fdb.storage]
            [fluree.db.interface.time-travel :as fdb.time-travel]
            [fluree.ledger-api.ledger.txgroup.txgroup-proto :as txproto]))

(set! *warn-on-reflection* true)


(defn v1->v2
  "Modifies index segments where the 'rhs' does not match the next segment's first-flake"
  [conn]
  (fdb.async/go-try
    (let [dbs        (txproto/ledger-list (:group conn))    ;; two-tuples of [network dbid]
          left-flake (fdb.flake/->Flake (Long/MAX_VALUE) 0 (Long/MAX_VALUE) 0 true nil)]
      (when (not-empty dbs)
        (log/info "Migrating data version from v1 to v2")
        ;; check every db
        (doseq [[network dbid] dbs]
          (let [db-ident   (str network "/" dbid)
                idx-points (-> @(fdb.api/ledger-info conn db-ident)
                               :indexes
                               keys
                               sort)]
            (log/info (str "Updating indexes " idx-points " for db: " db-ident))
            (doseq [idx-point idx-points]
              (log/info (str " - Updating index " idx-point " for db: " db-ident))
              (let [db-root (fdb.async/<? (fdb.storage/read-db-root conn network dbid idx-point))]
                (doseq [idx-type fdb.index/types]
                  (let [root-idx-key (-> db-root (get idx-type) :id)
                        branch-data  (fdb.async/<? (fdb.storage/read-branch conn root-idx-key))
                        new-children (loop [[child & r] (:children branch-data)
                                            i        0
                                            last-rhs nil
                                            acc      []]
                                       (if-not child
                                         acc
                                         (let [new-child (cond
                                                           ;; first segment, place in update farthest 'left-flake'
                                                           (and (zero? i) (not= left-flake (:first child)))
                                                           (do
                                                             (log/info "   -> Updating index segment: " (:id child) "(left flake)")
                                                             (assoc child :first left-flake))

                                                           ;; need to update child, out of sync
                                                           (and last-rhs (not= last-rhs (:first child)))
                                                           (do
                                                             (log/info "   -> Updating index segment: " (:id child))
                                                             (assoc child :first last-rhs))

                                                           ;; no change
                                                           :else
                                                           child)]
                                           (recur r (inc i) (:rhs child) (conj acc new-child)))))
                        branch-data* (assoc branch-data :children new-children)]
                    (fdb.async/<? (fdb.storage/write-branch-data conn root-idx-key branch-data*))))))))
        (log/info "Migration complete."))
      (txproto/set-data-version (:group conn) 2))))

(defn rename-nw-or-db
  "Ensures that a name conforms to [a-z0-9-]. Lowercases names, converts _ to -, removes all other special chars"
  [name]
  (-> name
      str/lower-case
      (str/replace #"_" "-")
      (str/replace #"[^a-z0-9-]" "")))


(defn update-dbid-state-atom-networks
  [state-atom old-network old-db new-network new-db]
  (let [old-value (get-in @state-atom [:networks old-network :dbs old-db])
        ;; dissoc old value
        _         (swap! state-atom update-in [:networks old-network :dbs] dissoc old-db)
        ;; assoc new value
        _         (swap! state-atom assoc-in [:networks new-network :dbs new-db] old-value)
        ;; if old network has no dbs left, delete
        _         (when (= {:dbs {}} (get-in @state-atom [:networks old-network]))
                    (swap! state-atom update-in [:networks] dissoc old-network))]
    true))


(defn v2->v3
  "Add _shard collection, ensure db names conform to new standard"
  [conn]
  (fdb.async/go-try
    (let [ledger-list @(fdb.api/ledger-list conn)
          update-txn  [{:_id  "_predicate"
                        :name "_auth/salt"
                        :doc  "Salt used for auth record, if the auth type requires it."
                        :type "bytes"}
                       {:_id                "_predicate"
                        :name               "_auth/type"
                        :doc                "Tag to identify underlying auth record type, if necessary."
                        :type               "tag"
                        :restrictCollection "_auth"
                        :restrictTag        true}
                       {:_id     "_collection"
                        :name    "_shard"
                        :doc     "Shard settings."
                        :version "1"}
                       {:_id                "_predicate"
                        :name               "_collection/shard"
                        :doc                "The shard that this collection is assigned to. If none assigned, defaults to 'default' shard."
                        :type               "ref"
                        :restrictCollection "_shard"}
                       {:_id    "_predicate"
                        :name   "_shard/name"
                        :doc    "Name of this shard"
                        :type   "string"
                        :unique true}
                       {:_id                "_predicate"
                        :name               "_shard/miners"
                        :doc                "Miners (auth records) assigned to this shard"
                        :type               "ref"
                        :restrictCollection "_auth"
                        :multi              true}
                       {:_id  "_predicate"
                        :name "_shard/mutable"
                        :doc  "Whether this shard is mutable. If not specified, defaults to 'false', meaning the data is immutable."
                        :type "boolean"}
                       {:_id    "_predicate"
                        :name   "_setting/id"
                        :doc    "Unique setting id."
                        :type   "string"
                        :unique true}]]
      (when (not-empty ledger-list)
        (log/info "Migrating data version from v2 to v3")
        (loop [[ledger & r] ledger-list]
          (when ledger
            (let [[network dbid] ledger
                  db-ident    (str network "/" dbid)
                  db-1        (-> (fdb.api/db conn db-ident)
                                  fdb.async/<?
                                  (fdb.time-travel/as-of-block 1)
                                  fdb.async/<?)
                  setting-res (fdb.async/<? (fdb.query-range/collection db-1 "_setting"))
                  setting-id  (:s (first setting-res))
                  setting-txn [{:_id setting-id
                                :id  "root"}]]
              (fdb.async/<? (fdb.api/transact-async conn db-ident update-txn))
              (fdb.async/<? (fdb.api/transact-async conn db-ident setting-txn))
              (recur r))))
        (txproto/lowercase-all-names (:group conn))
        (log/info "Migration complete."))
      (txproto/set-data-version (:group conn) 3))))


(defn v3->v4
  "Connect just add _tx/hash, as it needs to be subject _id 99."
  []
  (fdb.async/go-try
    (throw (ex-info "Cannot update ledger from version 3 to version 4. No forwards
    compatible."
                    {:status 400
                     :error  :db/invalid-request}))))

;; TODO - Refactor this function
(defn upgrade
  "Synchronous"
  [conn from-v to-v]
  (let [from-v (or from-v 1)
        to-v   (or to-v fdb.const/data_version)]                ;; v0-9-5-PREVIEW2 was first version marker we used - default
    (cond
      (= from-v to-v)
      true                                                  ;; no upgrade

      (= [1 2] [from-v to-v])
      (fdb.async/<?? (v1->v2 conn))

      (= [1 3] [from-v to-v])
      (do (fdb.async/<?? (v1->v2 conn))
          (fdb.async/<?? (v2->v3 conn)))

      (= [1 4] [from-v to-v])
      (do (fdb.async/<?? (v1->v2 conn))
          (fdb.async/<?? (v2->v3 conn))
          (fdb.async/<?? (v3->v4)))

      (= [2 3] [from-v to-v])
      (fdb.async/<?? (v2->v3 conn))

      (= [2 4] [from-v to-v])
      (do (fdb.async/<?? (v2->v3 conn))
          (fdb.async/<?? (v3->v4)))

      (= [3 4] [from-v to-v])
      (fdb.async/<?? (v3->v4)))))
