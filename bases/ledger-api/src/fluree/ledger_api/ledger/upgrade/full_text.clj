(ns fluree.ledger-api.ledger.upgrade.full-text
  (:require [clojure.core.async :as async :refer [<!!]]
            [clojure.tools.logging :as log]
            [environ.core :as environ]
            [fluree.db.interface.api :as fdb.api]
            [fluree.db.interface.constants :as fdb.const]
            [fluree.db.interface.dbproto :as fdb.dbproto]
            [fluree.db.interface.flake :as fdb.flake]
            [fluree.db.interface.query-range :as fdb.query-range]
            [fluree.ledger-api.server :as server]
            [fluree.ledger-api.ledger.txgroup.txgroup-proto :as txproto]))

(set! *warn-on-reflection* true)

(def pred-name "_predicate/fullText")
(def invalid-name "_predicate/fullText-invalid")

(defn wait-for-block
  [conn ledger expected-block]
  (<!! (fdb.api/db conn ledger {:syncTo expected-block})))

(defn mark-invalid-pred
  [{:keys [conn network dbid] :as db}]
  (let [ledger [network dbid]]
    (when-let [pred-id (fdb.dbproto/-p-prop db :id pred-name)]
      (when-not (= pred-id
                   fdb.const/$_predicate:fullText)
        (log/info "Marking fullText predicate with id" pred-id
                  "in ledger" ledger "invalid")
        (<!! (fdb.api/transact-async conn ledger
                                 [{:_id  pred-id,
                                   :name invalid-name
                                   :doc  "DEPRECATED"
                                   :_predicate/deprecated true}]))))))

(defn add-valid-pred
  [{:keys [conn network dbid] :as db}]
  (let [pred-id (fdb.dbproto/-p-prop db :id pred-name)
        ledger  [network dbid]]
    (if (nil? pred-id)
      (do (log/info "Adding valid fullText predicate to ledger" ledger)
          (<!! (fdb.api/transact-async conn ledger
                                   [{:_id  fdb.const/$_predicate:fullText
                                     :name pred-name
                                     :doc  "If true, full text search is enabled on this predicate."
                                     :type "boolean"}])))
      (log/error "fullText predicate with id" pred-id
                 "already exists in ledger" ledger))))

(defn transact-in-batches
  [conn ledger txns batch-size]
  (loop [batches   (partition-all batch-size txns)
            responses []]
    (if-let [batch (first batches)]
      (let [resp (<!! (fdb.api/transact-async conn ledger batch))]
        (recur (rest batches)
               (conj responses resp)))
      responses)))

(defn update-subjects
  [{:keys [conn network dbid] :as db}]
  (let [invalid-pred-id (fdb.dbproto/-p-prop db :id invalid-name)
        ledger          [network dbid]]
    (when invalid-pred-id
      (let [subject-flakes (<!! (fdb.query-range/index-range db :psot = [invalid-pred-id]))]
        (loop [flakes subject-flakes
               txns   []]
          (if-let [f (first flakes)]
            (let [txn {:_id         (fdb.flake/s f),
                       invalid-name nil,
                       pred-name    true}]
              (recur (rest flakes)
                     (conj txns txn)))
            (transact-in-batches conn ledger txns 50)))))))

(defn reset-index
  [{:keys [conn network dbid] :as db}]
  (let [indexer (-> conn :full-text/indexer :process)
        ledger  [network dbid]]
    (<!! (indexer {:action :reset, :db db}))))

(defn repair
  [conn ledger]
  (let [db            (<!! (fdb.api/db conn ledger))

        mark-result   (mark-invalid-pred db)
        marked-block  (:block mark-result)
        marked-db     (wait-for-block conn ledger marked-block)

        add-result    (add-valid-pred marked-db)
        added-block   (:block add-result)
        added-db      (wait-for-block conn ledger added-block)

        update-result (update-subjects added-db)
        updated-block (-> update-result last :block)
        updated-db    (wait-for-block conn ledger updated-block)]
    (log/info "Resetting full text index")
    (reset-index updated-db)))

(defn -main
  [& args]
  (let [[network ledger-id] (take 2 args)]
    (if (and network ledger-id)
      (do (log/info "Starting system")
          (let [{:keys [conn] :as system} (server/startup)]
            (.addShutdownHook (Runtime/getRuntime)
                              (Thread. ^Runnable
                                       (fn []
                                         (log/info "SHUTDOWN Start")
                                         (server/shutdown system)
                                         (log/info "SHUTDOWN Complete"))))
            (loop [count 0]
              (if (> count 5)
                (log/error "No shared private key configured")
                (when (nil? (txproto/get-shared-private-key (:group system) network ledger-id))
                  (Thread/sleep 500)
                  (recur (inc count)))))
            (log/info "Repairing full text index")
            (let [results (repair conn [network ledger-id])]
              (log/info "Full text repair completed:" results))))
      (log/error "You must supply the network and ledger-id"))))
