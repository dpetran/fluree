(ns fluree.ledger-api.ledger.transact.tx-meta
  (:require [fluree.db.interface.async :as fdb.async]
            [fluree.db.interface.constants :as fdb.const]
            [fluree.db.interface.dbproto :as fdb.dbproto]
            [fluree.db.interface.flake :as fdb.flake]
            [fluree.db.interface.transact :as fdb.tx]))

(set! *warn-on-reflection* true)

;; to handle transaction-related flakes

(def ^:const system-predicates
  "List of _tx predicates that only Fluree can assign, any user attempt to modify these should throw."
  #{fdb.const/$_tx:id fdb.const/$_tx:tx fdb.const/$_tx:sig fdb.const/$_tx:hash
    fdb.const/$_tx:auth fdb.const/$_tx:authority fdb.const/$_tx:nonce
    fdb.const/$_tx:error fdb.const/$_tx:tempids
    fdb.const/$_block:number fdb.const/$_block:instant
    fdb.const/$_block:hash fdb.const/$_block:prevHash
    fdb.const/$_block:transactions fdb.const/$_block:ledgers
    fdb.const/$_block:sigs})


(defn tx-meta-flakes
  ([tx-state] (tx-meta-flakes tx-state nil))
  ([{:keys [auth authority txid tx-string signature nonce t]} error-str]
   (let [tx-flakes [(fdb.flake/->Flake t fdb.const/$_tx:id txid t true nil)
                    (fdb.flake/->Flake t fdb.const/$_tx:tx tx-string t true nil)
                    (fdb.flake/->Flake t fdb.const/$_tx:sig signature t true nil)]]
     (cond-> tx-flakes
             auth (conj (fdb.flake/->Flake t fdb.const/$_tx:auth auth t true nil)) ;; note an error transaction may not have a valid auth
             authority (conj (fdb.flake/->Flake t fdb.const/$_tx:authority authority t true nil))
             nonce (conj (fdb.flake/->Flake t fdb.const/$_tx:nonce nonce t true nil))
             error-str (conj (fdb.flake/->Flake t fdb.const/$_tx:error error-str t true nil))))))


(defn generate-hash-flake
  "Generates transaction hash, and returns the hash flake.
  Flakes must already be sorted in proper block order."
  [flakes {:keys [t]}]
  (let [tx-hash (fdb.tx/gen-tx-hash flakes true)]
    (fdb.flake/->Flake t fdb.const/$_tx:hash tx-hash t true nil)))


(defn add-tx-hash-flake
  "Adds tx-hash flake to db by adding directly into novelty.
  This assumes the tx-hash is not indexed - if that is modified it could create an issue
  but only within a block transaction - between blocks we get a full new db from raft state and
  drop the db-after we create inside a transaction."
  [db tx-hash-flake]
  (let [flake-bytes (fdb.flake/size-flake tx-hash-flake)]
    (-> db
        (update-in [:novelty :spot] conj tx-hash-flake)
        (update-in [:novelty :psot] conj tx-hash-flake)
        (update-in [:novelty :psot] conj tx-hash-flake)
        (update-in [:stats :size] + flake-bytes)
        (update-in [:stats :flakes] inc))))


(defn generate-tx-error-flakes
  "If an error occurs, returns a set of flakes for the 't' that represents error."
  [db t tx-map command error-str]
  (fdb.async/go-try
    (let [db                (fdb.dbproto/-rootdb db)
          {:keys [auth authority nonce txid]} tx-map
          tx-state          {:txid      txid
                             :auth      (fdb.async/<? (fdb.dbproto/-subid db ["_auth/id" auth] false))
                             :authority (when authority (fdb.async/<? (fdb.dbproto/-subid db ["_auth/id" authority] false)))
                             :tx-string (:cmd command)
                             :signature (:sig command)
                             :nonce     nonce
                             :t         t}

          flakes            (->> (tx-meta-flakes tx-state error-str)
                                 (fdb.flake/sorted-set-by fdb.flake/cmp-flakes-block))
          hash-flake (generate-hash-flake flakes tx-state)]
      {:t      t
       :hash   (fdb.flake/o hash-flake)
       :flakes (conj flakes hash-flake)})))


(defn valid-tx-meta?
  "If a user supplies their own tx-meta we must validate it such that:
  - they never attempt to set the fluree-only predicates defined by system-predicates above
  - they never attempt to set a 't' value beyond the current t (historical t values are fine, assuming
    they have permission to do so which would be handled via smartfunction"
  [tx-meta-sid {:keys [validate-fn]}]
  (let [subject-flakes (get-in @validate-fn [:c-spec tx-meta-sid])]
    (doseq [flake subject-flakes]
      (when (system-predicates (fdb.flake/p flake))
        (throw (ex-info (str "Attempt to write a Fluree reserved predicate with flake: " flake)
                        {:error  :db/invalid-transaction
                         :status 400}))))
    true))
