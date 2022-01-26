(ns fluree.ledger-api.ledger.transact.auth
  (:refer-clojure :exclude [resolve])
  (:require [clojure.core.async :as async]
            [fluree.db.interface.auth :as fdb.auth]
            [fluree.db.interface.async :as fdb.async]
            [fluree.db.interface.dbproto :as fdb.dbproto]
            [fluree.db.interface.util :as fdb.util]
            [fluree.db.interface.permissions :as fdb.permissions]
            [fluree.db.interface.log :as fdb.log]))

(set! *warn-on-reflection* true)

(defn- valid-authority?
  [db auth authority]
  (async/go
    (let [connection? (async/<! (fdb.dbproto/-search db [auth "_auth/authority" authority]))]
      (cond (fdb.util/exception? connection?)
            (do (fdb.log/error connection? "valid-authority? search for connection between " [auth "_auth/authority" authority]
                           "unexpectedly failed!")
                false)

            (empty? connection?)
            false

            :else
            true))))


(defn- resolve-auth+authority-sids
  "Returns two-tuple of [auth-sid and authority-sid].
  If no authority exists, returns nil for authority-sid.

  Performs lookups in parallel.

  Returns exception if both auth and authority (when applicable)
  do not resolve to an _auth/id"
  [db auth authority]
  (async/go
    (let [auth-id-ch    (fdb.dbproto/-subid db ["_auth/id" auth] true)
          ;; kick off authority check in parallel (when applicable)
          authority-sid (when authority
                          (let [authority-id (if (string? authority) ["_auth/id" authority] authority)]
                            (async/<! (fdb.dbproto/-subid db authority-id true))))
          auth-sid      (async/<! auth-id-ch)]
      (cond
        (fdb.util/exception? auth-sid)
        (ex-info (str "Auth id for transaction does not exist in the database: " auth)
                 {:status 403 :error :db/invalid-auth})

        (fdb.util/exception? authority-sid)
        (ex-info (str "Authority " authority " does not exist.")
                 {:status 403 :error :db/invalid-auth})

        (and authority-sid (false? (async/<! (valid-authority? db auth-sid authority-sid))))
        (ex-info (str authority " is not an authority for auth: " auth)
                 {:status 403 :error :db/invalid-auth})

        :else
        [auth-sid authority-sid]))))


(defn add-auth-ids-permissions
  "Figures out transaction permissions, returns map."
  [db tx-map]
  (fdb.async/go-try
    (let [{:keys [auth authority]} tx-map
          [auth-sid authority-sid] (fdb.async/<? (resolve-auth+authority-sids db auth authority))
          roles          (fdb.async/<? (fdb.auth/roles db auth-sid))
          tx-permissions (-> (fdb.async/<? (permissions/permission-map db roles :transact))
                             (assoc :auth auth-sid))]
      (assoc tx-map :auth-sid auth-sid
                    :authority-sid authority-sid
                    :tx-permissions tx-permissions))))
