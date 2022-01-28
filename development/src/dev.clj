(ns dev
  (:require [clojure.repl :refer :all]
            [clojure.core.async :as async]
            [fluree.ledger-api.server :as server]
            [fluree.ledger-api.server-settings :as settings]
            [fluree.db.interface.api :as fdb.api]
            [fluree.db.interface.async :as fdb.async]
            [environ.core :as environ]))


(defn start-one
  ([] (start-one {}))
  ([override-settings]
   (let [settings (-> (settings/build-env environ/env)
                      (merge override-settings))]
     (server/startup settings))))


(defn stop-one [s]
  (when s (server/shutdown s))
  :stopped)

(comment
  (def ledger-peer (start-one {:fdb-api-port            8090
                               :fdb-mode                "ledger"
                               :fdb-group-servers       "ledger-server@localhost:11001"
                               :fdb-group-this-server   "ledger-server"
                               :fdb-group-log-directory "./development/data/raft"
                               :fdb-storage-file-root   "./development/data"}))

  (stop-one ledger-peer)

  (settings/build-env environ/env)






  ,)
