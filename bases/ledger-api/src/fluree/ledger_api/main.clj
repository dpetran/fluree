(ns fluree.ledger-api.main
  (:require
   [fluree.ledger-api.server :as server]
   [environ.core :as environ]
   [fluree.db.interface.log :as fdb.log])
  (:gen-class))

(defn -main []
  (if-let [command (:fdb-command environ/env)]
    (server/execute-command command)
    (let [system (server/startup)]
      (.addShutdownHook
        (Runtime/getRuntime)
        (Thread. ^Runnable
                 (fn []
                   (fdb.log/info "SHUTDOWN Start")
                   (server/shutdown system)
                   (fdb.log/info "SHUTDOWN Complete")))))))
