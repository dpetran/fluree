(ns fluree.ledger-api.main
  (:require
   [fluree.ledger-api.server :as server]
   [environ.core :as environ]
   [fluree.db.util.log :as log])
  (:gen-class))

(defn -main []
  (if-let [command (:fdb-command environ/env)]
    (server/execute-command command)
    (let [system (server/startup)]
      (.addShutdownHook
        (Runtime/getRuntime)
        (Thread. ^Runnable
                 (fn []
                   (log/info "SHUTDOWN Start")
                   (server/shutdown system)
                   (log/info "SHUTDOWN Complete")))))))
