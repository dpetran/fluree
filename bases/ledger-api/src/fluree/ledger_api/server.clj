(ns fluree.ledger-api.server
  (:gen-class)
  (:require [environ.core :as environ]
            [clojure.tools.logging :as log]
            [clojure.core.async :as async]
            [clojure.string :as str]
            [fluree.crypto :as crypto]
            [fluree.raft :as raft]
            [fluree.db.interface.async :as fdb.async]
            [fluree.db.interface.connection :as fdb.connect]
            [fluree.db.interface.constants :as fdb.const]
            [fluree.db.interface.util :as fdb.util]
            [fluree.ledger-api.server-settings :as settings]
            [fluree.ledger-api.peer.http-api :as http-api]
            [fluree.ledger-api.peer.messages :as messages]
            [fluree.ledger-api.ledger.indexing.full-text :as full-text]
            [fluree.ledger-api.ledger.reindex :refer [reindex]]
            [fluree.ledger-api.ledger.stats :as stats]
            [fluree.ledger-api.ledger.storage.memorystore :as memorystore]
            [fluree.ledger-api.ledger.txgroup.core :as txgroup]
            [fluree.ledger-api.ledger.upgrade :as upgrade]
            [fluree.ledger-api.ledger.consensus.tcp :as ftcp]
            [fluree.ledger-api.ledger.txgroup.txgroup-proto :as txproto]
            [clojure.pprint :as pprint]))

(set! *warn-on-reflection* true)

;; instantiates server operations

(defn local-message-process
  "Handles any local incoming messages, eventually producing a result
  that is passed to the producer-chan.

  The response on the producer chan will use the same req-id as the incoming message,
  allowing the response to be passed along to downstream client waiting.

  A message at this stage looks like:
  [operation req-id arg]"
  [system producer-chan]
  (fn [conn message]
    (async/thread
      (messages/message-handler (assoc system :conn conn) producer-chan message)
      true)))


(defn local-message-response
  "Monitors producer channel, and will respond to requests that are waiting."
  [conn producer-chan]
  (async/go-loop []
    (let [msg (async/<! producer-chan)]
      (when-not (nil? msg)
        (fdb.connect/process-events conn msg)
        (recur)))))




(defn shutdown
  "Perform a shutdown of system created with 'startup'"
  [system]
  (let [{:keys [conn webserver group stats]} system
        full-text-indexer (:full-text/indexer conn)
        try-continue (fn [f]
                       (try (f)
                            (catch Exception e
                              (log/error e "Exception executing close function: " (pr-str f)))))]
    (when (fn? (:close webserver))
      (try-continue (:close webserver)))
    (try-continue (fn [] (async/close! stats)))
    (when (fn? (:close group))
      (try-continue (:close group)))
    (when (fn? (:close full-text-indexer))
      (try-continue (:close full-text-indexer)))
    (when (fn? (:close conn))
      (try-continue (:close conn)))
    (ftcp/shutdown-client-event-loop (:this-server group))))


(defn check-version-upgrade-fn
  "Called whenever server newly becomes leader to upgrade raft data if needed."
  [conn system]
  ;; return a fluree/raft leader-watch 4-arg fn
  (fn [& _]
    (log/info "This server just became leader of the raft group.")
    ;; upgrade if needed
    (let [group           (:group conn)
          data-version    (txproto/data-version group)
          current-version fdb.const/data_version]
      (cond (= current-version data-version)
            nil

            ;; Current version > data-version, shutdown
            (< current-version data-version)
            (do (log/warn (str "Current data version: " current-version " is greater than the data version of Fluree currently running: " data-version ". Please retry this data with a more recent FlureeDB."))
                (shutdown system)
                (System/exit 1))

            ;; Can't hold up RAFT as it is used when upgrading - launch asynchronously
            (> current-version data-version)
            (future
              (upgrade/upgrade conn data-version current-version))))))


(defn assoc-some
  "Assoc k -> v in m if v is not nil. Returns m unaltered otherwise."
  [m k v]
  (if-not (nil? v)
    (assoc m k v)
    m))


(defn migrated?
  [{:keys [storage-list] :as conn} [network dbid]]
  (fdb.async/go-try
    (let [tspo-dir (str/join "/" [network dbid "tspo"])
          list-res (fdb.async/<? (storage-list tspo-dir))]
      (-> list-res seq boolean))))

(defn all-migrated?
  [{:keys [conn] :as system}]
  (fdb.async/go-try
   (loop [[ledger & rst] (-> conn :group txproto/all-ledger-list)]
     (if-not ledger
       true
       (if-not (fdb.async/<? (migrated? conn ledger))
         false
         (recur rst))))))

(defn startup
  ([] (startup (settings/build-env @environ/runtime-env)))
  ([settings]
   (log/info (str "Starting Fluree in mode: " (:fdb-mode settings)))
   (log/info "Starting with config:\n" (with-out-str
                                         (pprint/pprint
                                           (cond-> (into (sorted-map) settings) ;; hide encryption secret from logs
                                                   (:fdb-encryption-secret settings) (assoc :fdb-encryption-secret "prying eyes want to know...")))))
   (log/info "JVM arguments: " (str (stats/jvm-arguments)))
   (log/info "Memory Info: " (stats/memory-stats))

   (Thread/setDefaultUncaughtExceptionHandler
    (reify Thread$UncaughtExceptionHandler
      (uncaughtException [_ thread e]
        (log/error e "Uncaught exception on" (.getName thread)))))

   (let [config         (settings/build-settings settings)
         {:keys [transactor? consensus conn join?]} config
         consensus-type (:type consensus)
         storage-type   (:storage-type conn)
         memory?        (= :memory storage-type)
         group          (txgroup/start (:group config) consensus-type join?)
         remote-writer  (fn [k data]
                          (txproto/storage-write-async group k data))
         conn           (let [storage-write-fn  (case storage-type
                                                  :file remote-writer
                                                  :s3 remote-writer
                                                  :memory memorystore/connection-storage-write)
                              producer-chan     (async/chan (async/sliding-buffer 100))
                              publish-fn        (local-message-process {:config config :group group} producer-chan)
                              conn-opts         (cond-> (get-in config [:conn :options])

                                                  (= :memory storage-type)
                                                  (assoc :memory? true)

                                                  transactor?
                                                  (assoc :storage-write storage-write-fn
                                                         :publish publish-fn)

                                                  group
                                                  (assoc :group group))
                              servers           (when-not transactor?
                                                  (:fdb-query-peer-servers settings))
                              conn-impl         (fdb.connect/connect servers conn-opts)
                              full-text-indexer (full-text/start-indexer conn-impl)]
                          ;; launch message consumer, handles messages back from ledger
                          (local-message-response conn-impl producer-chan)

                          (assoc-some conn-impl :full-text/indexer full-text-indexer))
         system         {:config    config
                         :conn      conn
                         :webserver nil
                         :group     group}

         ;; add a leader-watch function to upgrade data if required
         _              (when (and (= :raft consensus-type) transactor?)
                          (raft/add-leader-watch (:raft group) ::upgrade (check-version-upgrade-fn conn system) :become-leader))

         webserver      (let [webserver-opts (-> (:webserver config)
                                                 (assoc :system system))]
                          (http-api/webserver-factory webserver-opts))
         ;; we are not a transacting peer in query mode, don't bother with this
         stats          (stats/initiate-stats-reporting system (-> config :stats :interval))
         system*        (assoc system :webserver webserver
                               :stats stats)]

     (when (and (or memory? (= consensus-type :in-memory))
                (not (and memory? (= consensus-type :in-memory))))
       (log/warn "Error during start-up. Currently if storage-type is 'memory', then consensus-type has to be 'in-memory' and vice versa.")
       (shutdown system*)
       (System/exit 1))

     (println "reindexing?" (pr-str (:reindexing? settings)))
     (when-not (and (not (:reindexing? settings))
                    (fdb.async/<?? (all-migrated? system)))
       (log/error "Error starting system. Index format out of date. Please upgrade indexes using the :reindex command.")
       (shutdown system*)
       (System/exit 1))

     ;; wait for initialization, and kick off some startup activities
     (when transactor?
       (txproto/-start-up-activities group conn system* shutdown join?))

     system*)))

(defn execute-command
  "Execute some arbitrary commands on FlureeDB (then exit)"
  [command]
  (println "executing command:" command)
  (case (fdb.util/str->keyword command)
    ;; generate public/private keys and also show corresponding account id
    :keygen
    (let [acct-keys  (crypto/generate-key-pair)
          account-id (crypto/account-id-from-private (:private acct-keys))]
      (println "Private:" (:private acct-keys))
      (println "Public:" (:public acct-keys))
      (println "Account id:" account-id))

    :reindex
    (let [{:keys [conn] :as system} (startup (assoc (settings/build-env @environ/runtime-env) :reindexing? true))]
      (try (doseq [[network dbid] (->> conn
                                       txproto/ledgers-info-map
                                       (map (juxt :network :ledger)))]
             (log/info "Rebuilding indexes for ledger [" network dbid "]")
             (let [status (fdb.async/<?? (reindex conn network dbid))]
               (log/info "Ledger rebuilding complete for ledger [" network dbid "]"
                         status)))
           (catch Exception e
             (log/error e "Failed to rebuild indexes."))
           (finally
             (shutdown system))))

    ;; else
    (println (str "Unknown command: " command)))
  (System/exit 0))
