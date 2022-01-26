(ns fluree.ledger-api.ledger.mutable
  (:require [fluree.db.interface.api :as fdb.api]
            [fluree.db.interface.async :as fdb.async]
            [fluree.db.interface.constants :as fdb.const]
            [fluree.db.interface.flake :as fdb.flake]
            [fluree.db.interface.log :as fdb.log]
            [fluree.db.interface.query-range :as fdb.query-range]
            [fluree.db.interface.storage :as fdb.storage]
            [fluree.db.interface.time-travel :as fdb.time-travel]
            [fluree.ledger-api.ledger.reindex :as reindex]))

(set! *warn-on-reflection* true)

(defn next-version
  [conn storage-block-key]
  (fdb.async/go-try
    (loop [n 1]
      (let [version-key (str storage-block-key "--v" n)]
        (if (fdb.async/<? (fdb.storage/exists? conn version-key))
          (recur (inc n))
          n)))))

(defn filter-flakes-from-block
  [block flakes]
  (let [ts (-> (map #(fdb.flake/t %) flakes) set)]
    (assoc block
      :flakes (filterv #(let [f %]
                          (and (not ((set flakes) f))
                               (not (and (= fdb.const/$_tx:tx (fdb.flake/p f))
                                         (ts (fdb.flake/t f))))))
                       (:flakes block)))))

(defn hide-block
  [conn nw ledger block flakes]
  (fdb.async/go-try
    (let [current-block (fdb.async/<? (fdb.storage/read-block conn nw ledger block))
          new-block     (filter-flakes-from-block current-block flakes)
          old-block-key (fdb.storage/ledger-block-file-path nw ledger block)
          new-block-key (str old-block-key "--v" (fdb.async/<? (next-version conn old-block-key)))
          _             (fdb.async/<?? (fdb.storage/rename conn old-block-key new-block-key))
          _             (fdb.async/<?? (fdb.storage/write-block conn nw ledger new-block))]
      (fdb.log/debug (str "Flakes hidden in block " block))
      true)))

(defn purge-block
  [conn nw ledger block flakes]
  (fdb.async/go-try
    (let [conn-rename   (:storage-rename conn)
          block-key     (fdb.storage/ledger-block-key nw ledger block)
          current-block (fdb.async/<? (fdb.storage/read-block conn nw ledger block))
          _             (fdb.async/<?? (conn-rename block-key (str block-key "-delete")))
          new-block     (filter-flakes-from-block current-block flakes)
          _             (fdb.async/<?? (fdb.storage/write-block conn nw ledger new-block))
          numVersions   (-> (next-version conn block-key) fdb.async/<? dec)]
      (loop [version numVersions]
        (when (> 0 version)
          (let [versioned-block     (fdb.async/<? (fdb.storage/read-block-version conn nw ledger block version))
                new-versioned-block (filter-flakes-from-block versioned-block flakes)]
            (fdb.async/<?? (fdb.storage/write-block-version conn nw ledger new-versioned-block version)))
          (recur (dec version))))
      (fdb.log/warn (str "Flakes purged from block " block))
      true)))

(defn identify-hide-blocks-flakes
  [db {:keys [block hide purge] :as query-map}]
  (fdb.async/go-try
    (let [[pattern idx] (fdb.api/get-history-pattern (or hide purge))
          [block-start block-end] (when block (fdb.async/<? (fdb.api/resolve-block-range db query-map)))
          from-t    (if (and block-start (not= 1 block-start))
                      (dec (:t (fdb.async/<? (fdb.time-travel/as-of-block db (dec block-start))))) -1)
          to-t      (if block-end
                      (:t (fdb.async/<? (fdb.time-travel/as-of-block db block-end))) (:t db))
          flakes    (fdb.async/<? (fdb.query-range/time-range db idx = pattern {:from-t from-t :to-t to-t}))
          block-map (fdb.async/<? (fdb.async/go-try (loop [[flake & r] flakes
                                                 t-map     {}
                                                 block-map {}]
                                            (if flake
                                              (if-let [block (get t-map (fdb.flake/t flake))]
                                                (recur r t-map (update block-map block conj flake))
                                                (let [t     (fdb.flake/t flake)
                                                      block (fdb.async/<? (fdb.time-travel/non-border-t-to-block db t))]
                                                  (if (get block-map block)
                                                    (recur r (assoc t-map t block) (update block-map block conj flake))
                                                    (recur r (assoc t-map t block) (assoc block-map block [flake])))))
                                              block-map))))]
      [block-map (count flakes)])))

;; Use a pattern [s p o] to declare flakes to hide.
;; Hide both additions and retractions, as well as the _tx/tx for that block
(defn hide-flakes
  [conn nw ledger query-map]
  ;; TODO - this can take some time. Need a good way to handle this.
  (fdb.async/go-try
    (let [db         (fdb.async/<? (fdb.api/db conn (str nw "/" ledger)))
          [block-map fuel] (fdb.async/<? (identify-hide-blocks-flakes db query-map))
          _          (when (not-empty block-map)
                       (loop [[[block flakes] & r] block-map]
                         (fdb.async/<?? (hide-block conn nw ledger block flakes))
                         (if r (recur r)
                             true)))
                                        ; Pass in a custom ecount, so as not to have multiple items
                                        ; with same subject id
          old-ecount (:ecount db)
          _          (fdb.async/<? (reindex/reindex conn nw ledger {:ecount old-ecount}))]
      {:status 200
       :result {:flakes-hidden fuel
                :blocks        (keys block-map)}
       :fuel   fuel})))

(defn purge-flakes
  [conn nw ledger query-map]
  ;; TODO - this can take some time. Need a good way to handle this.
  (fdb.async/go-try
    (let [db         (fdb.async/<? (fdb.api/db conn (str nw "/" ledger)))
          ;; TODO - this doesn't work if you've previously hidden the data.
          [block-map fuel] (fdb.async/<? (identify-hide-blocks-flakes db query-map))
          _          (when (not-empty block-map)
                       (loop [[[block flakes] & r] block-map]
                         (fdb.async/<?? (purge-block conn nw ledger block flakes))
                         (if r (recur r)
                             true)))
          ;; Pass in a custom ecount, so as not to have multiple items
          ;; with same subject id
          old-ecount (:ecount db)
          _          (fdb.async/<? (reindex/reindex conn nw ledger {:ecount old-ecount}))]
      {:status 200
       :result {:flakes-purged fuel
                :blocks        (keys block-map)}
       :fuel   fuel})))

(comment
  (require '[fluree.db.serde.protocol :as serdeproto])
  (def conn (:conn user/system))

  (fdb.async/<?? (hide-flakes conn "fluree" "test" {:hide [87960930223081]}))
  (fdb.async/<?? (purge-flakes conn "fluree" "test" {:purge [422212465065991]}))

  (->> (fdb.async/<?? (fdb.storage/read conn "fluree_test_block_000000000000004:v2"))
       (serdeproto/-deserialize-block (:serializer conn))))
