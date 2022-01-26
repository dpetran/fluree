(ns fluree.db.interface.time-travel
  (:require [fluree.db.time-travel :as time-travel-impl]))

(defn as-of-block
  "Gets the database as-of a specified block. Either block number or a time string in ISO-8601 format.
  Returns db as a promise channel"
  [db block-or-t-or-time]
  (time-travel-impl/as-of-block db block-or-t-or-time))

(defn non-border-t-to-block
  [db t]
  (time-travel-impl/non-border-t-to-block db t))
