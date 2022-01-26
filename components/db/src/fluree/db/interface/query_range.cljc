(ns fluree.db.interface.query-range
  (:require [fluree.db.query.range :as range-impl]))

(defn index-range
  "Range query across an index as of a 't' defined by the db.

  Ranges take the natural numeric sort orders, but all results will
  return in reverse order (newest subjects and predicates first).

  Returns core async channel.

  opts:
  :xform - xform applied to each result individually. This is not used when :chan is supplied.
  :limit - max number of flakes to return"
  ([db idx] (range-impl/index-range db idx))
  ([db idx opts] (range-impl/index-range db idx opts))
  ([db idx test match] (range-impl/index-range db idx test match))
  ([db idx test match opts] (range-impl/index-range db idx match opts))
  ([db idx start-test start-match end-test end-match]
   (range-impl/index-range db idx start-test start-match end-test end-match))
  ([db idx start-test start-match end-test end-match opts]
   (range-impl/index-range db idx start-test start-match end-test end-match opts)))

(defn time-range
  "Range query across an index.

  Uses a DB, but in the future support supplying a connection and db name, as we
  don't need a 't'

  Ranges take the natural numeric sort orders, but all results will return in
  reverse order (newest subjects and predicates first).

  Returns core async channel.

  opts:
  :from-t - start transaction (transaction 't' is negative, so smallest number
            is most recent). Defaults to db's t
  :to-t - stop transaction - can be null, which pulls full history
  :xform - xform applied to each result individually. This is not used
           when :chan is supplied.
  :limit - max number of flakes to return"
  ([db idx]
   (range-impl/time-range db idx))
  ([db idx opts]
   (range-impl/time-range db idx opts))
  ([db idx test match]
   (range-impl/time-range db idx test match))
  ([db idx test match opts]
   (range-impl/time-range db idx test match opts))
  ([db idx start-test start-match end-test end-match]
   (range-impl/time-range db idx start-test start-match end-test end-match))
  ([db idx start-test start-match end-test end-match opts]
   (range-impl/time-range db idx start-test start-match end-test end-match opts)))

(defn collection
  ([db name]
   (range-impl/collection db name))
  ([db name opts]
   (range-impl/collection db name opts)))
