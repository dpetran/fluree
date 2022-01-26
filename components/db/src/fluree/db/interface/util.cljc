(ns fluree.db.interface.util
  (:require [fluree.db.util.core :as util-impl]))

(defn str->keyword
  [s]
  (util-impl/str->keyword s))

(defn keyword->str
  [k]
  (util-impl/keyword->str k))

(defn without-nils
  [m]
  (util-impl/without-nils m))

(defn pred-ident?
  [x]
  (util-impl/pred-ident? x))

(defn temp-ident?
  [x]
  (util-impl/temp-ident? x))

(defn exception?
  [x]
  (util-impl/exception? x))

(defn current-time-millis
  []
  (util-impl/current-time-millis))

(declare catch*)
(defmacro try*
  [& body]
  `(util-impl/try* ~@body))

(defn random-uuid
  []
  (util-impl/random-uuid))
