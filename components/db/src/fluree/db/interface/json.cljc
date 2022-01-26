(ns fluree.db.interface.json
  (:require [fluree.db.util.json :as json-impl]))

(defn parse
  [x]
  (json-impl/parse x))

(defn stringify
  [x]
  (json-impl/stringify x))

(defn stringify-UTF8
  [x]
  (json-impl/stringify-UTF8 x))

(defn encode-BigDecimal-as-string
  [enable]
  (json-impl/encode-BigDecimal-as-string enable))
