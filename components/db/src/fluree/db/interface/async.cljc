(ns fluree.db.interface.async
  (:require [fluree.db.util.async :as fasync]))

(defn <?
  [ch]
  (fasync/<? ch))

(defn <??
  [ch]
  (fasync/<?? ch))

(defmacro go-try
  [& body]
  `(fasync/go-try ~@body))

(defn channel?
  [x]
  (fasync/channel? x))
