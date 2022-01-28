(ns fluree.db.interface.async
  (:require [fluree.db.util.async :as fasync]))

(defmacro <?
  [ch]
  `(fasync/<? ~ch))

(defmacro <??
  [ch]
  `(fasync/<?? ~ch))

(defmacro go-try
  [& body]
  `(fasync/go-try ~@body))

(defn channel?
  [x]
  (fasync/channel? x))
