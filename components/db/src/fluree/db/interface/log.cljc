(ns fluree.db.interface.log
  (:require [fluree.db.util.log :as log]))

(defmacro error
  [& args]
  `(log/error ~@args))

(defmacro warn
  [& args]
  `(log/warn ~@args))

(defmacro info
  [& args]
  `(log/info ~@args))

(defmacro debug
  [& args]
  `(log/debug ~@args))

(defmacro trace
  [& args]
  `(log/trace ~@args))
