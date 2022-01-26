(ns fluree.db.interface.http-signatures
  (:require [fluree.db.query.http-signatures :as http-signatures]))

(defn verify-request
  [request]
  (http-signatures/verify-request request))

(defn verify-request*
  ([req method uri]
   (http-signatures/verify-request* req method uri))
  ([req method action db-name]
   (http-signatures/verify-request* req method action db-name)))
