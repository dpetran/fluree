(ns fluree.db.interface.auth
  (:require [fluree.db.auth :as auth]
            [fluree.db.token-auth :as token-auth]))

(defn roles
  [db auth_id]
  (auth/roles db auth_id))

(defn root-role?
  [db auth_id]
  (auth/root-role? db auth_id))

(defn generate-jwt
  [secret payload]
  (token-auth/generate-jwt secret payload))

(defn verify-jwt
  [secret jwt]
  (token-auth/verify-jwt secret jwt))
