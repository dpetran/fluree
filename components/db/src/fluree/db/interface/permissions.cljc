(ns fluree.db.interface.permissions
  (:require [fluree.db.permissions :as permissions]
            [fluree.db.permissions-validate :as perm-validate]))

(defn permission-map
  [db roles permission-type]
  (permissions/permission-map db roles permission-type))

(defn allow-flake?
  ([db flake]
   (perm-validate/allow-flake? db flake))
  ([db flake permission]
   (perm-validate/allow-flake? db flake permission)))

(defn allow-flakes?
  [db flakes]
  (perm-validate/allow-flakes? db flakes))
