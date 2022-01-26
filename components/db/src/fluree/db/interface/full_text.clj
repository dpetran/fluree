(ns fluree.db.interface.full-text
  (:require [fluree.db.full-text :as full-text]))

(defn predicate?
  [f]
  (full-text/predicate? f))

(defn put-subject
  [idx wrtr subj pred-vals]
  (full-text/put-subject idx wrtr subj pred-vals))

(defn purge-subject
  [idx wrtr subj pred-vals]
  (full-text/purge-subject idx wrtr subj pred-vals))

(defn forget
  [idx wrtr]
  (full-text/forget idx wrtr))

(defn register-block
  [idx wrtr block-status]
  (full-text/register-block idx wrtr block-status))
