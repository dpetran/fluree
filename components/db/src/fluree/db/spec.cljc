(ns fluree.db.spec
  (:require [fluree.db.util.json :as json]
            [alphabase.core :as alphabase]
            [fluree.db.util.core :as util :refer [try* catch*]]
            [clojure.string :as str]))

#?(:clj (set! *warn-on-reflection* true))

(def ^:private EMAIL #"[a-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*@(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?")

(defn safe-name
  [x]
  (try*
    (name x)
    (catch* e
            (str x))))


(defn type-check-error
  ([message] (throw (ex-info message {:status 400 :error :db/invalid-type})))
  ([x p-type] (type-check-error (str "Could not coerce value to " (util/keyword->str p-type) ": " x "."))))


(defn- regex-match
  "Checks value is a string and matches against provided regex.
  If fails, returns a type check error.

  If optional message is provided, includes message in error response."
  [re x p-type]
  (if (string? x)
    (if (re-matches re x)
      x
      (type-check-error x p-type))
    (type-check-error (str "Could not coerce non-string value to " (util/keyword->str p-type) ": " x "."))))


(defn type-check
  "(type-check type object) transforms a object to match the type. If it cannot be transformed, it throws an ex-info with a map from paths into the object to errors encountered at those paths."
  [x p-type]
  (try*
    (if (nil? x)
      nil
      (case p-type
        :string (if (keyword? x)
                  (subs (str x) 1)
                  (str x))

        :boolean (cond
                   (true? x) true
                   (false? x) false
                   (and (string? x) (= "true" (str/lower-case x))) true
                   (and (string? x) (= "false" (str/lower-case x))) false
                   :else (type-check-error x p-type))

        :instant (try*
                   (util/date->millis x)
                   (catch* _ (type-check-error x p-type)))

        :date (regex-match #"^\d{4}-\d\d-\d\d$" x p-type)

        :dateTime (regex-match #"^\d{4}-\d\d-\d\d(T\d\d:\d\d:\d\d(\.\d+)?)?(([+-]\d\d:\d\d)|Z)?$" x p-type)

        :time (regex-match #"^\d\d:\d\d:\d\d(\.\d+)?$" x p-type)

        :duration (regex-match #"^P(?!$)(\d+Y)?(\d+M)?(\d+W)?(\d+D)?(T(?=\d+[HMS])(\d+H)?(\d+M)?(\d+S)?)?$" x p-type)

        :uuid (cond
                (string? x) x
                (uuid? x) (str x)
                :else (type-check-error x p-type))

        :uri (str x)

        :bytes (cond
                 (string? x) (let [uc (str/lower-case x)]
                               (if (re-matches #"^[0-9a-f]+$" uc)
                                 uc
                                 (type-check-error "Bytes type must be in hex string form, provided: " x)))
                 #?@(:clj  [(bytes? x) (alphabase/bytes->hex x)]
                     :cljs [(sequential? (js->clj x)) (alphabase/bytes->hex x)])

                 :else (type-check-error x p-type))

        :int (cond
               (int? x) x
               (string? x) #?(:clj  (Integer/parseInt x)
                              :cljs (js/parseInt x))
               :else (type-check-error x p-type))

        :long (cond
                (number? x) (long x)
                (string? x) #?(:clj  (Long/parseLong x)
                               :cljs (let [i (js/parseInt x)]
                                       (if (<= util/min-long i util/max-long)
                                         i
                                         (type-check-error (str "Long value is outside of javascript max integer size of 2^53 - 1, provided: " x ".")))))
                :else (type-check-error x p-type))

        :bigint #?(:clj  (bigint x)
                   :cljs (let [i (if (string? x)
                                   (js/parseInt x)
                                   x)]
                           (if (<= util/min-long i util/max-long)
                             i
                             (type-check-error (str "Bigintegers are not supported in javascript. max integer size of 2^53 - 1, provided: " x ".")))))

        :float (cond
                 (number? x) (float x)
                 (string? x) #?(:clj  (Float/parseFloat x)
                                :cljs (js/parseFloat x))
                 :else (type-check-error x p-type))

        ;; TODO - double in JS should have a check for a valid value, see: https://stackoverflow.com/questions/45929493/node-js-maximum-safe-floating-point-number
        :double (cond
                  (number? x) (double x)
                  (string? x) #?(:clj  (Double/parseDouble x)
                                 :cljs (js/parseFloat x))
                  :else (type-check-error x p-type))

        :bigdec #?(:clj  (bigdec x)
                   :cljs (type-check-error (str "Javascript does not support big decimals. Provided: " x ".")))

        :json (try*
                (if (string? x)                             ;;confirm parsable
                  (do (json/parse x)
                      x)
                  ;; try to convert to json
                  (json/stringify x))
                (catch* _ (type-check-error x p-type)))

        :geojson (try*
                   (let [parsed (if (string? x)
                                  (json/parse x)
                                  x)]
                     (if (json/valid-geojson? parsed)
                       (if (string? x)
                         x
                         (json/stringify x))
                       (type-check-error x p-type)))
                   (catch* _ (type-check-error x p-type)))

        :tag (long x)

        :ref (long x)

        ;; else
        (type-check-error (str "Unknown type: " p-type "."))))

    (catch* _ (type-check-error (str "Could not conform value to: " p-type
                                     " with value: " (pr-str x) #?@(:clj [" of type " (type x)]) ".")))))
