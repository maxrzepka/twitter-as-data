(ns nba.core-test
  (:use [midje sweet cascalog])
  (:require [clojure.test :refer :all]
            [nba.core :refer :all]
            [cascalog.api :as ca]
            [clojure.string :as s]
            ;; cannot use provided with such deps declaration
            ;[midje.sweet :as ms]
            ;[midje.cascalog :as mc]
            ))

(deftest test-split
  (fact "testing split with regexp"
        (s/split "as,v(df)g[ g j" #"[\|\[\]\(\)\{\}#@:?!;,\s]+")
        => ["as" "v" "df" "g" "g" "j"]
        (s/split "as,v(df)g[ g j" punc-regexp)
        => ["as" "v" "df" "g" "g" "j"]))

#_(deftest test-split-tweet
  (fact "test split tweet"
           ))

;; use external dataset for test or mock it ??

#_(deftest test-extractor
  (fact "test file->tweet")
  (fact "test extractor with hashtags,mentions,date and coord presents"))

;;broken TODO refactor code to 
#_(deftest test-etl-tweet
  (fact "testing etl-tweet"
           (ca/??- (etl-tweet :dir)) => (produces [["test text"]])
           (provided
            (ca/lfs-textline :dir) => [{:text "test text"}]
            (ca/lfs-textline "error") => {:type :1 :source nil :sink nil})
           ))

#_(deftest test-terms
  (fact "testing terms"
        (terms [[347840351815475200 "" "" () () ("rt"  "well" "i" "don't" "like") nil []]] [["i"] ["like"]]) => (produces [["rt" "well","like"]])))

(deftest test-terms-1
  (let [tweet [347840351815475200 "en" "" () () ["rt" "@lad_4_life" "@_la_laguneros_" "well" "i" "don't" "like" "spurs" "either" "but" "can't" "stand" "him" "even" "more" "lol"] nil []]
        tweet1 [347840351815475200 nil nil nil nil ["i" "rt" "stand " "him" "lol"] nil []]
        res (ca/??- (terms [tweet] ["i" "don't" "him"]))
        ->words (fn [res] (set (map second (first res))))
        terms1 (->words res)]
    (is
     (= ["we"] res)
     (= (set ["rt" "stand" ]) terms1))))


