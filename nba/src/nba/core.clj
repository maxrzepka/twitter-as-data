(ns nba.core
  (:require  [cascalog.api :as ca]
             [cascalog.ops :as co]
             [cascalog.tap :as ct]
             [cascalog.more-taps :as cmt]
             [clojure.string :as s]
             [clojure.data.json :as json]
             [clojure-csv.core :as csv]))

(defn parse-psv
  "Parse pipe-separated line"  
  [line]
  (first (csv/parse-csv line :delimiter \|)))

(ca/defmapcatop split [line]
  "reads string and splits it by regex"
  (s/split line #"[;,\s]+"))

;; Extract from json tweets the following tweets :
;;   - extract id , lang , text , hastag, mention
;;   - cleanup text field : replace \n and | to other characters
;;   - ??howto?? extract first line and extract keywords used for the search
;;   - find keywords in text --> new field keywords
;;   - language detection --> new field reallang
;;   - remove stopwords, punctuation , hyperlinks --> new field terms

(defn etf-tweet
  [dir out & {:keys [delimiter trap] :or {delimiter \| trap "error"} :as opts}]
  (ca/<- [?tweet]
      ((ca/lfs-textline dir) ?line)
      (json/read-str ?line :key-fn keyword :> ?tweet)
      (:trap (ca/lfs-textline trap))))

;; Some Stats :
;;   - terms frequency --> find relevant terms in this context (game, win, heat)
;;   - most found search keywords
;;   - most used hashtags
;;   - most used entities
;;   - compute TD-IDF
;; Goal : from stats determine manually most relevant terms and classify them
;;   - classes : spurs , miamiheat , both , none , positive , neutral , negative
;;
(defn most-frequent-terms
  [in out & {:keys [delimiter trap] :or {delimiter \| trap "error"} :as opts}]
  (let [count-q (ca/<- [?term ?count]         
          (co/count ?count)
          (split ?terms :> ?term)         
          (parse-psv ?line :> ?id ?lang ?text ?hashtag ?mention ?terms ?searchkeys)
          ((ca/lfs-textline in) ?line)   
          (:trap (ca/lfs-textline trap)))
        q (co/first-n count-q 20 :sort ["?count"] :reverse true)]
    (ca/??- q)))


;; TD-IDF as it is in Cascalog for the impatient
;; https://github.com/Quantisan/Impatient/blob/cascalog/part6/src/impatient/core.clj

#_(defn D [src]
  (let [src  (select-fields src ["?doc-id"])]
    (<- [?n-docs]
        (src ?doc-id)
        (c/distinct-count ?doc-id :> ?n-docs))))

#_(defn DF [src]
  (<- [?df-word ?df-count]
      (src ?doc-id ?df-word)
      (c/distinct-count ?doc-id ?df-word :> ?df-count)))

#_(defn TF [src]
  (<- [?doc-id ?tf-word ?tf-count]
      (src ?doc-id ?tf-word)
      (c/count ?tf-count)))

(defn tf-idf-formula [tf-count df-count n-docs]
  (->> (+ 1.0 df-count)
    (ca/div n-docs)
    (Math/log)
    (* tf-count)))

#_(defn TF-IDF [src]
  (let [n-doc (first (flatten (??- (D src))))]
    (<- [?doc-id ?tf-idf ?tf-word]
        ((TF src) ?doc-id ?tf-word ?tf-count)
        ((DF src) ?tf-word ?df-count)
        (tf-idf-formula ?tf-count ?df-count n-doc :> ?tf-idf))))

;; Some Analysis (only on english tweet ? ) :
;;   - unsupervised clustering based on TD-IDF
;;   - sentiment analysis on tweet
;;   - based on terms classes, perform clustering
;;   - supervised classification ( spurs , heat , neutral , both)
