(ns nba.core
  (:require  [cascalog.api :as ca]
             [cascalog.ops :as co]
             ;[cascalog.tap :as ct]
             ;[cascalog.checkpoint :as cc]
             [cascalog.more-taps :as cmt]
             [clojure.string :as s]
             [clojure.data.json :as json]
             [clojure-csv.core :as csv]
             [clj-time.core :as t]
             [clj-time.format :as tf]))

(defn parse-psv
  "Parse pipe-separated line"  
  [line]
  (first (csv/parse-csv line :delimiter \|)))

(defn parse-json [line]
  (json/read-str line :key-fn keyword))

(ca/defmapcatop split [line]
  "reads string and splits it by space"
  (s/split line #"[\s]+"))

(def punctuation "[](){}:;?!\",. \t\r\n")
(def punc-regexp #"[\[\]\(\)\{\}:?!;,\s]+")
 
(ca/defmapcatop split-by-punc [text]
  "split text by any kind of punctuation"
  (s/split text punc-regexp))

(defn delete-punct [s]
  (apply str
         (filter (comp not (set punctuation)) s)))

(defn scrub-text [s]
  "trim open punctuation and lower case"
  ((comp delete-punct s/lower-case) s))

;; Detect unicode string ,clean and convert it into meaning
;; TODO handle case where emoticon juxt word like "interesting:)"
;;   - http://rishida.net/tools/conversion/ app to test unicode strings
;; TODO get full list of unicode pictographs...
;;   - unicode databases
;;      - http://www.charbase.com/
;;      - http://www.fileformat.info/info/unicode/char/2764/index.htm
;; \u261d means WHITE UP POINTING INDEX
;; \u2764 means HEAVY BLACK HEART
;; remove invalid unicode [only the last is a good one]...

;; TODO detect unicode smiley
(defn unicode-smiley? [s]
  true)

;; TODO Store all emoticons known + match them with word :
;; ===))) is equivalent to =) ...
(defn emoticon?
  "Guess if a string is a smiley.
What is a smailey ?
Naive answer : any string containing only a given set of characters
"
  [^String s]
  (let [letters (set ";:,()PbD-_^$/3L'")]
    (and
     (> (.length s) 1)
     (every? letters s))))

;; classify smiley in sentiment category : positive/neutral/negative
;; Exploiting Emoticons in Sentiment Analysis
;;  http://eprints.eemcs.utwente.nl/23268/
;; Does Size Matter? Text and Grammar Revision for Parsing Social Media Data
;; http://cl.indiana.edu/~md7/papers/khan-et-al13.pdf

(defn hyperlink? [^String s]
  (boolean (re-find #"^(https?|ftp):" s)))

;; query to split twitter text into terms :
;;    - remove stopwords
(defn ->terms [text]
  (ca/<- [?term]
         (hyperlink? ?term :> false)
         (scrub-text ?word :> ?term)
         ;;TODO add word split for ascii emoticon
         (split text :> ?word)
         ([text] ?text)))

;; query to find keywords in terms
;; keywords are coming from file

(defn build-extract-join
  "Returns a function on maps : get-in _ ks + map f _"
  ([ks] (build-extract-join ks identity))
  ([ks f]
     (fn [m]
       (map f (get-in m ks)))))

(defn split-tweet
  "Plain fct to split text by space , clean terms and remove hyperlinks"
  [t]
  (keep (fn [w] (let [w (scrub-text w)]
                 (when-not (hyperlink? w) w)))
        (s/split t #"[\s]+")))

(def datetime-formatter (tf/formatter "MMM dd HH:mm:ss +0000 yyyy"))
(defn parse-datetime [s]
  (tf/parse datetime-formatter
            (.substring s 4)))

(defn extractor
  "Extract from json tweets the following tweets :
     - extract id , lang , text , hastags, mentions
     - convert text into terms
"
  [tweet]
  ((juxt :id :lang :text
         (build-extract-join [:entities :hashtags] :text)
         (build-extract-join [:entities :user_mentions] :screen_name)
         (comp split-tweet :text)
         (comp parse-datetime :created_at)
         )   
   tweet))

(defn etl-tweet
  [dir & {:keys [delimiter trap] :or {delimiter \| trap "error"} :as opts}]
  (ca/<- [?id ?lang ?text ?hashtags ?mentions ?terms]
         (extractor ?tweet :> ?id ?lang ?text ?hashtags ?mentions ?terms ?created_at)
         ((ca/lfs-textline dir) ?line)
         ;; TODO  extract header to extract search keywords used how?         
         (parse-json ?line :> ?tweet)
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

(ca/defmapcatop list1 [coll] (seq coll))

(defn terms
  "Source of all terms by tweet : in = output of etl-tweet"
  [in]
  (ca/<- [?id ?term]         
          (list1 ?terms :> ?term)         
          (in :> ?id ?lang ?text ?hashtag ?mention ?terms)))

(defn terms-frequency
  "in = output of etl-tweet"
  [in]
  (ca/<- [?term ?count]         
         (co/count ?count)
         ((terms in) ?id ?term)))

; (most-frequent-terms (etl-tweet "data/test/"))
(defn top20-most-frequent-terms
  "Depends on etl-tweet "
  [in]
  (let [count-q (terms-frequency in)
        q (co/first-n count-q 20 :sort ["?count"] :reverse true)]
    (ca/??- q)))


;; TD-IDF from Cascalog for the impatient adapted to tweets (just renaming)
;; https://github.com/Quantisan/Impatient/blob/cascalog/part6/src/impatient/core.clj

(defn D [src]
  (let [src  (ca/select-fields src ["?id"])]
    (ca/<- [?n-tweets]
        (src ?tweet-id)
        (co/distinct-count ?tweet-id :> ?n-tweets))))

(defn DF [src]
  (ca/<- [?df-term ?df-count]
      (src ?tweet-id ?df-term)
      (co/distinct-count ?tweet-id ?df-term :> ?df-count)))

(defn TF [src]
  (ca/<- [?tweet-id ?tf-term ?tf-count]
      (src ?tweet-id ?tf-term)
      (co/count ?tf-count)))

(defn tf-idf-formula [tf-count df-count n-tweets]
  (->> (+ 1.0 df-count)
    (ca/div n-tweets)
    (Math/log)
    (* tf-count)))

(defn TF-IDF [src]
  (let [n-tweet (first (flatten (ca/??- (D src))))]
    (ca/<- [?tweet-id ?tf-idf ?tf-term]
        ((TF src) ?tweet-id ?tf-term ?tf-count)
        ((DF src) ?tf-term ?df-count)
        (tf-idf-formula ?tf-count ?df-count n-tweet :> ?tf-idf))))

;; Some Analysis (only on english tweet ? ) :
;;   - unsupervised clustering based on TD-IDF
;;   - sentiment analysis on tweet
;;   - based on terms classes, perform clustering
;;   - supervised classification ( spurs , heat , neutral , both)


;; Main Function
(defn -main [in out searchkeys stop trap]
  (let [tweet-stage "data/out/tweet"]
    (ca/?- "ETL tweet data"
        (ca/lfs-seqfile tweet-stage)
        (etl-tweet in :trap (str trap "/tweet")))
    (ca/?- "Terms Frequency"
           (cmt/lfs-delimited out)
           (TF-IDF (terms (ca/lfs-seqfile tweet-stage))))
    ))
