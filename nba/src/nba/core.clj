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
  "split text by space, clean words and remove hyperlinks"
  [t]
  (keep (fn [w] (let [w (scrub-text w)]
                 (when (and (seq w)
                            (not (hyperlink? w))) w)))
        (s/split t #"[\s]+")))

(def datetime-formatter (tf/formatter "MMM dd HH:mm:ss +0000 yyyy"))
(defn parse-datetime [s]
  (try
    (tf/parse datetime-formatter (.substring s 4))
    (catch Throwable t nil)))

;; (map (comp (juxt identity time-split) last butlast extractor) (file->tweet ...))
(defn time-split [d]
  (if-let [d (parse-datetime d)]
    ((juxt t/hour t/minute t/sec) d)
    []))

(defn file->tweets
  "Parse a file line by line and parse them if in json format"
  [path]
  (keep (fn [s] (try (parse-json s) (catch Throwable t nil)))
        (.split (slurp path) "\n")))

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
         :created_at
         ;;cannot return nil so returns empty array
         (fn [t] (:coordinates (:coordinates t) [])) 
         )   
   tweet))

(def tweet-fields ["?id" "?lang" "?text" "?hashtags" "?mentions" "?terms" "?created_at" "?coord"])

(defn etl-tweet
  [dir & {:keys [trap] :or {trap "error"} :as opts}]
  (ca/<- [?id ?lang ?text ?hashtags ?mentions ?terms ?created_at ?coord]
         (extractor ?tweet :>> tweet-fields)
         ((ca/lfs-textline dir) ?line)
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

(defn expand-stop-field
  "Returns a query [?id ?term] where ?field is expand Given an tweet input tap expand the given field and exclude "
  [in stop field & {:keys [trap] :or {trap "error/stop"} :as opts}]
  (ca/<- [?id ?term]
         (stop ?term :> false)
         (list1 field :> ?term)
         (in :>> tweet-fields)
         (:trap (ca/lfs-textline trap))))

(defn terms-frequency
  "Given a input tap of tuples (id term) , returns query of term frequency"
  [in]
  (ca/<- [?term ?count]
         (co/count ?count)
         (in ?id ?term)))

(defn top-most
  "returns query of top n for agg"
  [count-q n agg & {:keys [trap] :or {trap "error/top"} :as opts}]
  (co/first-n count-q n :sort [agg] :reverse true :trap trap))

; (top-most-frequent-terms (etl-tweet "data/test/"))
(defn top-most-frequent-terms
  [in n & {:keys [trap] :or {trap "error/top"} :as opts}]
  (top-most (terms-frequency in) n "?count" :trap trap))

(defn searchkey-terms
  "Count how many times a searchkey find a tweet"
  [in searchkeys & {:keys [trap] :or {trap "error/terms"} :as opts}]
  (ca/<- [?id ?term]
         (searchkeys ?term :> true)
         (list1 ?terms :> ?term)
         (in :>> tweet-fields)
         (:trap (ca/lfs-textline trap))))

(defn time-count
  [in & {:keys [trap] :or {trap "error/time"} :as opts}]
  (ca/<- [?hour ?minute ?second ?count]
         (co/count ?count)
         (in :>> tweet-fields)
         (time-split ?created_at :> ?hour ?minute ?second)
         (:trap (ca/lfs-textline trap))))

(defn busiest-time
  [in n]
  (top-most (time-count in) n "?count"))

(defn non-empty? [c]
  (boolean (seq c)))

(defn localize [in  & {:keys [trap] :or {trap "error/local"} :as opts}]
  (ca/<- [?id ?coord]
         (non-empty? ?coord)
         (in :>> tweet-fields)))

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


;; Main Function
(defn -main [in out searchkeys stop trap]
  (let [tweet-stage (str out "/tweet")
        term-stage (str out "/term")]
    (ca/?- "ETL tweet data"
        (ca/lfs-seqfile tweet-stage :sinkmode :replace)
        (etl-tweet in :trap (str trap "/tweet")))
    (ca/?- "Time Aggregation"
           (cmt/lfs-delimited (str out "/time") :sinkmode :replace)
           (time-count (ca/lfs-seqfile tweet-stage) :trap (str trap "/time")))
    (ca/?- "Only localized Tweets"
           (cmt/lfs-delimited (str out "/local") :sinkmode :replace)
           (localize (ca/lfs-seqfile tweet-stage) :trap (str trap "/local")))    
    (ca/?- "Hashtag Count"
           (cmt/lfs-delimited (str out "/hashtag") :sinkmode :replace)
           (terms-frequency
            (expand-stop-field (ca/lfs-seqfile tweet-stage)
                               [] "?hashtags" :trap (str trap "/hashtag"))))
    (ca/?- "Mentions Count"
           (cmt/lfs-delimited (str out "/mention") :sinkmode :replace)
           (terms-frequency
            (expand-stop-field (ca/lfs-seqfile tweet-stage)
                               [] "?mentions" :trap (str trap "/mention"))))
    (ca/?- "Searchkeys Count"
           (cmt/lfs-delimited (str out "/search") :sinkmode :replace)
           (terms-frequency
            (searchkey-terms (ca/lfs-seqfile tweet-stage)
                             (cmt/lfs-delimited searchkeys)
                             :trap (str trap "/search"))))
    (ca/?- "Terms"
           (cmt/lfs-delimited term-stage :sinkmode :replace)
           (expand-stop-field
            (ca/lfs-seqfile tweet-stage)
            (cmt/lfs-delimited stop)
            "?terms"
            :trap (str trap "/term")))
    (ca/?- "Most Frequent Terms"
           (cmt/lfs-delimited (str out "/freqterm") :sinkmode :replace)
           (top-most-frequent-terms (cmt/lfs-delimited term-stage)
            100 :trap (str trap "/mostterm")))
    (let [src (ca/name-vars (cmt/lfs-delimited term-stage) ["?id" "?term"])]
      (ca/?- "TF-IDF for terms"
             (cmt/lfs-delimited (str out "/tdidf") :sinkmode :replace)
             (TF-IDF src)
             ;(TF-IDF (cmt/lfs-delimited term-stage))
             (:trap (ca/lfs-textline (str trap "/tdidf")))))))


;; Some queries for testing
#_(-main "data/test/game7.test.txt" "data/out/test"
         "data/test/game7.keywords.txt" "data/en.stop" "data/out/test/error")

#_(ca/??- (terms-frequency
           (expand-stop-field
            (etl-tweet "data/test/game7.test.txt") [] "?hashtags")))

#_(ca/??- (terms-frequency
           (expand-stop-field
            (ca/lfs-seqfile "data/out/tweet") [] "?hashtags"
            :trap "data/out/test/error")))
#_(ca/??- (searchkey-terms
           (etl-tweet "data/test/game7.test.txt")
           (cmt/lfs-delimited "data/test/game7.keywords.txt")))
