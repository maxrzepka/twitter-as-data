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

(defn parse-json [line]
  (json/read-str line :key-fn keyword))

(ca/defmapcatop split [line]
  "reads string and splits it by space"
  (s/split line #"[\s]+"))

(def punctuation "[](){}:;?!\",. \t\r\n")
(def punc-regexp #"[\[\]\(\)\{\}:?!;,\s]+")
;; 
(ca/defmapcatop split-by-punc [text]
  "split text by any kind of punctuation"
  (s/split text punc-regexp))

(defn delete-punct [s]
  (apply str
         (filter (comp not (set punctuation)) s)))

;;TODO handle case where emoticon juxt word like "interesting:)"
;; possible trim-split logic : 
;;   
(defn scrub-text [s]
  "trim open punctuation and lower case"
  ((comp delete-punct s/lower-case) s))

;; detect unicode string ,clean and convert it to meaning
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
  "Returns a function on maps : get-in _ ks + map f _ + join sep _"
  ([ks] (build-extract-join ks identity " "))
  ([ks f] (build-extract-join ks f " "))
  ([ks f sep]
     (fn [m]
       (s/join sep (map f (get-in m ks))))))

;; Extract from json tweets the following tweets :
;;   - extract id , lang , text , hastag, mention
(defn extractor [tweet]
  ((juxt :id :lang :text
         (build-extract-join [:entities :hashtags] :text)
         (build-extract-join [:entities :user_mentions] :screen_name))
   tweet))

(defn etf-tweet
  [dir out & {:keys [delimiter trap] :or {delimiter \| trap "error"} :as opts}]
  (ca/<- [?id ?lang ?text ?hashtags ?mentions]
         (extractor ?tweet :> ?id ?lang ?text ?hashtags ?mentions)
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
