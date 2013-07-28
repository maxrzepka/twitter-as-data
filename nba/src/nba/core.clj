(ns nba.core
  (:require  [cascalog.api :as ca]
             [cascalog.ops :as co]
             [cascalog.tap :as ct]
             [cascalog.more-taps :as cmt]
             [clojure.string :as s]
             [clojure-csv.core :as csv]))

(defn print-file
  "Use cascalog to print a file"
  [file-path]
  (let [file-tap (ca/lfs-textline file-path)]
    (ca/?<- (ca/stdout) [?line] (file-tap :> ?line))))

(defn parse-psv
  "Parse pipe-separated line"  
  [line]
  (first (csv/parse-csv line :delimiter \|)))

(ca/defmapcatop split [line]
  "reads string and splits it by regex"
  (s/split line #"[;,\s]+"))


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
