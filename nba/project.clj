(defproject nba-finals-2013 "0.1.0-SNAPSHOT"
  :description "2013 NBA Finals Tweets Analysis"
  :url "https://github.com/maxrzepka/twitter-as-data/nba"
  :license {:name "Apache License, Version 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0"
            :distribution :repo}
  :uberjar-name "nba.jar"
  :aot [nba.core]
  :main nba.core
  :min-lein-version "2.0.0"
  ;:source-paths ["src"]
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.clojure/tools.logging "0.2.6"]
                 [cascalog "1.10.1"]
                 ;[cascalog-more-taps "0.3.1-SNAPSHOT"]
                 ;[cascalog/cascalog-checkpoint "1.10.2-SNAPSHOT"]
                 [clojure-csv/clojure-csv "2.0.0-alpha2"]
                 ;[org.clojars.sunng/geohash "1.0.1"]
                 [org.clojure/data.json "0.2.2"]
                 [date-clj "1.0.1"]]
  :exclusions [org.apache.hadoop/hadoop-core
               org.clojure/clojure]
  :profiles {:dev {:dependencies [[midje-cascalog "0.4.0"]
                                  [org.apache.hadoop/hadoop-core "1.0.4"
                                   :exclusions [org.slf4j/slf4j-api
                                                commons-logging
                                                commons-codec
                                                org.slf4j/slf4j-log4j12
                                                log4j]]]}
             :provided {:dependencies [[org.apache.hadoop/hadoop-core "0.20.2-dev"]]}}
  )
