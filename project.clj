(defproject fmjrey/kinsky-async "0.1.0-SNAPSHOT"
  :description "Kafka clojure client based on core.async and kinsky"
  :plugins [[lein-codox "0.9.1"]
            [lein-ancient "0.6.15"]]
  :url "https://github.com/fmjrey/kinsky-async"
  :license {:name "MIT License"
            :url  "https://github.com/fmjrey/kinsky-async/tree/master/LICENSE"}
  :codox {:source-uri "https://github.com/fmjrey/kinsky-async/blob/{version}/{filepath}#L{line}"
          :metadata   {:doc/format :markdown}}
  :global-vars {*warn-on-reflection* true}
  :deploy-repositories [["snapshots" :clojars] ["releases" :clojars]]
  :dependencies [[org.clojure/clojure            "1.10.0"]
                 [org.clojure/core.async         "0.4.490"]
                 [org.apache.kafka/kafka-clients "2.3.0"]
                 [spootnik/kinsky "0.1.25-SNAPSHOT"]]
  :test-selectors {:default     (complement :integration)
                   :integration :integration
                   :all         (constantly true)}
  :profiles {:dev {:dependencies [[org.slf4j/slf4j-nop "1.7.25"]
                                  ;; for kafka embedded
                                  [org.apache.kafka/kafka_2.12 "2.3.0"]
                                  [org.apache.zookeeper/zookeeper "3.4.14"
                                   :exclusions [io.netty/netty
                                                jline
                                                org.apache.yetus/audience-annotations
                                                org.slf4j/slf4j-log4j12
                                                log4j]]]}})
