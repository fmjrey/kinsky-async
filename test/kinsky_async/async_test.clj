(ns kinsky-async.async-test
  (:require [clojure.test :refer :all :as t]
            [clojure.pprint :as pp]
            [clojure.core.async :as a]
            [kinsky.client :as client]
            [kinsky-async.async :as async]
            [kinsky-async.embedded :as e]))

(def host "localhost")
(def kafka-port 9093)
(def zk-port 2183)
(def bootstrap-servers (format "%s:%s" host kafka-port))

(t/use-fixtures
  :once (fn [f]
          (let [z-dir (e/create-tmp-dir "zookeeper-data-dir")
                k-dir (e/create-tmp-dir "kafka-log-dir")]
            (try
              (with-open [k (e/start-embedded-kafka
                             {::e/host host
                              ::e/kafka-port kafka-port
                              ::e/zk-port zk-port
                              ::e/zookeeper-data-dir (str z-dir)
                              ::e/kafka-log-dir (str k-dir)
                              ::e/broker-config {"auto.create.topics.enable" "true"}})]
                (f))
              (catch Throwable t
                (throw t))
              (finally
                (e/delete-dir z-dir)
                (e/delete-dir k-dir))))))

(deftest duplex
  (testing "duplex index order"
    (let [up (a/chan 1)
          down (a/chan 1)
          duplex (async/duplex up down)]
      (is (= up (nth duplex 0)))
      (is (= down (nth duplex 1))))
    (let [up (a/chan 1)
          down (a/chan 1)
          duplex (async/duplex up down [up down])]
      (is (= up (nth duplex 0)))
      (is (= down (nth duplex 1))))
    (let [up (a/chan 1)
          down (a/chan 1)
          duplex (async/duplex up down [down up])]
      (is (= down (nth duplex 0)))
      (is (= up (nth duplex 1)))))
  (let [up (a/chan 1)
        down (a/chan 1)
        duplex (async/duplex up down)]
    (testing "->duplex->up->"
      (a/>!! duplex :up)
      (is (= :up (a/<!! up))))
    (testing "->down->duplex->"
      (a/>!! down :down)
      (is (= :down (a/<!! duplex))))))

(deftest channel-listener
  (testing "expected events"
    (let [tp1  (client/->topic-partition {:topic "t" :partition 1})
          tp2  (client/->topic-partition {:topic "t" :partition 2})
          tp3  (client/->topic-partition {:topic "t" :partition 3})
          tp4  (client/->topic-partition {:topic "t" :partition 4})
          ch   (a/chan)
          sink (client/rebalance-listener (async/channel-listener ch))]
      (.onPartitionsAssigned sink [tp1 tp2])
      (.onPartitionsRevoked sink [tp3 tp4])
      (a/close! ch)
      (is (= (a/<!! (a/into [] ch))
             [{:event :assigned
               :partitions [{:topic     "t" :partition 1}
                            {:topic     "t" :partition 2}]
               :type       :rebalance}
              {:event :revoked
               :partitions [{:topic     "t" :partition 3}
                            {:topic     "t" :partition 4}]
               :type       :rebalance}])))))

(defn close-response [op]
  [(async/->op-response op nil nil)
   async/eof-response])

(deftest producer
  (testing "close! channel"
    (let [[in out] (async/producer {:bootstrap.servers bootstrap-servers}
                                   :string :string)]
      (a/close! in)
      (is (= (close-response :close)
             (a/<!! (a/into [] out))))))
  (testing ":op :stop"
    (let [[in out] (async/producer {:bootstrap.servers bootstrap-servers}
                                   :string :string)]
      (a/>!! in {:op :stop})
      (is (= (close-response :stop)
             (a/<!! (a/into [] out))))))
  (testing ":op :close"
    (let [[in out] (async/producer {:bootstrap.servers bootstrap-servers}
                                   :string :string)]
      (a/>!! in {:op :close})
      (is (= (close-response :close)
             (a/<!! (a/into [] out))))))
  (testing ":op :flush"
   (let [[in out] (async/producer {:bootstrap.servers bootstrap-servers}
                                  :string :string)]
     (a/onto-chan in [{:op :flush}])
     (is (= (into [(async/->op-response :flush nil nil)]
                  (close-response :close))
            (a/<!! (a/into [] out)))))))

(deftest consumer
  (testing "close! channel"
    (let [[out ctl] (async/consumer {:bootstrap.servers bootstrap-servers}
                                    :string :string)]
      (a/close! ctl)
      (is (= (close-response :close)
             (a/<!! (a/into [] out))))))
  (testing ":op :stop"
    (let [[out ctl] (async/consumer {:bootstrap.servers bootstrap-servers}
                                    :string :string)]
      (a/>!! ctl {:op :stop})
      (is (= (close-response :stop)
             (a/<!! (a/into [] out))))))
  (testing ":op :close"
    (let [[out ctl] (async/consumer {:bootstrap.servers bootstrap-servers}
                                    :string :string)]
      (a/>!! ctl {:op :close})
      (is (= (close-response :close)
             (a/<!! (a/into [] out)))))))

(deftest topics
  (testing "collection of keywords"
    (let [[in out] (async/consumer {:bootstrap.servers bootstrap-servers
                                    :group.id "consumer-group-id"}
                                   :string :string)]
      (a/>!! in {:op     :subscribe
                 :topics [:x :y :z]}))))

(deftest roundtrip
  (let [msgs {:account-a {:action :login}
              :account-b {:action :logout}
              :account-c {:action :register}}
        p (async/producer {:duplex? true
                           :bootstrap.servers bootstrap-servers}
                          (client/keyword-serializer)
                          (client/edn-serializer))
        base-consumer-conf {:duplex? true
                            :bootstrap.servers bootstrap-servers
                            "enable.auto.commit" "false"
                            "auto.offset.reset" "earliest"
                            "isolation.level" "read_committed"}
        d (client/keyword-deserializer)
        s (client/edn-deserializer)
        setup! (fn [c t]
                 (a/>!! c {:op :subscribe :topic t})
                 (doseq [[k v] msgs]
                   (a/>!! p {:op :record :topic t :key k :value v}))
                 (a/close! p)
                 (future (Thread/sleep 5000) (a/close! c)))]
    (testing "msg roundtrip"
      (let [t "account"
            c (async/consumer (assoc base-consumer-conf
                                     :group.id "consumer-group-id-1")
                              d s)]
        (setup! c t)
        (is (= (->> (a/<!! (a/into [] c))
                    (filter #(-> % :type (= :record)))
                    (map (juxt :key :value))
                    (into {}))
               msgs))))))
