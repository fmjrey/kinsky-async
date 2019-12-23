(ns kinsky-async.async
  "Clojure core.async support on top of kinsky.
   See https://github.com/fmjrey/kinsky-async for example usage."
  (:require [clojure.core.async :as a]
            [clojure.core.async.impl.protocols :as impl]
            [kinsky.client      :as client])
  (:import [java.lang IllegalStateException]
           [java.util ConcurrentModificationException]
           [org.apache.kafka.common.errors WakeupException]))

(defn duplex
  "Create a single duplex channel from 2 `up` and `down` channels.
  Writing to the duplex channel writes to the `up` channel.
  Reading from the duplex channel reads from the `down` channel.
  An additional vector can be passed to back the implementation
  of the Indexed interface and its`nth` method.
  Alternatively one can use `get` with the key `:in`, `:up`, or `:sink`
  to get the `up` channel, and `:out`, `:down`, or `:source` to get the
  `down` channel."
  ([up down] (duplex up down [up down]))
  ([up down indexed]
   (reify
     impl/ReadPort
     (take! [_ fn-handler]
       (impl/take! down fn-handler))
     impl/WritePort
     (put! [_ val fn-handler]
       (impl/put! up val fn-handler))
     impl/Channel
     (close! [_]
       (impl/close! up)
       (impl/close! down))
     (closed? [_]
       (and (impl/closed? up)
            (impl/closed? down)))
     clojure.lang.Indexed
     (nth [_ idx]
       (nth indexed idx))
     (nth [_ idx not-found]
       (nth indexed idx not-found))
     clojure.lang.Counted
     (count [_] 2)
     clojure.lang.ILookup
     (valAt [this key]
       (.valAt this key nil))
     (valAt [_ key not-found]
       (case key
         (:sink :up :in) up
         (:source :down :out) down
         not-found)))))

(def default-input-buffer
  "Default amount of messages buffered on control channels."
  10)

(def default-output-buffer
  "Default amount of messages buffered on the record channel."
  100)

(def default-timeout
  "Default timeout, by default we poll at 100ms intervals."
  100)

(defn exception?
  "Test if a value is a subclass of Exception"
  [e]
  (instance? Exception e))

(defn ex-response
  "Return a response containing the given exception."
  [e]
  {:type :exception :exception e})

(def eof-response {:type :eof})

(defn make-consumer
  "Build a consumer, with or without deserializers"
  [config kd vd]
  (let [opts (dissoc config :input-buffer :output-buffer :timeout :topic)]
    (cond
      (and kd vd)      (client/consumer opts kd vd)
      :else            (client/consumer opts))))

(defn channel-listener
  "A rebalance-listener compatible call back which produces all
   events onto a channel."
  [ch]
  (fn [event]
    (a/put! ch (assoc event :type :rebalance))))

(def record-xform
  "Rely on the standard transducer but indicate that this is a record."
  (comp client/record-xform (map #(assoc % :type :record))))

(defn get-within
  [ctl tm]
  (let [max-wait (a/timeout tm)]
    (a/alt!!
      max-wait ([_] nil)
      ctl      ([v] (or v {:op :close})))))

(def send? #{:record :records :send-from})
(def close? #{:close :stop})

(defn ->op-response
  "Convert operation result or exception into a response map."
  [op result e]
  (cond
    e (assoc (ex-response e) :op op)
    (send? op) (assoc result :op op
                             :type :record-metadata)
    :else {:op op :type :result :result result}))

(defn- respond
  "Send `result` or exception `e` back to application on `out` channel,
  or when provided on `response-ch` which is then closed.
  Return `result` or nil if an exception `e` is provided (not nil)."
  ([op out response-ch result]
   (respond op out response-ch result nil))
  ([op out response-ch result e]
   (a/>!! (or response-ch out) (->op-response op result e))
   (when response-ch (a/close! response-ch))
   (when-not e result)))

(defn poller-ctl
  [ctl out driver timeout]
  (if-let [payload (get-within ctl timeout)]
    (let [{:keys [op response-ch]} payload
          op (or op :close)]
      (if (close? op)
        (respond op out response-ch (client/close! driver (:timeout payload)))
        (do
          (try
            (case op
              :callback
              (let [f (:callback payload)]
                (or (f driver out)
                    (not (:process-result? payload)))) ;; TODO: explain

              (or
               (case op
                 :subscribe
                 (respond op out response-ch
                   (client/subscribe! driver
                                      (or (:topics payload) (:topic payload))
                                      (channel-listener out)))

                 :unsubscribe
                 (respond op out response-ch (client/unsubscribe! driver))

                 :seek
                 (let [{:keys [offset topic-partition topic-partitions]} payload]
                   (respond op out response-ch
                     (if (number? offset)
                       (client/seek! driver topic-partition offset)
                       (case offset
                         :beginning (client/seek-beginning! driver topic-partitions)
                         :end (client/seek-end! driver topic-partitions)))))

                 :commit
                 (respond op out response-ch
                          (if-let [topic-offsets (:topic-offsets payload)]
                            (client/commit! driver topic-offsets)
                            (client/commit! driver)))

                 :pause
                 (respond op out response-ch
                          (client/pause! driver (:topic-partitions payload)))

                 :resume
                 (respond op out response-ch
                          (client/resume! driver (:topic-partitions payload)))

                 :partitions-for
                 (respond op out response-ch
                          {:op         :partitions-for
                           :type       :partitions
                           :partitions (client/partitions-for driver
                                                              (-> payload
                                                                  :topic
                                                                  name))}))
               true))))))
    true))

(defn close-poller
  [ctl out driver]
  (a/put! out eof-response)
  (a/close! ctl)
  (client/close! driver)
  (a/close! out))

(defn safe-poll
  [ctl recs out driver timeout]
  (try
    (let [records (client/poll! driver timeout)]
      (a/>!! recs records)
      true)
    (catch WakeupException _
      (poller-ctl ctl out driver timeout))
    (catch IllegalStateException _
      (poller-ctl ctl out driver timeout))
    (catch ConcurrentModificationException e
      ;; Cannot happen unless there is a bug, so don't hide it
      (throw e))
    (catch Exception e
      (a/put! out (ex-response e))
      false)))

(defn poller-thread
  "Poll for next messages, catching exceptions and yielding them."
  [driver inbuf outbuf timeout]
  (let [ctl      (a/chan inbuf)
        mult     (a/mult ctl)
        c1       (a/chan inbuf)
        c2       (a/chan inbuf)
        out      (a/chan outbuf)
        recs     (a/chan outbuf record-xform (fn [e] (throw e)))]
    (a/tap mult c1)
    (a/tap mult c2)
    (a/pipe recs out)
    (a/thread
      (.setName (Thread/currentThread) (str driver "-control-poller"))
      (loop []
        (when-let [cr (a/<!! c2)]
          (client/wake-up! driver)
          (recur))))
    (a/thread
      (.setName (Thread/currentThread) (str driver "-poller"))
      (try
        (loop []
          (when (safe-poll c1 recs out driver timeout)
            (recur)))
        (catch Exception e
          (a/put! out (ex-response e)))
        (finally
          (close-poller ctl out driver))))
    [ctl out]))

(defn consumer
  "Build an async consumer. Yields a vector of two channels `[out ctl]`,
   the first conveys records and operation results back to the application,
   and the second is for the application to send operations.

   Arguments config ks and vs work as for kinsky.client/consumer.
   The config map must be a valid consumer configuration map and may contain
   the following additional keys:

   - `:input-buffer`: fixed buffer size for `ctl` channel.
   - `:output-buffer`: fixed buffer size for `out` channel.
   - `:timeout`: Poll interval in milliseconds
   - `:topic` : Automatically subscribe to this topic before launching loop
   - `:duplex?`: yields a duplex channel instead of a vector of two channels.

   The resulting control channel is used to interact with the consumer driver
   and expects map payloads, whose operation is determined by their `:op` key.
   An optional response channel can be provided under the `:response-ch` key,
   in which case the operation results are sent to that channel instead of the
   out/duplex channel.
   The optional response channel is closed once the operation completes
   while the out/duplex channel remains open until a `:close` operation.

   The following operations are handled:

   - `:subscribe`: `{:op :subscribe :topic \"foo\"}` subscribe to a topic.
   - `:unsubscribe`: `{:op :unsubscribe}`, unsubscribe from all topics.
   - `:seek`: `{:op :seek :offset 1234 :topic-partition {:topic \"foo\"
                                                         :partition 0}}`, or
      `{:op :seek :offset :beginning :topic-partitions [{:topic \"foo\"
                                                         :partition 0}]}`, or
      `{:op :seek :offset :end :topic-partitions [{:topic \"foo\"
                                                   :partition 0}
                                                  {:topic \"bar\"
                                                   :partition 0}]}`.
      Overrides the fetch offsets the consumer will use on the next poll.
      The offset must be a long value to seek on a single topic-partition,
      or keyword `:beginning` or `:end` to seek on a seq of topic-partitions.
   - `:partitions-for`: `{:op :partitions-for :topic \"foo\"}`, yield partition
      info for the given topic. If a `:response` key is present, produce the
      response there instead of on the output channel.
   - `:commit`: `{:op :commit}` commit offsets, an optional `:topic-offsets` key
      may be present for specific offset committing.
   - `:pause`: `{:op :pause}` pause consumption.
   - `:resume`: `{:op :resume}` resume consumption.
   - `:callback`: `{:op :callback :callback (fn [d ch])}` Execute a function
      of 2 arguments, the consumer driver and output channel, on a woken up
      driver.
   - `:close`: `{:op :close}` stop and close consumer and all channels.
   - `:stop`: `{:op :stop}` same as :close.


   The out/duplex or response channel emit payloads as maps containing an `:op`
   key which value is taken from the input operation generating the output and
   a `:type` key where `:type` may be:

   - `:success`: the operation succeeded, there are no additional result keys.
   - `:record`: A consumed record, details are in additional keys.
   - `:exception`: An exception raised, details are in additional keys.
   - `:rebalance`: A rebalance event.
   - `:eof`: The end of this stream.
   - `:partitions`: The result of a `:partitions-for` operation are under an
     additional `:partitions` key.
   - `:woken-up`: A notification that the consumer was woken up.

   A closed control channel has the same effect as a :close operation.
  "
  ([config]
   (consumer config nil nil))
  ([config kd vd]
   (let [topic     (:topic config)
         duplex?   (:duplex? config)
         inbuf     (or (:input-buffer config) default-input-buffer)
         outbuf    (or (:output-buffer config) default-output-buffer)
         timeout   (or (:timeout config) default-timeout)
         driver    (make-consumer (dissoc config :duplex? :topic) kd vd)
         [ctl out] (poller-thread driver inbuf outbuf timeout)]
     (when topic
       (a/put! ctl {:op :subscribe :topic topic})) ;; use poller thread!
     (if duplex?
       (duplex ctl out [out ctl])
       [out ctl]))))

(defn make-producer
  "Build a producer, with or without serializers"
  [config ks vs]
  (cond
    (and ks vs) (client/producer config ks vs)
    :else       (client/producer config)))

(defn producer
  "Build a producer. Yields a vector of two channels `[in out]`, the first
   is for the application to send records and operations, and the second
   conveys operation results back to the application.

   Arguments mirror those of kinsky.client/producer.
   The config map must be a valid consumer configuration map and may contain
   the following additional keys:

   - `:input-buffer`: fixed buffer size for `in` channel.
   - `:output-buffer`: fixed buffer size for `out` channel.
   - `:duplex?`: yields a duplex channel instead of a vector of two chans

   The input channel is used to send operations to the producer driver
   and expects map payloads, whose operation is determined by their `:op` key.
   An optional response channel can be provided under the `:response-ch` key,
   in which case the operation results are sent to that channel instead of the
   out/duplex channel.
   The optional response channel is closed once the operation completes
   while the out/duplex channel remains open until a `:close` operation.

   The following operations are handled:

   - `:record`: `{:op :record :topic \"foo\"}` send a record out, also
      performed when no `:op` key is present.
   - `:records`: `{:op :records :records [...]}` send a collection of records.
   - `:flush`: `{:op :flush}`, flush unsent messages.
   - `:init-transactions`: prepare the producer for transaction operations using
      the `transactional.id` from producer config.
   - `:begin-transaction`: initiate a new transaction.
   - `:commit-transaction`: terminate current transaction.
   - `:abort-transaction`: abort current transaction.
   - `:partitions-for`: `{:op :partitions-for :topic \"foo\"}`, yield partition
      info for the given topic.
   - `:close`: `{:op :close}`, close the producer and all channels.
   - `:stop`: `{:op :stop}` same as :close.

   The out/duplex or response channel emit payloads as maps containing an `:op`
   key which value is taken from the input operation generating the output and
   a `:type` key where `:type` may be:

   - `:success`: the operation succeeded, there are no additional result keys.
   - `:record-metadata`: The result of a successful send with additional keys:
      {:topic     \"topic-name\"
       :partition 0 ;; nil if unknown
       :offset    1234567890
       :timestamp 9876543210}
   - `:exception`: An exception was raised, details are in additional keys.
   - `:partitions`: The result of a `:partitions-for` operation are under an
     additional `:partitions` key.
   - `:eof`: The producer is closed.

   A closed input channel has the same effect as a `:close` operation.
  "
  ([config]
   (producer config nil nil))
  ([config ks]
   (producer config ks ks))
  ([config ks vs]
   (let [duplex? (:duplex? config)
         inbuf   (or (:input-buffer config) default-input-buffer)
         outbuf  (or (:output-buffer config) default-output-buffer)
         opts    (dissoc config :input-buffer :output-buffer :duplex?)
         driver  (make-producer opts ks vs)
         in      (a/chan inbuf)
         out     (a/chan outbuf)
         cb-for  (fn [op n response-ch] ;; return a send callback fn
                   (if response-ch
                     (if (= n 1)
                       (fn [rm e]
                         (a/>!! response-ch (->op-response op rm e))
                         (a/close! response-ch))
                       (let [counter (atom n)]
                         (fn [rm e]
                           (a/>!! response-ch (->op-response op rm e))
                           (when (zero? (swap! counter dec))
                             (a/close! response-ch)))))
                     (fn [rm e] (a/>!! out (->op-response op rm e)))))
         send!   (fn [record cb] ;; send record to kafka with callback
                   (client/send-cb! driver (dissoc record :op :response-ch) cb))]

     (a/thread
       (.setName (Thread/currentThread) (str driver))
       (try
         (loop [{:keys [op timeout callback response-ch topic records]
                 :as   record} (a/<!! in)]
           (if (or (nil? record) (close? op))
             (respond (or op :close) out response-ch (client/close! driver timeout))
             (do
               (try
                 (case op
                   :flush
                   (respond op out response-ch (client/flush! driver))

                   :callback
                   (callback driver)

                   :partitions-for
                   (respond op out response-ch
                            {:op         :partitions-for
                             :type       :partitions
                             :partitions (client/partitions-for driver
                                                                (name topic))})

                   :records
                   (let [n (count records)
                         cb (cb-for :records n response-ch)]
                     (doseq [record records]
                       (send! record cb)))

                   (nil :record)
                   (send! record (cb-for :record 1 response-ch))

                   :init-transactions
                   (respond op out response-ch (client/init-transactions! driver))
                   :begin-transaction
                   (respond op out  response-ch (client/begin-transaction! driver))
                   :commit-transaction
                   (respond op out response-ch (client/commit-transaction! driver))
                   :abort-transaction
                   (respond op out response-ch (client/abort-transaction! driver)))

                 (catch Exception e
                   (respond op response-ch nil e)))

               (recur (a/<!! in)))))
         (catch Exception e
           (a/put! out (->op-response nil nil e)))
         (finally
           (a/put! out eof-response)
           (a/close! out)
           (a/close! in)
           (client/close! driver))))

     (if duplex?
       (duplex in out)
       [in out]))))
