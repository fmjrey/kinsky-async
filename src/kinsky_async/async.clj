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
     clojure.lang.ILookup
     (valAt [this key]
       (.valAt this key nil))
     (valAt [_ key not-found]
       (case key
         [:sink :up]     up
         [:source :down] down
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
      ctl      ([v] v))))

(defn poller-ctl
  [ctl out driver timeout]
  (if-let [payload (get-within ctl timeout)]
    (let [op (:op payload)]
      (case op
        :callback
        (let [f (:callback payload)]
          (or (f driver out)
              (not (:process-result? payload))))

        :stop
        false

        (or
         (case op
           :subscribe
           (client/subscribe! driver
                              (or (:topics payload) (:topic payload))
                              (channel-listener out))

           :unsubscribe
           (client/unsubscribe! driver)

           :seek
           (let [{:keys [offset topic-partition topic-partitions]} payload]
             (if (number? offset)
               (client/seek! driver topic-partition offset)
               (case offset
                 :beginning (client/seek-beginning! driver topic-partitions)
                 :end (client/seek-end! driver topic-partitions))))

           :commit
           (if-let [topic-offsets (:topic-offsets payload)]
             (client/commit! driver topic-offsets)
             (client/commit! driver))

           :pause
           (client/pause! driver (:topic-partitions payload))

           :resume
           (client/resume! driver (:topic-partitions payload))

           :partitions-for
           (do
             (let [parts (client/partitions-for driver (:topic payload))]
               (a/put! (or (:response payload) out)
                       {:type       :partitions
                        :partitions parts}))))
         true)))
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
      (.setName (Thread/currentThread) "kafka-control-poller")
      (loop []
        (when-let [cr (a/<!! c2)]
          (client/wake-up! driver)
          (recur))))
    (a/thread
      (.setName (Thread/currentThread) "kafka-consumer-poller")
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
  "Build an async consumer. Yields a vector of record and control
   channels.

   Arguments config ks and vs work as for kinsky.client/consumer.
   The config map must be a valid consumer configuration map and may contain
   the following additional keys:

   - `:input-buffer`: Maximum backlog of control channel messages.
   - `:output-buffer`: Maximum queued consumed messages.
   - `:timeout`: Poll interval
   - `:topic` : Automatically subscribe to this topic before launching loop
   - `:duplex?`: yields a duplex channel instead of a vector of two chans

   The resulting control channel is used to interact with the consumer driver
   and expects map payloads, whose operation is determined by their
   `:op` key. The following commands are handled:

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
      info for the given topic. If a `:response` key is
      present, produce the response there instead of on
      the record channel.
   - `commit`: `{:op :commit}` commit offsets, an optional `:topic-offsets` key
      may be present for specific offset committing.
   - `:pause`: `{:op :pause}` pause consumption.
   - `:resume`: `{:op :resume}` resume consumption.
   - `:callback`: `{:op :callback :callback (fn [d ch])}` Execute a function
      of 2 arguments, the consumer driver and output channel, on a woken up
      driver.
   - `:stop`: `{:op :stop}` stop and close consumer.


   The resulting output channel will emit payloads with as maps containing a
   `:type` key where `:type` may be:

   - `:record`: A consumed record.
   - `:exception`: An exception raised
   - `:rebalance`: A rebalance event.
   - `:eof`: The end of this stream.
   - `:partitions`: The result of a `:partitions-for` operation.
   - `:woken-up`: A notification that the consumer was woken up.
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
  "Build a producer, reading records to send from a channel.

   Arguments config ks and vs work as for kinsky.client/producer.
   The config map must be a valid consumer configuration map and may contain
   the following additional keys:

   - `:input-buffer`: Maximum backlog of control channel messages.
   - `:output-buffer`: Maximum queued consumed messages.
   - `:duplex?`: yields a duplex channel instead of a vector of two chans

   Yields a vector of two values `[in out]`, an input channel and
   an output channel.

   The resulting input channel is used to interact with the producer driver
   and expects map payloads, whose operation is determined by their
   `:op` key. The following commands are handled:

   - `:record`: `{:op :record :topic \"foo\"}` send a record out, also
      performed when no `:op` key is present. An optional `:response` entry
      should contain a channel which will convey the send result as a map
      containing a `:type` key where `:type` may be:, either:
      - `:exception`: the exception in entry `:exception` was raised
      - `:record-metadata`: a producer record map in the form:
        {:type      :record-metadata
         :topic     \"topic-name\"
         :partition 0 ;; nil if unknown
         :offset    1234567890
         :timestamp 9876543210}
      When a response channel is provided, results are not sent to the
      out/duplex channel, only to the response channel in a blocking manner,
      so make sure the response channel is consumed.
      The response channel will be closed after the operation.
   - `:records`: `{:op :records :records [...]}` send a collection of records,
      sending responses to an optional `:response` channel entry which will
      be closed after the operation.
   - `:flush`: `{:op :flush}`, flush unsent messages.
   - `:partitions-for`: `{:op :partitions-for :topic \"foo\"}`, yield partition
      info for the given topic. If a `:response` key is present, produce the
      response there instead of on the record channel.
   - `:close`: `{:op :close}`, close the producer.

   The resulting output channel will emit payloads as maps containing a
   `:type` key where `:type` may be:

   - `:record-metadata`: The result of a successful send as a map in the form:
      {:topic     \"topic-name\"
       :partition 0 ;; nil if unknown
       :offset    1234567890
       :timestamp 9876543210}
   - `:exception`: An exception raised
   - `:partitions`: The result of a `:partitions-for` operation.
   - `:eof`: The producer is closed.
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
         send?   (fn [op] ;; return true if op is sending to kafka
                   (or (nil? op)
                       (#{:record :records :send-from} op)))
         afn-for (fn afn-for ;; return async fn to use for sending response
                   ([response] (afn-for nil response))
                   ([op response]
                    (if (and response (send? op))
                      a/>!!
                      a/put!)))
         ->resp  (fn ->resp ;; convert record metadata or exception to resp map
                   ([e] (->resp nil e))
                   ([rm e]
                    (cond
                      (and rm (nil? e)) (assoc rm :type :record-metadata)
                      (and e (nil? rm)) (ex-response e)
                      :else (->resp
                              (IllegalArgumentException.
                                "record metadata & exception are nil")))))
         cb-for  (fn [n response] ;; return a callback fn
                   (let [afn (afn-for response)]
                     (if response
                       (if (= n 1)
                         (fn [rm e]
                           (afn response (->resp rm e))
                           (a/close! response))
                         (let [counter (atom n)]
                           (fn [rm e]
                             (afn response (->resp rm e))
                             (when (zero? (swap! counter dec))
                               (a/close! response)))))
                       (fn [rm e] (afn out (->resp rm e))))))
         send!   (fn [record cb] ;; send record to kafka with callback
                   (client/send-cb! driver (dissoc record :op :response) cb))]

     (a/thread
       (try
         (loop [{:keys [op timeout callback response topic records]
                 :as   record} (a/<!! in)]
           (try
             (case op
               :close
               (client/close! driver timeout)

               :flush
               (client/flush! driver)

               :callback
               (callback driver)

               :partitions-for
               (when response
                 (a/put! response
                         {:type       :partitions
                          :partitions (client/partitions-for driver
                                                             (name topic))}))

               :records
               (let [n (count records)
                       cb (cb-for n response)]
                 (doseq [record records]
                   (send! record cb)))

               (nil :record)
               (send! record (cb-for 1 response)))

             (catch Exception e
               ((afn-for op response) (or response out) (->resp e))))

           (when (not= op :close)
             (recur (a/<!! in))))
         (catch Exception e
           (a/put! out (->resp e)))
         (finally
           (a/put! out eof-response)
           (a/close! out))))

     (if duplex?
       (duplex in out)
       [in out]))))
