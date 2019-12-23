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
      ctl      ([v] (or v {:op :close})))))

(defn poller-ctl
  [ctl out driver timeout]
  (if-let [payload (get-within ctl timeout)]
    (let [op (:op payload)]
      (case op
        :callback
        (let [f (:callback payload)]
          (or (f driver out)
              (not (:process-result? payload))))

        [:close :stop]
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
               (a/put! (or (:response-ch payload) out)
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
   - `:close`: `{:op :close}` stop and close consumer.
   - `:stop`: `{:op :stop}` same as :close.


   The resulting output channel will emit payloads with as maps containing a
   `:type` key where `:type` may be:

   - `:record`: A consumed record.
   - `:exception`: An exception raised
   - `:rebalance`: A rebalance event.
   - `:eof`: The end of this stream.
   - `:partitions`: The result of a `:partitions-for` operation.
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
  "Build a producer, reading records to send from a channel.

   Arguments mirror those of kinsky.client/producer.
   The config map must be a valid consumer configuration map and may contain
   the following additional keys:

   - `:input-buffer`: Maximum backlog of control channel messages.
   - `:output-buffer`: Maximum queued consumed messages.
   - `:duplex?`: yields a duplex channel instead of a vector of two chans

   Yields a vector of two values `[in out]`, an input channel where operations
   to be performed should be sent, and an output channel where operation
   results are sent.

   The input channel is used to send operations to the producer driver
   and expects map payloads, whose operation is determined by their
   `:op` key.
   An optional response channel can be provided under the `:response-ch` key,
   in which case the operation results are sent to that channel instead of the
   out/duplex channel. Result are sent to the response channel in a blocking
   manner, while results sent to the out/duplex channel are sent in a
   non-blocking manner. Therefore make sure the response channel is consumed.
   The response channel is closed after the operation completes while the
   out/duplex channel remains open until a `:close` operation.

   The following operations are handled:

   - `:record`: `{:op :record :topic \"foo\"}` send a record out, also
      performed when no `:op` key is present.
   - `:records`: `{:op :records :records [...]}` send a collection of records,
      sending responses to an optional `:response` channel entry which will
      be closed after the operation.
   - `:flush`: `{:op :flush}`, flush unsent messages.
   - `:init-transactions`: prepare the producer for transaction operations using
      the `transactional.id` from producer config.
   - `:begin-transaction`: initiate a new transaction.
   - `:commit-transaction`: terminate current transaction.
   - `:abort-transaction`: abort current transaction.
   - `:partitions-for`: `{:op :partitions-for :topic \"foo\"}`, yield partition
      info for the given topic. If a `:response` key is present, produce the
      response there instead of on the record channel.
   - `:close`: `{:op :close}`, close the producer, all channels are closed
     if successful.

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
         send?   (fn [op] ;; return true if op is sending to kafka
                   (or (nil? op)
                       (#{:record :records :send-from} op)))
         afn-for (fn afn-for [op response-ch]
                   ;; return async fn to use for sending response
                   (if response-ch
                     a/>!! ;; producer blocks until response is consumed
                     a/put!))
         ->resp  (fn ->resp [op res e]
                   ;; convert operation result or exception to response map
                   (cond
                      e          (assoc (ex-response e) :op op)
                      (send? op) (assoc res :op    op
                                            :type :record-metadata)
                      :else      {:op op :type :success}))
         cb-for  (fn [op n response-ch] ;; return a send callback fn
                   (let [afn (afn-for op response-ch)]
                     (if response-ch
                       (if (= n 1)
                         (fn [rm e]
                           (afn response-ch (->resp op rm e))
                           (a/close! response-ch))
                         (let [counter (atom n)]
                           (fn [rm e]
                             (afn response-ch (->resp op rm e))
                             (when (zero? (swap! counter dec))
                               (a/close! response-ch)))))
                       (fn [rm e] (afn out (->resp op rm e))))))
         send!   (fn [record cb] ;; send record to kafka with callback
                   (client/send-cb! driver (dissoc record :op :response-ch) cb))
         respond (fn respond
                   ([op response-ch response]
                    (respond op response-ch response nil))
                   ([op response-ch response e]
                    ((afn-for op response-ch)
                     (or response-ch out)
                     (->resp op response e))))]

     (a/thread
       (.setName (Thread/currentThread) (str driver))
       (try
         (loop [{:keys [op timeout callback response-ch topic records]
                 :as   record} (a/<!! in)]
           (if (or (nil? record) (= op :close))
             (do
               (respond :close response-ch (client/close! driver timeout))
               (when response-ch (a/close! response-ch)))
             (do
               (try
                 (case op
                   :flush
                   (client/flush! driver)

                   :callback
                   (callback driver)

                   :partitions-for
                   (respond op response-ch
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
                   (respond op response-ch (client/init-transactions! driver))
                   :begin-transaction
                   (respond op response-ch (client/begin-transaction! driver))
                   :commit-transaction
                   (respond op response-ch (client/commit-transaction! driver))
                   :abort-transaction
                   (respond op response-ch (client/abort-transaction! driver)))

                 (catch Exception e
                   (respond op response-ch nil e)))

               (recur (a/<!! in)))))
         (catch Exception e
           (a/put! out (->resp nil nil e)))
         (finally
           (a/put! out eof-response)
           (a/close! out)
           (a/close! in))))

     (if duplex?
       (duplex in out)
       [in out]))))
