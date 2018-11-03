Kinsky: Clojure Kafka client library
====================================

[![Build Status](https://secure.travis-ci.org/fmjrey/kinsky-async.svg)](http://travis-ci.org/fmjrey/kinsky-async)

Kinsky is a clojure client library for [Apache Kafka](http://kafka.apache.org)
based on `core.async` and [kinsky](https://github.com/pyr/kinsky).
Abstracting Kafka behind `core.async` channels makes it possible to implement
business logic with `core.async` channels and make kafka dependency a config
option. More precisely an application could be configured to only use channels
and not kafka to enable a faster development workflow, faster tests,
including end-to-end logical testing without the added complexity of kafka,
the latter can be dealt with during integration testing.

The original code of this library was initially part of
[kinsky](https://github.com/pyr/kinsky). It has been extracted into this
separate library when the original `kinsky` authors decided to deprecate
their `core.async` facade which they did not use in production.

This library has been used in prototyping activities, but it's not yet been
battled tested in a production environment. 

## Usage

```clojure
   [[fmjrey/kinsky-async "0.1.0"]]
```

## Documentation

* [API Documentation](http://fmjrey.github.io/kinsky-async)


## Changelog

### 0.1.0

Continuing from [kinsky 0.1.24](https://github.com/pyr/kinsky)
- Producer: return [in out] vector, takes :duplex? config option
  and records with an optional :response channel
- Consumer: added :seek op
- Minor typo and code refactoring

## Examples

The examples assume the following require forms:

```clojure
(:require [kinsky.client      :as client]
          [kinsky-async.async :as async]
          [clojure.core.async :as a :refer [go <! >!]])
```

### Production

```clojure
(let [ch (async/producer {:bootstrap.servers "localhost:9092"} :keyword :edn)]
   (go
     (>! ch {:topic "account" :key :account-a :value {:action :login}})
     (>! ch {:topic "account" :key :account-a :value {:action :logout}})))
```

### Consumption

```clojure
(let [ch     (async/consumer {:bootstrap.servers "localhost:9092"
                              :group.id (str (java.util.UUID/randomUUID))}
                             (client/string-deserializer)
                             (client/string-deserializer))
      topic  "tests"]
						  
  (a/go-loop []
    (when-let [record (a/<! ch)]
      (println (pr-str record))
      (recur)))
  (a/put! ch {:op :partitions-for :topic topic})
  (a/put! ch {:op :subscribe :topic topic})
  (a/put! ch {:op :commit})
  (a/put! ch {:op :pause :topic-partitions [{:topic topic :partition 0}
                                             {:topic topic :partition 1}
                                             {:topic topic :partition 2}
                                             {:topic topic :partition 3}]})
  (a/put! ch {:op :resume :topic-partitions [{:topic topic :partition 0}
                                              {:topic topic :partition 1}
                                              {:topic topic :partition 2}
                                              {:topic topic :partition 3}]})
  (a/put! ch {:op :stop}))
```

### Examples

#### Fusing two topics

```clojure
  (let [popts {:bootstrap.servers "localhost:9092"}
        copts (assoc popts :group.id "consumer-group-id")
        c-ch  (kinsky.async/consumer copts :string :string)
        p-ch  (kinsky.async/producer popts :string :string)]

    (a/go
      ;; fuse topics
	  (a/>! c-ch {:op :subscribe :topic "test1"})
      (let [transit (a/chan 10 (map #(assoc % :topic "test2")))]
        (a/pipe c-ch transit)
        (a/pipe transit p-ch))))
```
