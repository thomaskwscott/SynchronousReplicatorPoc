## Synchronous Replication Interceptor

### Overview

This project intends to make Replication using Confluent Replicator synchronous using a Producer interceptor. When using this interceptor, the future returned when producing messages to Kafka will not return until the produced message has been replicated to another cluster using Replicator.

### Installation

To install add the built jar to the classpath and add the following properties to your Producer:

```
props.put("interceptor.classes","io.confluent.interceptors.SynchronousReplicationInterceptor");
props.put("replicator.group.id","connect-replicator");
```

Where "connect-replicator" is the name of the consumer group to which replicator offsets are committed

### How it works 

The interceptor receives the producer callback from the broker and this includes the partion/offset of the message produced. It reads the __consumer_offsets topic and blocks until the replicator consumer group has also committed this offset. Since Replicator does not commit the offset until the message has been replicated this can be used as a garuntee that the message is present in the destination cluster.

### Big Caveat

This project intends to prove the concept only and performance is terrible!  