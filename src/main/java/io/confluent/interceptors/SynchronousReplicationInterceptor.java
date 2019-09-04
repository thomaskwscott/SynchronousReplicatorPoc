package io.confluent.interceptors;

import kafka.coordinator.group.BaseKey;
import kafka.coordinator.group.GroupMetadataManager;
import kafka.coordinator.group.GroupTopicPartition;
import kafka.coordinator.group.OffsetKey;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.nio.ByteBuffer;
import java.util.*;

public class SynchronousReplicationInterceptor<K, V> implements ProducerInterceptor<K,V> {

    private String replicatorGroupId;
    private Consumer<byte[],byte[]> offsetsConsumer;
    private Map<String,Long> currentOffsets = new HashMap<String, Long>();


    public ProducerRecord<K, V> onSend(ProducerRecord<K, V> producerRecord) {
        return producerRecord;
    }

    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        //System.out.println("Interceptor fired! Offset sought: " + (recordMetadata.offset()+1));
        if(!currentOffsets.containsKey(recordMetadata.topic() + "-" + recordMetadata.partition())) {
            currentOffsets.put(recordMetadata.topic() + "-" + recordMetadata.partition(),-1l);
        }
        while(recordMetadata.offset()+1 > currentOffsets.get(recordMetadata.topic() + "-" + recordMetadata.partition())) {
            //System.out.println("Comparing: " +  recordMetadata.offset() + " with stored: " + currentOffsets.get(recordMetadata.topic() + "-" + recordMetadata.partition()));
            ConsumerRecords<byte[],byte[]> offsetRecords = offsetsConsumer.poll(100);
            for(ConsumerRecord<byte[],byte[]> record : offsetRecords) {
                BaseKey key = GroupMetadataManager.readMessageKey(ByteBuffer.wrap(record.key()));
                // skip non offset records
                if(key instanceof OffsetKey) {
                    GroupTopicPartition gpt = ((OffsetKey)key).key();
                    if(gpt.group().equals(replicatorGroupId) && record.value() != null) {
                        long offset = GroupMetadataManager.readOffsetMessageValue(ByteBuffer.wrap(record.value())).offset();
                        //System.out.println("Putting: " +  gpt.topicPartition().topic() + "-" + gpt.topicPartition().partition() + "," + offset);
                        currentOffsets.put(gpt.topicPartition().topic() + "-" + gpt.topicPartition().partition(),offset);
                    }

                }

            }

        }
    }

    private byte[] toPrimitive(Byte[] oBytes)
    {

        byte[] bytes = new byte[oBytes.length];
        for(int i = 0; i < oBytes.length; i++){
            bytes[i] = oBytes[i];
        }
        return bytes;

    }

    public void close() {

    }

    public void configure(Map<String, ?> config) {
        replicatorGroupId = config.get("replicator.group.id").toString();

        Properties consumerProperties = new Properties();
        consumerProperties.putAll(config);
        consumerProperties.remove("interceptor.classes");
        consumerProperties.put("key.deserializer", ByteArrayDeserializer.class.getName());
        consumerProperties.put("value.deserializer", ByteArrayDeserializer.class.getName());
        consumerProperties.put("group.id","SynchronousReplicationInterceptor-" + UUID.randomUUID().toString());
        // no point in reading offsets before the producer starts producing
        consumerProperties.put("auto.offset.reset", "latest");
        offsetsConsumer = new KafkaConsumer<byte[],byte[]>(consumerProperties);
        offsetsConsumer.subscribe(Arrays.asList("__consumer_offsets"));
    }
}
