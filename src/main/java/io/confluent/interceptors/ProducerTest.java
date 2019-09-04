package io.confluent.interceptors;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Arrays;
import java.util.Properties;

public class ProducerTest {

    public static void main(String[] args) {
        Properties props = new Properties();


        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:10091");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put("interceptor.classes","io.confluent.interceptors.SynchronousReplicationInterceptor");
        props.put("replicator.group.id","connect-replicator");
        props.put("max.in.flight.requests.per.connection","1");
        props.put("batch.size","1");
        props.put("linger.ms","0");

        KafkaProducer<byte[], byte[]> producer = new KafkaProducer<byte[], byte[]>(props);

        byte[] payload = new byte[100];
        Arrays.fill(payload, (byte) 1);
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<byte[], byte[]>("wikipedia.parsed", payload);

        long startTime = System.currentTimeMillis();
        for (int i = 0; i < 3; i++) {
            try {
                System.out.println("About to send record " + i);
                producer.send(record).get();
                System.out.println("Sent record " + i);
            } catch (Exception e){
                System.out.println("Error producing to topic wikipedia.parsed");
                e.printStackTrace();
            }
        }
        producer.close();
        long endTime = System.currentTimeMillis();
        long durationMs = endTime - startTime;
        float durationS = durationMs / 1000;
        float rate = 3 / durationS;

        System.out.println("Sent " + 3 + " records in " + durationS + " seconds (" +durationMs + " milliseconds), total of " + rate + " events per second");
    }
}

