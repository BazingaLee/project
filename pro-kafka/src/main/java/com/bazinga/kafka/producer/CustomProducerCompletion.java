package com.bazinga.kafka.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

public class CustomProducerCompletion {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "cdh19-47:9092, cdh19-48:9092, cdh19-49:9092");
        properties.put("acks", "all");
        properties.put("retries", 2);
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 1);
        properties.put("buffer.memory", 33554432);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //自定义分区 ProducerConfig.PARTITIONER_CLASS_CONFIG
        properties.put("partitioner.class", "com.bazinga.kafka.producer.CustomPartitioner");

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);
        for (int i = 0; i < 9; i++){
            kafkaProducer.send(new ProducerRecord<String, String>("kafka-demo", i+"", "Hi" + i), new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (recordMetadata != null){
                        System.out.println("Topic:" + recordMetadata.topic() + "\t" +
                                "Partition:" + recordMetadata.partition() + "\t" + "offset:" + recordMetadata.offset()
                        );
                    }
                }
            });
        }
        kafkaProducer.close();

    }
}