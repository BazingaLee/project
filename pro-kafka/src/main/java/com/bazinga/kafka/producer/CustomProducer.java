package com.bazinga.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class CustomProducer {
    public static void main(String[] args) {
        Properties properties = new Properties();
        //kafka地址
        properties.put("bootstrap.servers", "cdh19-47:9092, cdh19-48:9092, cdh19-49:9092");
        //acks=-1
        properties.put("acks", "all");
        properties.put("retries", 0);
        //基于大小的批处理
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        //基于时间的批处理
        properties.put("linger.ms", 1);
        //客户端缓存大小
        properties.put("buffer.memory", 33554432);
        //k v序列化
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i < 9; i++){
            producer.send(new ProducerRecord<String, String>("kafka-demo","" + i, "Hello" + i ));
        }
        //Thread.sleep(1000);
        producer.close(); //忘记close关了,它就是基于批处理的条件( 基于大小的批处理； 基于时间的批处理，看是否达到，没有达到就不会send；)

    }
}
