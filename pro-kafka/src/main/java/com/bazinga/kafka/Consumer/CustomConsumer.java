package com.bazinga.kafka.Consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

//高级API
public class CustomConsumer  {
    public static void main(String[] args) {
        Properties properties = new Properties();
        //定义kafka集群地址
        properties.put("bootstrap.servers", "cdh19-47:9092, cdh19-48:9092, cdh19-49:9092");
        //消费者组id
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "bazinga");
        //是否自动提交偏移量：(kafka集群管理)
        properties.put("enable.auto.commit", "true");
        //间隔多长时间提交一次offset
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        //key，value的反序列化
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
        kafkaConsumer.subscribe(Arrays.asList("kafka-demo"));  //订阅主题
        while (true){
            ConsumerRecords<String, String> records = kafkaConsumer.poll(100); //定义Consumer, poll拉数据
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("Topic:" + record.topic() + "\t" +
                        "Partition:" + record.partition() + "\t" + "Offset:" +record.offset()
                        + "\t" + "key:" + record.key() + "\t" + "value:" + record.value());
            }
        }
    }
}