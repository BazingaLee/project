package com.bazinga.kafka.producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

public class CustomPartitioner implements Partitioner {
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        return 1; //控制分区
    }

    public void close() {

    }

    /**
     * 可以添加属性
     * @param config
     */
    public void configure(Map<String, ?> config) {

    }
}