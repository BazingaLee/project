package com.bazinga.kafka.Consumer;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.cluster.BrokerEndPoint;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class LowLevelConsumer {
    public static void main(String[] args) {
        //1.集群
        ArrayList<String> list = new ArrayList<>();
        list.add("cdh19-47");
        list.add("cdh19-48");
        list.add("cdh19-49");
        //2.主题
        String topic = "kafka-demo";
        //3.分区
        int partition = 0;
        //4.offset
        long offset = 1;

        //5.获取leader
        String leader = getLeader(list, topic, partition);
        //6.连接leader获取数据
        getData(leader, topic, partition, offset);

    }

    private static void getData(String leader, String topic, int partition, long offset) {
        //1.创建SimpleConsumer
        SimpleConsumer consumer = new SimpleConsumer(leader, 9092, 2000, 1024 * 1024 * 2, "getData");
        //2.发送请求
        //3.构建请求对象FetchRequestBuilder
        FetchRequestBuilder builder = new FetchRequestBuilder();
        FetchRequestBuilder requestBuilder = builder.addFetch(topic, partition, offset, 1024 * 1024);
        FetchRequest fetchRequest = requestBuilder.build();
        //4.获取响应
        FetchResponse fetchResponse = consumer.fetch(fetchRequest);
        //5.解析响应
        ByteBufferMessageSet messageAndOffsets = fetchResponse.messageSet(topic, partition);
        //6.遍历
        for (MessageAndOffset messageAndOffset : messageAndOffsets) {
            long message_offset = messageAndOffset.offset();
            Message message = messageAndOffset.message();
            //7.解析message
            ByteBuffer byteBuffer = message.payload(); //payload是有效负载
            byte[] bytes = new byte[byteBuffer.limit()];
            byteBuffer.get(bytes);
            //8.获取数据
            System.out.println("offset:" + message_offset + "\t" + "value:" + new String(bytes));

        }

    }

    private static String getLeader(ArrayList<String> list, String topic, int partition) {
        //1.循环发送请求，获取leader
        for (String host : list) {
            //2.创建SimpleConsumer对象
            SimpleConsumer consumer = new SimpleConsumer(
                    host,
                    9092,
                    2000,
                    1024*1024,
                    "getLeader"
            );
            //3.发送获取leader请求
            //4.构造请求TopicMetadataRequest
            TopicMetadataRequest request = new TopicMetadataRequest(Arrays.asList(topic));
            //5.获取响应TopicMetadataResponse
            TopicMetadataResponse response = consumer.send(request);
            //6.解析响应
            List<TopicMetadata> topicsMetadata = response.topicsMetadata();
            //7.遍历topicsMetadata
            for (TopicMetadata topicMetadata : topicsMetadata) {
                List<PartitionMetadata> partitionsMetadata = topicMetadata.partitionsMetadata();
                //8.遍历partitionsMetadata
                for (PartitionMetadata partitionMetadata : partitionsMetadata) {
                    //9.判断
                    if (partitionMetadata.partitionId() == partition){
                        BrokerEndPoint endPoint = partitionMetadata.leader();
                        return endPoint.host();

                    }
                }

            }

        }

        return null;
    }
}