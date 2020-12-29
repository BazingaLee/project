package com.bazinga.elasticsearch;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

public class InitDemo {

    public static RestHighLevelClient getClient() {

        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(new HttpHost("10.0.221.74", 9200, "http")));
        System.out.println(client);
        return client;
    }

    public static void main(String[] args) {
        getClient();
    }
}
