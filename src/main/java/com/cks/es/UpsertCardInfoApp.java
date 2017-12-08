package com.cks.es;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;

/**
 * @Author: cks
 * @Date: Created by 16:58 2017/12/08
 * @Package: com.cks.es
 * @Description:es Java API入门
 */
public class UpsertCardInfoApp {
    public static void main(String[] args) throws Exception{
        Settings settings = Settings.builder()
                .put("cluster.name","elasticsearch")
                .put("client.transport.sniff",true)
                .build();

        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"),9300));

        IndexRequest indexRequest = new IndexRequest("car_shop","cars","1")
                .source(XContentFactory.jsonBuilder().startObject()
                .field("brand","宝马")
                .field("name","宝马320")
                .field("price",320000)
                .field("produce","2017-11-11")
                .endObject());

        UpdateRequest updateRequest = new UpdateRequest("car_shop","cars","1")
                .doc(XContentFactory.jsonBuilder().startObject()
                .field("price",330000)
                .endObject()).upsert(indexRequest);

        UpdateResponse updateResponse = client.update(updateRequest).get();

        System.out.println(updateResponse.getVersion());
        client.close();
    }
}
