package com.cks.es;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;

/**
 * 场景，一般来说，我们都可以在一些汽车网站上，或者在混合销售多个品牌的汽车4S店的内部，都可以在系统里调出来多个汽车的信息，放在网页上，进行对比
 *
 * @Author: cks
 * @Date: Created by 17:15 2017/12/08
 * @Package: com.cks.es
 * @Description:批量拿出数据
 */
public class MgetMultiCarInfoApp {
    public static void main(String[] args) throws Exception {
        Settings settings = Settings.builder()
                .put("cluster.name", "elasticsearch")
                .put("client.transport.sniff", true)
                .build();

        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));

        MultiGetResponse multiGetItemResponses = client.prepareMultiGet()
                .add("car_shop","cars","1")
                .add("car_shop","cars","2")
                .get();

        multiGetItemResponses.forEach(multiGetItemResponse ->{
            GetResponse getResponse = multiGetItemResponse.getResponse();
            if(getResponse.isExists()){
                System.out.println(getResponse.getSourceAsString());
            }
        });

        client.close();
    }
}
