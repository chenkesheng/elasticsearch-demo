package com.cks.es;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;

/**
 * 业务场景：有一个汽车销售公司，拥有很多家4S店，这些4S店的数据，
 * 都会在一段时间内陆续传递过来，汽车的销售数据，现在希望能够在内存中缓存比如1000条销售数据，然后一次性批量上传到es中去
 *
 * @Author: cks
 * @Date: Created by 17:24 2017/12/08
 * @Package: com.cks.es
 * @Description:批量上传数据/删除/修改
 */
public class BulkUploadSalesDataApp {
    public static void main(String[] args) throws Exception{
        Settings settings = Settings.builder()
                .put("cluster.name","elasticsearch")
                .build();

        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"),9300));

        BulkRequestBuilder bulkRequest = client.prepareBulk();

        IndexRequestBuilder indexRequestBuilder = client.prepareIndex("car_shop","sales","3")
                .setSource(XContentFactory.jsonBuilder().startObject()
                .field("brand","奔驰")
                .field("name","奔驰c200")
                .field("price",350000)
                .field("produce_date","2017-11-20")
                .field("sale_price",320000)
                .field("sale_date","2017-11-25")
                .endObject());

        bulkRequest.add(indexRequestBuilder);

        UpdateRequestBuilder updateRequestBuilder = client.prepareUpdate("car_shop","sales","1")
                .setDoc(XContentFactory.jsonBuilder().startObject()
                .field("sale_price",29000)
                .endObject());
        bulkRequest.add(updateRequestBuilder);

        DeleteRequestBuilder deleteRequestBuilder = client.prepareDelete("car_shop","sales","2");
        bulkRequest.add(deleteRequestBuilder);

        BulkResponse bulkResponse = bulkRequest.get();

        bulkResponse.forEach(bulk -> System.out.println(bulk.getVersion()));

    }
}
