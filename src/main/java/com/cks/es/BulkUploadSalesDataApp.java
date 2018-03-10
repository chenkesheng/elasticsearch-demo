package com.cks.es;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.util.Collections;
import java.util.concurrent.ExecutionException;

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
                .put("cluster.name","CollectorDBCluster")
                .build();

        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.30.100"),9300));
        //查询es中的所有index/type/document
        GetIndexResponse response = client.admin().indices().prepareGetIndex().execute().actionGet();
        System.out.println(response.getIndices().length);
        String[] indices = response.getIndices();
        for(String indice : indices){
            System.out.println(indice);
            if (indice.equals(".kibana")){
                continue;
            }
            GetMappingsResponse res = null;
            try {
                res = client.admin().indices().getMappings(new GetMappingsRequest().indices(indice)).get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
            ImmutableOpenMap<String, MappingMetaData> mapping  = res.mappings().get(indice);
            for (ObjectObjectCursor<String, MappingMetaData> c : mapping) {
                System.out.println("type = "+c.key);
                System.out.println("columns = "+c.value.source());
            }
        }


//        BulkRequestBuilder bulkRequest = client.prepareBulk();
//
//        IndexRequestBuilder indexRequestBuilder = client.prepareIndex("car_shop","sales","3")
//                .setSource(XContentFactory.jsonBuilder().startObject()
//                .field("brand","奔驰")
//                .field("name","奔驰c200")
//                .field("price",350000)
//                .field("produce_date","2017-11-20")
//                .field("sale_price",320000)
//                .field("sale_date","2017-11-25")
//                .endObject());
//
//        bulkRequest.add(indexRequestBuilder);
//
//        UpdateRequestBuilder updateRequestBuilder = client.prepareUpdate("car_shop","sales","1")
//                .setDoc(XContentFactory.jsonBuilder().startObject()
//                .field("sale_price",29000)
//                .endObject());
//        bulkRequest.add(updateRequestBuilder);
//
//        DeleteRequestBuilder deleteRequestBuilder = client.prepareDelete("car_shop","sales","2");
//        bulkRequest.add(deleteRequestBuilder);
//
//        BulkResponse bulkResponse = bulkRequest.get();
//
//        bulkResponse.forEach(bulk -> System.out.println(bulk.getVersion()));

    }
}
