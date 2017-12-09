package com.cks.es;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.script.mustache.SearchTemplateRequestBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;

/**
 * @Author: cks
 * @Date: Created by 19:25 2017/12/9
 * @Package: com.cks.es
 * @Description:分页查询模版
 */
public class SearchTemplatePageQuery {
    public static void main(String[] args) throws Exception {
        Settings settings = Settings.builder()
                .put("cluster.name", "elasticsearch")
                .build();

        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));

        Map<String, Object> params = new HashMap<>();
        SearchResponse searchResponse = new SearchTemplateRequestBuilder(client)
                .setScript("page_query_by_brand")
                .setScriptType(ScriptType.FILE)
                .setScriptParams(params)
                .setRequest(new SearchRequest("car_shop").types("sales"))
                .get()
                .getResponse();

        for (SearchHit searchHit : searchResponse.getHits().getHits()) {
            System.out.println(searchHit);
        }
        client.close();
    }
}
