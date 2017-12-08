package com.cks.es;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;

/**
 * 比如说，现在要下载大批量的数据，从es，放到excel中，我们说，月度，或者年度，销售记录，很多，比如几千条，几万条，几十万条
 * 其实就要用到我们之前讲解的es scroll api，对大量数据批量的获取和处理
 *
 * @Author: cks
 * @Date: Created by 17:44 2017/12/08
 * @Package: com.cks.es
 * @Description:分批查询
 */
public class ScrollDownloadSalesDataApp {
    public static void main(String[] args) throws Exception {
        Settings settings = Settings.builder()
                .put("cluster.name", "elasticsearch")
                .build();

        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));

        SearchResponse searchResponse = client.prepareSearch("car_shop")
                .setTypes("sales")
                .setQuery(QueryBuilders.termQuery("brand.keyword", "宝马"))
                .setScroll(new TimeValue(60000))
                .setSize(1)
                .get();

        int batchCount = 0;

        do {
            for (SearchHit searchHit : searchResponse.getHits()) {
                System.out.println("batch: " + ++batchCount);
                System.out.println(searchHit.getSourceAsString());
            }

            searchResponse = client.prepareSearchScroll(searchResponse.getScrollId())
                    .setScroll(new TimeValue(60000))
                    .execute()
                    .actionGet();
        } while (searchResponse.getHits().getHits().length != 0);

        client.close();
    }
}
