package com.atguigu.gmall0820.publisher.service.impl;

import com.atguigu.gmall0820.publisher.service.OrderService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class OrderServiceImpl implements OrderService {

    @Autowired
    JestClient jestClient;

    @Override
    public Map searchOrderWide(String date, String keyword, int pageNo, int pagesize) {
        String indexName="gmall0820_order_wide_"+date+"-query";

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //分词匹配
        searchSourceBuilder.query(new MatchQueryBuilder("sku_name",keyword).operator(Operator.AND));

        //分页
        searchSourceBuilder.size(pagesize);
        searchSourceBuilder.from((pageNo-1)*pagesize);

        //聚合
        TermsAggregationBuilder ageAggs = AggregationBuilders.terms("groupby_age").field("user_age").size(100);
        searchSourceBuilder.aggregation(ageAggs);

        TermsAggregationBuilder genderAggs = AggregationBuilders.terms("groupby_gender").field("user_gender").size(2);
        searchSourceBuilder.aggregation(genderAggs);

        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(indexName).addType("_doc").build();

        HashMap<String, Object> resultMap = new HashMap<>();
        Long total=0L;
        List<Map> detailList=new ArrayList<>();
         Map<String,Long>  ageAggMap=new HashMap<>();
         Map<String,Long>  genderAggMap=new HashMap<>();

        try {
            SearchResult searchResult = jestClient.execute(search);
            //总数
            total=searchResult.getTotal();
             //明细
            List<SearchResult.Hit<Map, Void>> hits = searchResult.getHits(Map.class);
            for (SearchResult.Hit<Map, Void> hit : hits) {
                detailList.add(hit.source);
            }
//            性别聚合
            List<TermsAggregation.Entry> genderBuckets = searchResult.getAggregations().getTermsAggregation("groupby_gender").getBuckets();
            for (TermsAggregation.Entry genderBucket : genderBuckets) {
                genderAggMap.put(genderBucket.getKey(),genderBucket.getCount());
            }
//            年龄聚合
            List<TermsAggregation.Entry> ageBuckets = searchResult.getAggregations().getTermsAggregation("groupby_age").getBuckets();
            for (TermsAggregation.Entry ageBucket : ageBuckets) {
                ageAggMap.put(ageBucket.getKey(),ageBucket.getCount());
            }


        } catch (IOException e) {
            e.printStackTrace();
        }
        resultMap.put("total",total);
        resultMap.put("detail",detailList);
        resultMap.put("ageAggMap",ageAggMap);
        resultMap.put("genderAggMap",genderAggMap);
        return resultMap;
    }
}
