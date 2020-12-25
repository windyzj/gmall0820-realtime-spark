package com.atguigu.gmall0820.realtime.spark.util

import java.util

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, BulkResult, Index, Search, SearchResult}
import org.elasticsearch.index.query.{BoolQueryBuilder, MatchQueryBuilder, TermQueryBuilder}
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder
import org.elasticsearch.search.sort.SortOrder

object MyEsUtil {
  private var factory: JestClientFactory = null;

  def getClient: JestClient = {
    if (factory == null) build();
    factory.getObject

  }

  def build(): Unit = {
    factory = new JestClientFactory
    factory.setHttpClientConfig(new HttpClientConfig.Builder("http://hdp1:9200")
      .multiThreaded(true)
      .maxTotalConnection(20)
      .connTimeout(10000).readTimeout(1000).build())

  }

  def main(args: Array[String]): Unit = {
    //  saveData()
    queryData()
  }

  def saveData(): Unit = {
    val jest: JestClient = getClient
    //创建一个保存动作
    val index: Index = new Index.Builder(Movie("999", "急先锋"))
      .index("movie_test0820_2020-12-23").`type`("_doc").build()
    jest.execute(index)
    jest.close()
  }

  // batch bulk
  def saveBulkData(dataList: List[(Any, String)], indexName: String): Unit = {
    if (dataList != null && dataList.size > 0) {
      val jest: JestClient = getClient
      val bulkBuilder = new Bulk.Builder()
      for ((data, id) <- dataList) {
        val index: Index = new Index.Builder(data)
          .index(indexName).`type`("_doc").id(id).build()
        bulkBuilder.addAction(index)
      }
      val bulk: Bulk = bulkBuilder.build()
      val result: BulkResult = jest.execute(bulk)
      println("已保存：" + (result.getItems.size() - result.getFailedItems.size()) + "条数据！")
      jest.close()
    }
  }

  //查询
  def queryData(): Unit = {
    val jest: JestClient = getClient
    val query = "{\n  \"query\": {\n     \"bool\": {\n       \"must\": [\n         {\"match\": {\n           \"name\": \"红海\"\n         }}\n       ],\n       \"filter\": {\n         \"term\": {\n           \"actorList.name\": \"张涵予\"\n         }\n       }\n     }\n  },\n  \"highlight\": {\n    \"fields\": {\"name\": {}}\n  }\n  , \"size\": 20\n  ,\"from\": 0\n  , \"sort\": [\n    {\n      \"doubanScore\": {\n        \"order\": \"desc\"\n      }\n    }\n  ]\n}"
    val searchSourceBuilder = new SearchSourceBuilder()
    val boolQueryBuilder = new BoolQueryBuilder()
    boolQueryBuilder.must(new MatchQueryBuilder("name", "红海"))
    boolQueryBuilder.filter(new TermQueryBuilder("actorList.name", "张涵予"))
    searchSourceBuilder.query(boolQueryBuilder)

    searchSourceBuilder.highlighter(new HighlightBuilder().field("name"))

    searchSourceBuilder.size(20)
    searchSourceBuilder.from(0)

    searchSourceBuilder.sort("doubanScore", SortOrder.DESC)

    val searchQuery: String = searchSourceBuilder.toString()
    println(searchQuery)
    val search: Search = new Search.Builder(searchQuery).addIndex("movie_chn0820").build()

    val result: SearchResult = jest.execute(search)
    val rsList: util.List[SearchResult#Hit[util.Map[String, String], Void]] = result.getHits(classOf[util.Map[String, String]])
    //  import  scala.collection.JavaConversions  2.11
    import scala.collection.JavaConverters._ //2.12
    for (rs <- rsList.asScala) { //2.12
      val source: util.Map[String, String] = rs.source
      println(source)
      println(rs.highlight.get("name"))
    }

    jest.close()
  }


  case class Movie(id: String, movie_name: String)

}
