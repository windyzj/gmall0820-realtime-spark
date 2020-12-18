package com.atguigu.gmall0820.realtime.spark.app

import java.lang
import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall0820.realtime.spark.util.{MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

object DauApp {

  def main(args: Array[String]): Unit = {
        // 创建sparkstreaming 环境
         val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("dau_app")
         val ssc = new StreamingContext(sparkConf,Seconds(5))
         val groupId="dau_app_group"
         val topic="ODS_BASE_LOG"  //operational data store
       //  从kafka 接收数据
        val recordInputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic,ssc,groupId)
       //  处理   0 数据整理   1筛选 2 去重
       //    recordInputDstream.map(_.value()).print()
     //0 数据整理格式
      val jsonDstream: DStream[JSONObject] = recordInputDstream.map { record =>
      val jsonObject: JSONObject = JSON.parseObject(record.value())
         //整理日期 ， 提取出 日期和小时
         val ts: lang.Long = jsonObject.getLong("ts")
         val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
         val dateTimeStr: String = dateFormat.format(new Date(ts))
         val dateTimeArr: Array[String] = dateTimeStr.split(" ")
         jsonObject.put("dt",dateTimeArr(0))
         jsonObject.put("hr",dateTimeArr(1))
         jsonObject
      }
      // 筛选出 用户首次访问的页面
    val firstVisitDstream: DStream[JSONObject] = jsonDstream.filter(jsonObj => {
      var ifFirst=false
      val pageJson: JSONObject = jsonObj.getJSONObject("page")
      if (pageJson != null) {
        val lastPageId: String = pageJson.getString("last_page_id")
        if (lastPageId == null || lastPageId.length == 0) {
          ifFirst=true
        }
      }
      ifFirst
    })
    firstVisitDstream.print(100)

    // 去重   使用redis进行去重 // 需要优化
    firstVisitDstream.filter{jsonObj=>
      val jedis: Jedis = RedisUtil.getJedisClient
      //  redis  :  type ? set      key    dau:2020-12-18    value ?  mid   expire? 24*3600
      //   去重api :   sadd
      val mid: String = jsonObj.getJSONObject("common").getString("mid")
      val dt: String = jsonObj.getString("dt")
      val dauKey="dau:"+dt
      val nonExists: lang.Long = jedis.sadd(dauKey, mid)  // 返回0 表示 已存在  返回 1表示 未存在
      jedis.expire(dauKey,24*3600)
      jedis.close()
      if(nonExists==1L){
        true
      }else{
        false
      }
    }


    val dauJsonObjDstream: DStream[JSONObject] = firstVisitDstream.mapPartitions { jsonItr =>
      val jedis: Jedis = RedisUtil.getJedisClient
      val sourceList: List[JSONObject] = jsonItr.toList
      val rsList = new ListBuffer[JSONObject]()
      println("未筛选前："+sourceList.size)
      for (jsonObj <- sourceList) {
        val mid: String = jsonObj.getJSONObject("common").getString("mid")
        val dt: String = jsonObj.getString("dt")
        val dauKey = "dau:" + dt
        val nonExists: lang.Long = jedis.sadd(dauKey, mid) // 返回0 表示 已存在  返回 1表示 未存在
        jedis.expire(dauKey, 24 * 3600)
        if (nonExists == 1L) {
          rsList.append(jsonObj)
        }
      }
      jedis.close()
      println("筛选后："+rsList.size)
      rsList.toIterator
    }

    dauJsonObjDstream.print(100)


       //  输出数据
    dauJsonObjDstream


      //启动
      ssc.start()
      ssc.awaitTermination()


  }

}

//string  key  dau:2020-12-18:mid_210 bitmap