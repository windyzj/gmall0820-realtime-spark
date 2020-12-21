package com.atguigu.gmall0820.realtime.spark.app

import java.lang
import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall0820.realtime.spark.bean.DauInfo
import com.atguigu.gmall0820.realtime.spark.util.{MyEsUtil, MyKafkaUtil, OffsetManagerUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils, OffsetRange}
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
      // 从redis中得到偏移量
      val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic,groupId)

       //  从kafka 接收数据
       //判断offset是否有值 如果无值 去kafka默认偏移量
       var recordInputDstream: InputDStream[ConsumerRecord[String, String]] =null
       if(offsetMap!=null&&offsetMap.size>0){
           recordInputDstream  = MyKafkaUtil.getKafkaStream(topic,ssc,offsetMap,groupId)
       }else{
           recordInputDstream  = MyKafkaUtil.getKafkaStream(topic,ssc,  groupId)
       }

    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]

    val recordInputWithOffsetDstream:  DStream[ConsumerRecord[String, String]] = recordInputDstream.transform {
        rdd =>
        //driver
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        println(offsetRanges(0).untilOffset + "*****")
        rdd
    }


    //  处理   0 数据整理   1筛选 2 去重
       //    recordInputDstream.map(_.value()).print()
     //0 数据整理格式
      val jsonDstream: DStream[JSONObject] = recordInputWithOffsetDstream.map { record =>
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
    dauJsonObjDstream.cache()
     dauJsonObjDstream.print(100)

    val dauInfoDstream: DStream[DauInfo] = dauJsonObjDstream.map(jsonObj => {
      val commonObj: JSONObject = jsonObj.getJSONObject("common")
      DauInfo(commonObj.getString("mid"),
        commonObj.getString("uid"),
        commonObj.getString("ar"),
        commonObj.getString("ch"),
        commonObj.getString("vc"),
        jsonObj.getString("dt"),
        jsonObj.getString("hr"),
        System.currentTimeMillis())
    })


       //  输出数据
    dauInfoDstream.foreachRDD{rdd=>
      rdd.foreachPartition{ dauItr=>
        val dt: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
        //通过增加id  来实现相同mid的幂等性
        val dateList: List[(DauInfo, String)] = dauItr.toList.map(dauInfo=>(dauInfo,dauInfo.mid))
        MyEsUtil.saveBulkData(dateList,"gmall0820_dau_info_"+dt)
        //1executor
      }
      //2  driver
      OffsetManagerUtil.saveOffset(topic,groupId,offsetRanges)

    }


      //启动
      ssc.start()
      ssc.awaitTermination()


  }

}

//string  key  dau:2020-12-18:mid_210 bitmap