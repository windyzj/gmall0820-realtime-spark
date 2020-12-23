package com.atguigu.gmall0820.realtime.spark.app

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.atguigu.gmall0820.realtime.spark.util.{MyKafkaUtil, MykafkaSink, OffsetManagerUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

object BaseDBCanalApp {

  def main(args: Array[String]): Unit = {
    // 创建sparkstreaming 环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("dau_app")
    val ssc = new StreamingContext(sparkConf,Seconds(5))
    val groupId="base_db_canal_app"
    val topic="ODS_BASE_DB_C"  //operational data store

    // 偏移量处理

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

    //得到每个批次偏移量的结束点
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]

    val recordInputWithOffsetDstream:  DStream[ConsumerRecord[String, String]] = recordInputDstream.transform {
      rdd =>
        //driver
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        println(offsetRanges(0).untilOffset + "*****")
        rdd
    }

    val jsonObjDstream: DStream[JSONObject] = recordInputWithOffsetDstream.map(record =>
      JSON.parseObject(record.value())
    )

    val  dimTables=Array("user_info","sku_info","base_province")

    jsonObjDstream.foreachRDD{rdd=>

      rdd.foreachPartition{jsoObjItr=>
        val jedis: Jedis = RedisUtil.getJedisClient
        for (jsonObj <- jsoObjItr ) {
          //每条数据根据 table 和 type  放入不同的kafka topic中
           // val jSONObject: JSONObject = jsonObj.asInstanceOf[JSONObject]
          val table: String = jsonObj.getString("table")
          if(table.equals("base_province")){
            println("!1123123")
          }

          if (dimTables.contains(table)) {

            //维度表处理方式
            println(jsonObj.toJSONString)

            // type?  string   key? pk  value?  json  api? set
            val pks: JSONArray = jsonObj.getJSONArray("pkNames")
            val pkFieldName: String = pks.getString(0)
            val dataArr: JSONArray = jsonObj.getJSONArray("data")
              import scala.collection.JavaConverters._
             for ( data <- dataArr.asScala ) {
               val dataJsonObj: JSONObject = data.asInstanceOf[JSONObject]
               val pkValue: String = dataJsonObj.getString(pkFieldName)
               val key="DIM:"+table.toUpperCase+":"+pkValue
               val value=dataJsonObj.toJSONString
                jedis.set(key,value)
              }
          } else {
          // 事实表处理方式
            val optType: String = jsonObj.getString("type")
            var opt = ""
            if (optType == "INSERT") {
              opt = "I"
            } else if (optType == "UPDATE") {
              opt = "U"
            } else if (optType == "DELETE") {
              opt = "D"
            }
            if (opt.length > 0) {
              val topic = "DWD_" + table.toUpperCase + "_" + opt
              println(jsonObj.toJSONString)
              val dataArr: JSONArray = jsonObj.getJSONArray("data")
              //            import scala.collection.JavaConverters._
              //            for ( data <- dataArr.asScala ) {
              //
              //            }

              for (i <- 0 to dataArr.size() - 1) {
                val dataStr: String = dataArr.getString(i)
                MykafkaSink.send(topic, dataStr)
              }
            }

          }
        }
        jedis.close()
      }

      OffsetManagerUtil.saveOffset(topic,groupId,offsetRanges)

    }

    ssc.start()
    ssc.awaitTermination()



  }

}
