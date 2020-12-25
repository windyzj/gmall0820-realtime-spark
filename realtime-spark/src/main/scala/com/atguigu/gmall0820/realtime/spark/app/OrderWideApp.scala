package com.atguigu.gmall0820.realtime.spark.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall0820.realtime.spark.bean.{OrderDetail, OrderInfo, OrderWide}
import com.atguigu.gmall0820.realtime.spark.util.{MyEsUtil, MyKafkaUtil, OffsetManagerUtil, RedisUtil}
import org.apache.commons.lang3.time.DateUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

object OrderWideApp {

  def main(args: Array[String]): Unit = {
      // 环境配置
      val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("order_wide_app")
      val ssc = new StreamingContext(sparkConf,Seconds(5))
      val groupId="order_wide_group"
      val orderInfoTopic="DWD_ORDER_INFO_I"  //operational data store
      val orderDetailTopic="DWD_ORDER_DETAIL_I"  //operational data store
    //  读取redis中的偏移量
       val orderInfoOffset: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(orderInfoTopic,groupId)
       val orderDetailOffset: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(orderDetailTopic,groupId)

     // 用偏移量加载 kafka 流
     var orderInfoInputDstream: InputDStream[ConsumerRecord[String, String]]=null
    if(orderInfoOffset!=null&&orderInfoOffset.size>0){
      orderInfoInputDstream  = MyKafkaUtil.getKafkaStream(orderInfoTopic,ssc,orderInfoOffset,groupId)
    }else{
      orderInfoInputDstream  = MyKafkaUtil.getKafkaStream(orderInfoTopic,ssc,groupId)
    }

    var orderDetailInputDstream: InputDStream[ConsumerRecord[String, String]]=null
    if(orderDetailOffset!=null&&orderDetailOffset.size>0){
      orderDetailInputDstream  = MyKafkaUtil.getKafkaStream(orderDetailTopic,ssc,orderDetailOffset,groupId)
    }else{
      orderDetailInputDstream  = MyKafkaUtil.getKafkaStream(orderDetailTopic,ssc,groupId)
    }


    //  从kafka流中读取 偏移量结束点
    var orderInfoOffsetRanges: Array[OffsetRange]=null
    val orderInfoOffsetDstream: DStream[ConsumerRecord[String, String]] = orderInfoInputDstream.transform { rdd =>
      orderInfoOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges // d ? ex? d
      rdd
    }
    var orderDetailOffsetRanges: Array[OffsetRange]=null
    val orderDetailOffsetDstream: DStream[ConsumerRecord[String, String]] = orderDetailInputDstream.transform { rdd =>
      orderDetailOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges // d ? ex? d
      rdd
    }

    val orderInfoDstream: DStream[OrderInfo] = orderInfoOffsetDstream.map(record => {
      val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])
      val dateTimeArr: Array[String] = orderInfo.create_time.split(" ")
      orderInfo.create_date = dateTimeArr(0)
      orderInfo.create_hour = dateTimeArr(1).split(":")(0)
      orderInfo
    })
   // orderInfoDstream.print(100)
    val orderDetailDstream: DStream[OrderDetail] = orderDetailOffsetDstream.map(record =>
      JSON.parseObject(record.value(), classOf[OrderDetail])
    )
  //  orderDetailDstream.print(100)


    val orderInfoWithDimDstream: DStream[OrderInfo] = orderInfoDstream.mapPartitions { orderInfoItr =>
      val jedis: Jedis = RedisUtil.getJedisClient
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd") //线程不安全
    val orderInfoList = ListBuffer[OrderInfo]()
      for (orderInfo <- orderInfoItr) {
        // jedis
        //  dim  String get  key ?  DIM:USERINFO:user_id
        val userKey = "DIM:USER_INFO:" + orderInfo.user_id
        val userInfoJson: String = jedis.get(userKey)
        if (userInfoJson != null && userInfoJson.size > 0) {
          val userJSONObj: JSONObject = JSON.parseObject(userInfoJson)
          val birthday: String = userJSONObj.getString("birthday")
          val birthdayDate: Date = dateFormat.parse(birthday)
          val curDateTime: Date = dateFormat.parse(orderInfo.create_time)
          val diffMs: Long = curDateTime.getTime - birthdayDate.getTime
          orderInfo.user_age = (diffMs / 1000L / 3600L / 24L / 365L).toInt
          orderInfo.user_gender = userJSONObj.getString("gender")
        }
        // 地区
        val provinceKey = "DIM:BASE_PROVINCE:" + orderInfo.province_id
        val provinceJson: String = jedis.get(provinceKey)
        if (provinceJson != null && provinceJson.size > 0) {
          val provinceJSONObj: JSONObject = JSON.parseObject(provinceJson)

          orderInfo.province_name = provinceJSONObj.getString("name") //百度suger
          orderInfo.province_area_code = provinceJSONObj.getString("area_code") //阿里datav
          orderInfo.province_iso_code = provinceJSONObj.getString("iso_code") //superset
          orderInfo.province_3166_2_code = provinceJSONObj.getString("iso_3166_2") //es

        }
        orderInfoList.append(orderInfo)
      }
      jedis.close()
      orderInfoList.toIterator
    }
   // orderInfoWithDimDstream.print(100)

    val orderInfoWithKeyDstream: DStream[(Long, OrderInfo)] = orderInfoWithDimDstream.map(orderInfo=>(orderInfo.id,orderInfo))
    val orderDetailWithKeyDstream: DStream[(Long, OrderDetail)] = orderDetailDstream.map(orderDetail=>(orderDetail.order_id,orderDetail))
    val orderjsonDstream: DStream[(Long, (OrderInfo, OrderDetail))] = orderInfoWithKeyDstream.join(orderDetailWithKeyDstream)

    val orderFullJoinDstream: DStream[(Long, (Option[OrderInfo], Option[OrderDetail]))] = orderInfoWithKeyDstream.fullOuterJoin(orderDetailWithKeyDstream)

    //1 orderInfo 存在
    // 1.1 orderDetailOpt 也存在 合并在一起 ==> OrderWide  new OrderWide(orderInfo,orderDetail)
    // 1.2  orderDetailOpt 不存在  不合并
    // 1.3写缓存
    // 1.4 读缓存    合并   ==> OrderWide   new OrderWide(orderInfo,orderDetail)
    // 2 orderInfo 不存在 orderDetailOpt存在
    // 2.1  写缓存
    // 2.2 读缓存      合并   ==> OrderWide   new OrderWide(orderInfo,orderDetail)
    val orderWideDstream: DStream[OrderWide] = orderFullJoinDstream.flatMap { case (orderId, (orderInfoOpt, orderDetailOpt)) =>
      val jedis: Jedis = RedisUtil.getJedisClient
      import scala.collection.JavaConverters._
      val orderWideList: ListBuffer[OrderWide] = ListBuffer[OrderWide]()
      if (orderInfoOpt != None) {
        //1 orderInfo 存在
        val orderInfo: OrderInfo = orderInfoOpt.get
        // 1.1 orderDetailOpt 也存在 合并在一起 ==> OrderWide  new OrderWide(orderInfo,orderDetail)
        // 1.2  orderDetailOpt 不存在  不合并
        if (orderDetailOpt != None) {
          val orderDetail: OrderDetail = orderDetailOpt.get
          orderWideList.append(new OrderWide(orderInfo, orderDetail))
        }
        // 1.3写缓存
        //Jedis   type ?  string     key?  "DWD:ORDER_INFO:"order_id  value ? orderInfoJson  api ?  set get   expire?  10分钟
        val orderInfoKey = "DWD:ORDER_INFO:" + orderInfo.id
        //json4s  --> scala 专用工具
        val orderInfoJson: String = JSON.toJSONString(orderInfo,new SerializeConfig(true))
        jedis.setex(orderInfoKey, 600, orderInfoJson)
        // 1.4 读缓存  合并   ==> OrderWide   new OrderWide(orderInfo,orderDetail)
        val orderDetailKey = "DWD:ORDER_DETAIL:" + orderInfo.id
        val orderDetailSet: util.Set[String] = jedis.smembers(orderDetailKey)
        if (orderDetailSet != null && orderDetailSet.size() > 0) {
          for (orderDetailJson <- orderDetailSet.asScala) {
            val orderDetail: OrderDetail = JSON.parseObject(orderDetailJson, classOf[OrderDetail])
            orderWideList += new OrderWide(orderInfo, orderDetail)
          }
        }

      } else { // 2 orderInfo 不存在 orderDetailOpt存在
        val orderDetail: OrderDetail = orderDetailOpt.get
        // 2.1  写缓存
        //   type?   set     key?  DWD:ORDER_DETAIL:[order_id]  value? orderDetailJsons    api ?  sadd  smembers   expire? 10分钟
        val orderDetailKey = "DWD:ORDER_DETAIL:" + orderDetail.order_id
        val orderDetailJson: String = JSON.toJSONString(orderDetail,new SerializeConfig(true))
        jedis.sadd(orderDetailKey, orderDetailJson)
        jedis.expire(orderDetailKey, 600)
        // 2.2 读缓存      合并   ==> OrderWide   new OrderWide(orderInfo,orderDetail)
        val orderInfoKey = "DWD:ORDER_INFO:" + orderDetail.order_id
        val orderInfoJson: String = jedis.get(orderInfoKey)
        if (orderInfoJson != null && orderInfoJson.length > 0) {
          val orderInfo: OrderInfo = JSON.parseObject(orderInfoJson, classOf[OrderInfo])
          orderWideList += new OrderWide(orderInfo, orderDetail)
        }

      }
      jedis.close()
      orderWideList
    }
    orderWideDstream.cache()
    orderWideDstream.print(1000)

    orderWideDstream.foreachRDD{rdd=>
      rdd.foreachPartition{orderWideItr=>
         val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
         val date: String = dateFormat.format(new Date())
         val orderWideWithIdList: List[(OrderWide, String)] = orderWideItr.toList.map(orderWide=>(orderWide,orderWide.detail_id.toString))
         MyEsUtil.saveBulkData(orderWideWithIdList,"gmall0820_order_wide_"+date)
      }
      OffsetManagerUtil.saveOffset(orderInfoTopic,groupId,orderInfoOffsetRanges)
      OffsetManagerUtil.saveOffset(orderDetailTopic,groupId,orderDetailOffsetRanges)

    }
    //  流格式转换 case class  或 jsonObject
   // orderjsonDstream.print(1000)
    ssc.start()
    ssc.awaitTermination()



  }

}
