package com.atguigu.gmall0820.realtime.spark.util

import java.util

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.Jedis


object OffsetManagerUtil {

   //要实现写入偏移量
  def saveOffset(topic:String ,group:String  , offsetRanges: Array[OffsetRange]): Unit ={

    val jedis: Jedis = RedisUtil.getJedisClient
    //hash  key : topic+group   field:partition   value: offset
    val offsetKey=topic+":"+group
    val offsetMap = new util.HashMap[String,String]()
    for (offsetRange <- offsetRanges ) {
       val partition: String = offsetRange.partition.toString
       val untilOffset: String = offsetRange.untilOffset.toString
       println("写入偏移量：分区"+partition +":"+offsetRange.fromOffset+"-->"+untilOffset)
       offsetMap.put(partition,untilOffset)
    }
    jedis.hmset(offsetKey,offsetMap)
    jedis.close()
  }


  def getOffset(topic:String ,group:String): Map[TopicPartition,Long]={
      val jedis: Jedis = RedisUtil.getJedisClient
      //hash  key : topic+group   field:partition   value: offset
       val offsetKey=topic+":"+group
    //从redis中查询偏移量
       val offsetMap: util.Map[String, String] = jedis.hgetAll(offsetKey)
       jedis.close()
    // 转换为kafka要求的偏移量结构
      import scala.collection.JavaConverters._
      val topicPartitionMap: Map[TopicPartition, Long] = offsetMap.asScala.map { case (partition, offset) =>
       val topicPartition: TopicPartition = new TopicPartition(topic, partition.toInt)
       (topicPartition, offset.toLong)
       }.toMap
       println("加载偏移量："+topicPartitionMap)
       topicPartitionMap

  }
}
