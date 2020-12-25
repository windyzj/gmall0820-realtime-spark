package com.atguigu.gmall0820.realtime.spark.bootstrap

import java.util

import com.alibaba.fastjson.JSONObject
import com.atguigu.gmall0820.realtime.spark.util.{  MySqlUtil, MykafkaSink }

object DimMysqlToKafka {

  def main(args: Array[String]): Unit = {
    val  dimTables=Array("sku_info","user_info","base_province")
    // 读mysql
    for (tableName <- dimTables ) {
      val dataList: util.List[JSONObject] = MySqlUtil.queryList("select * from "+tableName)
      //整理结构
      val canalJSONObj = new JSONObject()
      canalJSONObj.put("data",dataList)
      canalJSONObj.put("table",tableName)
      canalJSONObj.put("type","INSERT")

      canalJSONObj.put("pkNames",util.Arrays.asList("id"))
      // 写kafka
      println(canalJSONObj.toJSONString)
      MykafkaSink.send("ODS_BASE_DB_C",canalJSONObj.toJSONString)
    }

    MykafkaSink.close

  }

}
