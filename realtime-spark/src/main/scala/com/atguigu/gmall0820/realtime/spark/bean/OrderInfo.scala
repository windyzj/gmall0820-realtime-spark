package com.atguigu.gmall0820.realtime.spark.bean

case class OrderInfo(
                      id: Long =0L,   // 主键
                      province_id: Long=0L, //省市id
                      order_status: String=null, //订单状态 未支付 已支付 发货中 ...
                      user_id: Long=0L, //用户id
                      total_amount:  Double=0D, //订单金额（ 支付金额)
                      activity_reduce_amount: Double=0D, // 优惠活动的减免金额
                      coupon_reduce_amount: Double=0D,  // 优惠券的减免金额
                      original_total_amount: Double=0D,  // 订单原始金额
                      feight_fee: Double=0D,    // 运费
                      feight_fee_reduce: Double=0D,   // 运费减免
                      expire_time: String =null,      // 过期时间
                      refundable_time:String =null,   // 可退款时间
                      create_time: String=null,       // 创建时间
                      operate_time: String=null,       // 修改时间
                      var create_date: String=null,    // 创建日期
                      var create_hour: String=null,    // 创建小时

                      //查询维表得到
                      var province_name:String=null,  //地区名称
                      var province_area_code:String=null,  //地区编码 (中国行政区位码)
                      var province_3166_2_code:String=null,//地区编码 (国际码 3166-2)
                      var province_iso_code:String=null,  //地区编码 (国际码 旧码)

                      var user_age :Int=0,  //用户年龄
                      var user_gender:String=null //用户性别
                    ){


}