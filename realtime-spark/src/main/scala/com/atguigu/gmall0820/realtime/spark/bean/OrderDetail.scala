package com.atguigu.gmall0820.realtime.spark.bean

case class OrderDetail(
                        id: Long, //订单明细id
                        order_id:Long, //订单id
                        sku_id: Long, //商品id
                        order_price: Double, //订单中的商品单价
                        sku_num:Long, //订单中的商品数量
                        sku_name: String,//订单中的商品名称（冗余)
                        create_time: String,  //创建时间
                        split_total_amount:Double=0D,  //分摊实际支付金额
                        split_activity_amount:Double=0D,//分摊优惠活动减免金额
                        split_coupon_amount:Double=0D  //分摊优惠券减免金额
                      )
