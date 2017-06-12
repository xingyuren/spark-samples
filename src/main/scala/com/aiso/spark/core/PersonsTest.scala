package com.aiso.spark.core

import com.aiso.spark.common.Helper
import org.apache.spark.sql.hive.HiveContext

/**
  * TODO 用户标签开发之统计类标签计算
  */
object PersonsTest {
  def main(args: Array[String]) {
    val sc = Helper.sparkContext
    val sqlContext = new HiveContext(sc)

    val (startDate, endDate) = ("2017-03-10", "2017-06-10")
    //TODO 读取用户id数据
    val userIDRDD = sqlContext.table("tb_user_ids")

      .map(row => (row.getAs[Long]("user_id"), None))
    //TODO 订单数据读取
    val orderRDD = sqlContext.table("tb_order")
      .map(row => (
        row.getAs[Long]("user_id"),
        row.getAs[String]("time"),
        row.getAs[Long]("order_id"),
        row.getAs[Int]("order_state"))
      )
      .filter(t => t._2 >= startDate && t._2 <= endDate)
    orderRDD.cache()

    //TODO 用户最近三个月总订单数量
    val userTotalOrderCountRDD = orderRDD.map(t => (t._1, 1))
      .reduceByKey(_ + _)

    //TODO 用户最近三个月的成功订单数量 order state 为1
    val userSuccessOrderCountRDD = orderRDD
      .filter(_._4 == 1)
      .map(t => (t._1, 1))
      .reduceByKey(_ + _)

    //TODO 异常订单数据获取
    val orderDetailRDD = sqlContext.table("tb_order_detail")
      .map(row => (
        row.getAs[Long]("order_id"),
        row.getAs[Int]("product_num"),
        row.getAs[String]("time")
        ))

      .filter(t => t._3 >= startDate && t._3 <= endDate)
      .filter(t => t._2 > 10)
      .map(t => (t._1, None))
      .distinct()

    //TODO 用户最近三个月的异常订单数据计算
    val userAbnormalOrderCountRDD = orderRDD
      .map(t => (t._3, t._1))
      .join(orderDetailRDD)
      .map(t => (t._2._1, 1))

    //TODO 黄牛用户计算（1 表示黄牛 0 表示不是黄牛）
    val scalperRDD = userIDRDD
      .leftOuterJoin(userTotalOrderCountRDD)
      .leftOuterJoin(userSuccessOrderCountRDD)
      .leftOuterJoin(userAbnormalOrderCountRDD)
      .map {
        case (userID, (((_, total), success), abnormal)) => {
          val (v1, v2, v3) = (total.getOrElse(0), success.getOrElse(0),
            abnormal.getOrElse(0)
            )
          if (v1 == 0) (userID, 0)
          else if (v1 > 10 && 1.0 * v2 / v1 < 0.4 && v3 > 3) (userID, 1)
          else (userID, 0)
        }
      }

  }
}
