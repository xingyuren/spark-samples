package com.aiso.spark.streaming

import org.apache.spark._
import org.apache.spark.streaming._

object SparkSteamingContext {

  def main(args: Array[String]) {
    //构建streamingContext
    val conf = new SparkConf().setAppName("SteamingWordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(5))
    //接受的数据地址
    val ds = ssc.socketTextStream("192.168.80.105", 8888)
    //RDD运算
    val result = ds.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    //打印结果
    result.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
