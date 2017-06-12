package com.aiso.spark.common

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by avc on 2016/10/26.
 */
object Helper {

  //默认配置文件读取
  val config: Config = ConfigFactory.load()

  //初始化Spark配置
  val sparkConf = new SparkConf()
    .setIfMissing("spark.master", config.getString("spark.master"))
    .setIfMissing("spark.app.name", config.getString("spark.app.name"))
    .setIfMissing("redis.host", config.getString("redis.host"))
    .setIfMissing("redis.port", config.getString("redis.port"))
    .setIfMissing("es.nodes", config.getString("es.nodes"))
    .setIfMissing("es.port", config.getString("es.port")).setIfMissing("redis.port", config.getString("redis.port"))
    .setIfMissing("mysql.host", config.getString("mysql.host"))
    .setIfMissing("mysql.user", config.getString("mysql.user"))
    .setIfMissing("mysql.password", config.getString("mysql.password"))
    .setIfMissing("mysql.db", config.getString("mysql.db"))
    .setIfMissing("hbase.zookeeper.quorum", config.getString("hbase.zookeeper.quorum"))
    .setIfMissing("spark.cleaner.ttl", "3600")

  def getNewContext():SparkContext = {
    new SparkContext(sparkConf)
  }

  //初始化SparkContext
  val sparkContext: SparkContext = getNewContext()

  //命令行参数解析
  def parseOptions(args: Array[String], index: Int, defaultValue: String): String = if (args.length > index) args(index) else defaultValue

  def mysqlConf = {
    val prop = new java.util.Properties()
    prop.put("user", sparkContext.getConf.get("mysql.user"))
    prop.put("password", sparkContext.getConf.get("mysql.password"))
    prop.put("driver", "com.mysql.jdbc.Driver")
    prop
  }
  def hiveConf = {
    val prop = new java.util.Properties()
    prop.put("user", "")
    prop.put("password", "")
    prop.put("driver", "org.apache.hive.jdbc.HiveDriver")
    prop
  }
}
