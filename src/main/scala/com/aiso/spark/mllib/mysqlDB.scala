package com.aiso.spark.mllib

import java.sql.{Connection, DriverManager}

object mysqlDB {
  /**
    * 'host': "192.168.1.201",
    * 'user': "root",
    * 'password': 'new.1234',
    * 'database': "personas",
    * 'charset': 'utf8'
    **/

  //模式匹配
  val url = "jdbc:MySQL://192.168.1.201:3306/personas"

  //所有数据的聚类结果写入数据库[sn,特征,聚类类别ID]
  def cluster_resultToDB(iterator: Iterator[(String, String, String, String, String, String, String, String, String, String, String, String, String, String, Int)]): Unit = {
    var conn: Connection = null
    var ps: java.sql.PreparedStatement = null
    val sql = "insert into device_feature_cluster(sn,stat_date,period,brand,province,price,size,workday_oc_dist,restday_oc_dist,workday_channel_dist,restday_channel_dist,pg_subject_dist,pg_year_dist,pg_region_dist,cluster_id) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
    conn = DriverManager.getConnection(url, "root", "new.1234")
    ps = conn.prepareStatement(sql)
    iterator.foreach(data => {
      ps.setString(1, data._1)
      ps.setString(2, data._2)
      ps.setString(3, data._3)
      ps.setString(4, data._4)
      ps.setString(5, data._5)
      ps.setString(6, data._6)
      ps.setString(7, data._7)
      ps.setString(8, data._8)
      ps.setString(9, data._9)
      ps.setString(10, data._10)
      ps.setString(11, data._11)
      ps.setString(12, data._12)
      ps.setString(13, data._13)
      ps.setString(14, data._14)
      ps.setInt(15, data._15)
      ps.executeUpdate() //执行Sql语句
    }
    )
  }

  //聚类评价指标[聚类个数：聚合度值]
  def index_resultTODB(k: Int, index: Double): Unit = {
    var conn: Connection = null
    var ps: java.sql.PreparedStatement = null
    val sql = "insert into cluster_result_index1(cluster_number,index_value) values (?,?)"
    conn = DriverManager.getConnection(url, "root", "new.1234")
    ps = conn.prepareStatement(sql)
    //TODO 清空表中的数据
//    ps.executeUpdate("TRUNCATE " + "cluster_result_index1")
    ps.setInt(1, k)
    ps.setDouble(2, index)
    ps.executeUpdate() //执行sql语句
  }

  //问卷调查数据写入数据库
  def family_composeTODB(iterator: Iterator[(String, String, String, String)]): Unit = {
    var conn: Connection = null
    var ps: java.sql.PreparedStatement = null
    val sql = "insert into family_compose(sn,member_num,has_child,has_old) values (?,?,?,?)"
    conn = DriverManager.getConnection(url, "root", "new.1234")
    ps = conn.prepareStatement(sql)
    iterator.foreach(data => {
      ps.setString(1, data._1)
      ps.setString(2, data._2)
      ps.setString(3, data._3)
      ps.setString(4, data._4)
      ps.executeUpdate() //执行Sql语句
    }
    )
  }

}