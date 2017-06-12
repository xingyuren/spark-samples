package com.aiso.spark.mllib

import java.sql.DriverManager

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}

object ClusterEvaluateIndex1 {


  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setMaster("local[1]")
      .setAppName("ClusterEvaluateIndex1")
    val sc = new SparkContext(conf)

    run(sc, "2017-03-15", "15")

    sc.stop()

  }

  def run(sc: SparkContext, analysisDate: String, recentDaysNum: String) = {

    testRun(sc, "2017-03-15", "15", 4)
    testRun(sc, "2017-03-15", "15", 8)
    testRun(sc, "2017-03-15", "15", 12)
    testRun(sc, "2017-03-15", "15", 16)

  }


  def testRun(sc: SparkContext, analysisDate: String, recentDaysNum: String, k: Int) = {


    //TODO 聚类-评估指标1-聚合度

    val initRDD = sc.textFile("/user/hdfs/rsync/uservector/" + analysisDate + "-" + recentDaysNum + "-UserVectorAllETL")
    //////////////test///////////////
    //    val vec = sc.textFile("UserVectorAllETL.txt")
    //////////////test///////////////
    //data training points
    val vec = initRDD.map(line => {
      val cols = line.split("\t")
      //TODO 合并向量字符串
      val sb = new StringBuilder
      for (i <- 3 until cols.length) {
        if (i < (cols.length - 1)) {
          sb.append(cols(i) + ",")
        } else {
          sb.append(cols(i))
        }

      }

      //TODO 转换成大向量
      val vector = Vectors.dense(sb.toString.split(",")
        .map(_.toDouble))

      vector
    })


    //TODO 模型训练
    // 类簇的个数 number of clusters
    //    val k = 8
    // 设置最大迭代次数 maxIterations max number of iterations
    val dataModelTrainTimes = 30
    // 运行3次选出最优解 runs number of parallel runs, defaults to 1. The best model is returned
    val runs = 3
    // 初始聚类中心的选取为k-means++ initializationMode initialization model, either "random" or "k-means||" (default).
    val initMode = "k-means||"

    //TODO 生成模型
    val model_k = KMeans.train(vec, k, dataModelTrainTimes, runs, initMode)

    //TODO 计算集合内方差和，数值越小说明聚类效果越好
    val ssd = model_k.computeCost(vec)
    //评估结果存入数据库
    mysqlDB.index_resultTODB(k, ssd)

    //TODO 聚类中心点打印
    val centers = model_k.clusterCenters
//    centers.foreach(println(_))

    //////////////////////////////////////////////////////////////////////////////////////
    //TODO 保存预测结果

    val label_feature_vec = initRDD.map(line => {

      val cols = line.split("\t")

      var i = 0
      val sn = cols(i)
      i = i + 1
      val stat_date = cols(i)
      i = i + 1
      val period = cols(i)
      i = i + 1

      val brand = cols(i)
      i = i + 1
      val province = cols(i)
      i = i + 1
      val price = cols(i)
      i = i + 1
      val size = cols(i)
      i = i + 1

      val workday_oc_dist = cols(i)
      i = i + 1
      val restday_oc_dist = cols(i)
      i = i + 1
      val workday_channel_dist = cols(i)
      i = i + 1
      val restday_channel_dist = cols(i)
      i = i + 1
      val pg_subject_dist = cols(i)
      i = i + 1
      val pg_year_dist = cols(i)
      i = i + 1
      val pg_region_dist = cols(i)


      //TODO 合并向量字符串
      val sb = new StringBuilder
      for (i <- 3 until cols.length) {
        if (i < (cols.length - 1)) {
          sb.append(cols(i) + ",")
        } else {
          sb.append(cols(i))
        }
      }
      val vectorStr = sb.toString

      //TODO 所有信息+向量合并字符串
      (sn, stat_date, period, brand, province, price, size, workday_oc_dist, restday_oc_dist, workday_channel_dist, restday_channel_dist, pg_subject_dist, pg_year_dist, pg_region_dist, vectorStr)

    })

    //TODO 保存预测结果（所有信息+类别ID)
    val cluster_result = label_feature_vec.map(x => {
      val v = Vectors.dense(x._15.split(",")
        .map(_.toDouble))

      //预测 生成类别ID
      val cluster_id = model_k.predict(v)

      x._1 + "\t" + x._2 + "\t" + x._3 + "\t" + x._4 + "\t" + x._5 + "\t" + x._6 + "\t" + x._7 + "\t" + x._8 + "\t" + x._9 + "\t" + x._10 + "\t" + x._11 + "\t" + x._12 + "\t" + x._13 + "\t" + x._14 + "\t" + cluster_id

    })

      .saveAsTextFile("/user/hdfs/rsync/uservector/" + analysisDate + "-" + recentDaysNum + "-ClusterResult-" + k)


    /////////////////////////////////////////////////////////////////////////////////////
    //TODO 聚类-评估指标2-类别ID与家庭构成类别ID的差异
    val samples =
      sc.textFile("/user/hdfs/rsync/uservector/" + analysisDate + "-" + recentDaysNum + "-FamilyVectorDataExport")
        //////////////test//////////////////////
        //      sc.textFile("2017-03-15-15-FamilyVectorDataExport.txt")
        //////////////test//////////////////////
        .map(line => {
        val arr = line.split("\t")
        val sn = arr(0)

        val member_num = arr(12)

        val has_child = arr(14)
        val has_old = arr(15)

        //TODO 合并向量字符串
        val sb = new StringBuilder
        for (i <- 1 until arr.length - 4)
          if (i < (arr.length - 5)) {
            sb.append(arr(i) + ",")
          } else {
            sb.append(arr(i))
          }

        //var sample_vectorStr = ""
        //          //向量字段
        //          for (i <- 1 until arr.length - 1)
        //            sample_vectorStr = sample_vectorStr + "," + arr(i)
        //
        //          val sample_vector = Vectors.dense(sample_vectorStr.split(",")
        //            .map(_.toDouble))

        //带有sn号和向量的字段，样本向量，样本中的家庭构成
        (sn, sb.toString, member_num, has_child, has_old)
      })

    //样本数据结果
    val sample_label_vec = samples.map(x => {
      val v = Vectors.dense(x._2.split(",")
        .map(_.toDouble))
      //TODO 预测 生成类别ID
      val cluster_id = model_k.predict(v)
      (x._1, cluster_id, x._3, x._4, x._5)
    })

    //TODO 清空指标数据表
//    JdbcUtils.truncate(mysqlDB.url, "sample_cluster_result_k" + k, Helper.mysqlConf)

//    classOf[com.mysql.jdbc.Driver]
//    val conn = DriverManager.getConnection(mysqlDB.url, "root", "new.1234")
//    try {
//      val statement = conn.createStatement()
//      statement.executeUpdate("TRUNCATE " + "sample_cluster_result_k" + k);
//    }
//    catch {
//      case e: Exception => e.printStackTrace
//    }
//    finally {
//      conn.close
//    }


    //TODO 样本数据聚类结果及家庭组成  并写入到Mysql
    sample_label_vec.foreachPartition(items => {
      val conn = DriverManager.getConnection(mysqlDB.url, "root", "new.1234")
      val sql = "insert into sample_allTags_cluster_result_k" + k + "(sn,cluster_id,family_compose, has_child,has_old) values (?,?,?,?,?)"
      val ps = conn.prepareStatement(sql)
      items.foreach(item => {
        ps.setString(1, item._1)
        ps.setInt(2, item._2)
        ps.setString(3, item._3)
        ps.setString(4, item._4)
        ps.setString(5, item._5)
        ps.executeUpdate()
      })
      conn.close()

    })


  }


}
