package com.aiso.spark.mllib

import com.aiso.spark.common.Helper
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors


/**
  * Created by Administrator on 2017/4/20.
  */
object kmeans {

//  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
//  Logger.getLogger("org.apache.eclipse.jetty.server").setLevel(Level.OFF)


  def main(args: Array[String]) {

    val sc = Helper.sparkContext

    val sourcePath = "/user/hdfs/rsync/uservector/2017-03-15-151493853344253UserVectorAllETL"

    run(sc, sourcePath)

    sc.stop()

  }

  def run(sc: SparkContext, dataPath: String) = {

    val initRDD = sc.textFile(dataPath)

    val vec = initRDD.map(line => {

      val li = line.replaceAll("\"", "")

      val arr = li.split("\t")

      val sn = arr(0)

      var vectorStr = ""

      for (i <- 3 until arr.length)
        vectorStr = vectorStr + "," + arr(i)

      val vector = Vectors.dense(vectorStr.split(",")
        .filter(ele => {
          !ele.isEmpty && !ele.contains(",")
        }).map(_.toDouble))

      (vector)

    })
    //    vec.foreach(println(_))

    val label_feature_vec = initRDD.map(line => {

      val li = line.replaceAll("\"", "")

      val arr = li.split("\t")

      val sn = arr(0)
      val stat_data = arr(1)
      val period = arr(2)
      val brand = arr(3)
      val province = arr(4)
      val price = arr(5)
      val size = arr(6)
      val workday_oc_dist = arr(7)
      val restday_oc_dist = arr(8)
      val workday_channel_dist = arr(9)
      val restday_channel_dist = arr(10)
      val pg_subject_dist = arr(11)
      val pg_yeay_dist = arr(12)
      val pg_region_dist = arr(13)

      var vectorStr = ""
      var featureStr = ""

      for (i <- 3 until arr.length)
        vectorStr = vectorStr + "," + arr(i)
      //      for(i <- 3 until arr.length)
      //        featureStr = featureStr+"\t"+arr(i)

      (sn, stat_data, period, brand, province, price, size, workday_oc_dist, restday_oc_dist, workday_channel_dist, restday_channel_dist, pg_subject_dist, pg_yeay_dist, pg_region_dist, vectorStr)

    })

    /////////////////////////////////////////////////////////////////////////////////////////
    //模型训练
    // 设置簇的个数
    val dataModelNumber = 8
    // 设置最大迭代次数
    val dataModelTrainTimes = 30
    // 运行10次选出最优解
    val runs = 10
    // 初始聚类中心的选取为k-means++
    val initMode = "k-means||"
    val model_k = KMeans.train(vec, dataModelNumber, dataModelTrainTimes, runs, initMode)
    // 计算集合内方差和，数值越小说明同一簇实例之间的距离越小
    val WSSSE = model_k.computeCost(vec)
    println("Within Set Sum of Squared Errors = " + WSSSE)
    //预测样本类别
    val clu_label_vec = label_feature_vec.map(x => {
      val v = Vectors.dense(x._15.split(",")
        .filter(ele => {
          !ele.isEmpty && !ele.contains(",")
        }).map(_.toDouble))

      (x._1, x._2, x._3, x._4, x._5, x._6, x._7, x._8, x._9, x._10, x._11, x._12, x._13, x._14, model_k.predict(v)) //每个样本对应的sn号,特征,样本所属id

    })
    //    clu_label_vec.foreach(println(_))
    clu_label_vec
      .map(x => {
        x._1 + "\t" + x._2 + "\t" + x._3 + "\t" + x._4 + "\t" + x._5 + "\t" + x._6 + "\t" + x._7 + "\t" + x._8 + "\t" + x._9 + "\t" + x._10 + "\t" + x._11 + "\t" + x._12 + "\t" + x._13 + "\t" + x._14 + "\t" + x._15
      })
      .saveAsTextFile("/user/hdfs/rsync/uservector/" + System.currentTimeMillis()
        + "-ClusterResult")

    //          .foreachPartition(cluster_resultToDB)    //预测结果写入数据库
    //    clu_label_vec.foreachPartition(cluster_resultToFS)      //预测结果写入HDFS
    //获取对象类型
    //    println(model_k.clusterCenters.getClass.getName)
    //
    //    println("Cluster centers:")
    //    for(c <- model_k.clusterCenters){
    //      println(c.getClass.getName)
    //      println(" "+c.toString)
    //    }
  }

}

