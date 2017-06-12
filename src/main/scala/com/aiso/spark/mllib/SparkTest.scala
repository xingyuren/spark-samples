package com.aiso.spark.mllib

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkContext, SparkConf}

object SparkTest {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setMaster("local[1]")
      .setAppName("SparkTest")
    val sc = new SparkContext(conf)
//    run(sc, "2017-01-04", "30")

    println(sc)
    sc.stop()
  }

  def run(sc: SparkContext, analysisDate: String, recentDaysNum: String) = {

    //    val dataPath = "hdfs://192.168.1.15:8020/user/hdfs/rsync/uservector/2017-05-04-ClusterResult"

    //    sc.textFile(dataPath).foreach(println(_))

    //    val wo_default = UserVectorHelper.getCntDistStr("0", UserVectorConstant.BH_OC_HOUR_ARR, "0")
    //    println(None.getOrElse(wo_default))
    //
    //    UserVectorHelper.getCntDistStr("0", UserVectorConstant.BH_OC_HOUR_ARR, "0").toVector

    //    val tmpTerminalCnt_one = sc.textFile("E:\\aowei\\tracker-user\\doc\\user-tmp-sn.txt")
    //    val tmpTerminalCnt_two = sc.textFile("E:\\aowei\\tracker-user\\doc\\user-tmp-sn.txt").distinct
    //
    //    println(tmpTerminalCnt_one.distinct.count)
    //    val cnt = tmpTerminalCnt_one.intersection(tmpTerminalCnt_one).count
    //    println(cnt)
    //
    //    val sqlContext = new HiveContext(sc)
    //
    //    sqlContext.sql("select distinct sn from hr.user_vector_terminal").rdd.map(_(0))


    val samples =
      sc.textFile("2017-03-15-15-FamilyVectorDataExport.txt")
        .map(line => {
          val arr = line.split("\t")
          val sn = arr(0)

          val member_num = arr(12)

          //TODO 合并向量字符串
          val sb = new StringBuilder
          for (i <- 1 until arr.length - 1){
            if (i < (arr.length - 2)) {
              sb.append(arr(i) + ",")
            } else {
              sb.append(arr(i))
            }
          }

          //var sample_vectorStr = ""
          //          //向量字段
          //          for (i <- 1 until arr.length - 1)
          //            sample_vectorStr = sample_vectorStr + "," + arr(i)
          //
          //          val sample_vector = Vectors.dense(sample_vectorStr.split(",")
          //            .map(_.toDouble))

          //带有sn号和向量的字段，样本向量，样本中的家庭构成
          (sn, sb.toString, member_num)
        })
    //样本数据结果
    val sample_label_vec = samples.map(x => {
      val vector = Vectors.dense(x._2.split(",")
        .map(_.toDouble))
//      x._2.split(",")        .map(_.toDouble).foreach(println(_))
//        println(x._2)

      vector
    })

    //TODO 模型训练
    // 类簇的个数 number of clusters
    //    val k = 8
    // 设置最大迭代次数 maxIterations max number of iterations
    //    val dataModelTrainTimes = 30
    val dataModelTrainTimes = 1
    // 运行3次选出最优解 runs number of parallel runs, defaults to 1. The best model is returned
    //    val runs = 3
    val runs = 1
    // 初始聚类中心的选取为k-means++ initializationMode initialization model, either "random" or "k-means||" (default).
    val initMode = "k-means||"

    //TODO 生成模型
    val model_k = KMeans.train(sample_label_vec, 1, dataModelTrainTimes, runs, initMode)


    samples.map(x => {
      val v = Vectors.dense(x._2.split(",")
        .map(_.toDouble))
      //TODO 预测 生成类别ID
      val cluster_id = model_k.predict(v)
      (x._1, cluster_id, x._3)
    })

      .foreach(println(_))


  }

}
