package com.aiso.spark.mllib

import java.sql.DriverManager
import com.aiso.spark.common.Helper
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors

object ClusterEvaluateIndex2 {

//  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  def main(args: Array[String]) {
    //    val trainPath = "/user/hdfs/rsync/uservector/2017-03-15-15-UserVectorAllETL"
    //    val testPath = "2017-03-15-15-FamilyVectorDataExport.txt"

    val conf = new SparkConf()
      .setMaster("local[1]")
      .setAppName("SparkTest")
    val sc = new SparkContext(conf)
    run(sc, "2017-03-15", "30")
    sc.stop()

  }

  def run(sc: SparkContext, analysisDate: String, recentDaysNum: String) = {

    testRun(sc, "2017-03-15", "15", 4)
    testRun(sc, "2017-03-15", "15", 8)
    testRun(sc, "2017-03-15", "15", 12)
    testRun(sc, "2017-03-15", "15", 16)

  }

  def testRun(sc: SparkContext, analysisDate: String, recentDaysNum: String, k: Int) = {

    //data training points
    val vec = sc.textFile("/user/hdfs/rsync/uservector/" + analysisDate + "-" + recentDaysNum + "-UserVectorAllETL")
      //////////////test///////////////
      //    val vec = sc.textFile("UserVectorAllETL.txt")
      //////////////test///////////////
      .map(line => {
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

    /////////////////////////////////////////////////////////////////////////////////////////
    //TODO 模型训练
    // 类簇的个数 number of clusters
    //    val k = 8
    // 设置最大迭代次数 maxIterations max number of iterations
    val dataModelTrainTimes = 30
    ///////////test/////////////////////////
    //    val dataModelTrainTimes = 1
    // 运行3次选出最优解 runs number of parallel runs, defaults to 1. The best model is returned
    val runs = 3
    /////////////test//////////////
    //    val runs = 1
    ////////////test///////////////
    // 初始聚类中心的选取为k-means++ initializationMode initialization model, either "random" or "k-means||" (default).
    val initMode = "k-means||"

    //TODO 生成模型
    val model_k = KMeans.train(vec, k, dataModelTrainTimes, runs, initMode)

    //TODO 计算集合内方差和，数值越小说明聚类效果越好
    val ssd = model_k.computeCost(vec)
    //评估结果存入数据库
    //    mysqlDB.index_resultTODB(k, ssd)

    //TODO 聚类中心点打印
    val centers = model_k.clusterCenters
    //    centers.foreach(println(_))



  }
}
