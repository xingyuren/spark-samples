package com.aiso.spark.mllib

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}

object ClusterEvaluateIndex3 {

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

    //TODO 小时交叉维度与之前的大向量合并





  }
}
