package com.aiso.spark.mllib

import com.aiso.spark.common.Helper
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.{StandardScaler, VectorAssembler}
import org.apache.spark.sql.hive.HiveContext

/**
  * TODO 用户标签开发之统计类标签计算
  */
object PersonsTestCluster {
  def main(args: Array[String]) {
    val sc = Helper.sparkContext
    val sqlContext = new HiveContext(sc)
    //TODO 读取数据形成DataFrame
    val df = sqlContext.table("tb_order")
    df.cache()


    //TODO 模型构建
    val vector = new VectorAssembler()
      .setInputCols(Array("featureNames"))
      .setOutputCol("f1")

    val scaler = new StandardScaler()
      .setInputCol("f1")
      .setOutputCol("features")

    val clusterNums = 4
    val kmeans = new KMeans()
      .setK(clusterNums)
      .setFeaturesCol("features")
      .setPredictionCol("prediction")

    val pipline = new Pipeline()

    pipline.setStages(Array(vector, scaler, kmeans))

    val model = pipline.fit(df)

    //TODO 预测 resultDataFrame 中存在一个列叫做prediction 表示聚类类别
    val resultDataFrame = model.transform(df)


//    val dfString = sqlContext.createDataFrame(Seq(("a1", "b1", "c1"), ("a2", "b2", "c2"))).toDF("colx", "coly", "colz")
//    val dfDouble = sqlContext.createDataFrame(Seq((0.0, 1.0, 2.0), (3.0, 4.0, 5.0))).toDF("colx", "coly", "colz")
//
    val va = new VectorAssembler().setInputCols(Array("colx", "coly", "colz")).setOutputCol("ft")
    val pipeline = new Pipeline().setStages(Array(va))

  }
}

