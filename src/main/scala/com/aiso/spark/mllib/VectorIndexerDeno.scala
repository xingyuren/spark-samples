package com.aiso.spark.mllib

/**
  * VectorIndexer解决数据集中的类别特征Vector。它可以自动识别哪些特征是类别型的，并且将原始值转换为类别指标。它的处理流程如下：

  * 1.获得一个向量类型的输入以及maxCategories参数。

  * 2.基于原始数值识别哪些特征需要被类别化，其中最多maxCategories需要被类别化。

  * 3.对于每一个类别特征计算0-based类别指标。

  * 4.对类别特征进行索引然后将原始值转换为指标。

  * 索引后的类别特征可以帮助决策树等算法处理类别型特征，并得到较好结果。

  * 在下面的例子中，我们读入一个数据集，然后使用VectorIndexer来决定哪些特征需要被作为非数值类型处理，将非数值型特征转换为他们的索引。

  */

import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object VectorIndexerDeno {
  val conf = new SparkConf()
    .setMaster("local[1]")
    .setAppName("SparkTest")
  val sc = new SparkContext(conf)
  val sqlContext = new HiveContext(sc)
  val data = sqlContext.read.format("libsvm").load("data/mllib/sample_libsvm_data.txt")

  val indexer = new VectorIndexer()
    .setInputCol("features")
    .setOutputCol("indexed")
    .setMaxCategories(10)

  val indexerModel = indexer.fit(data)

  val categoricalFeatures: Set[Int] = indexerModel.categoryMaps.keys.toSet
  println(s"Chose ${categoricalFeatures.size} categorical features: " +
    categoricalFeatures.mkString(", "))

  // Create new column "indexed" with categorical values transformed to indices
  val indexedData = indexerModel.transform(data)
  indexedData.show()
}
