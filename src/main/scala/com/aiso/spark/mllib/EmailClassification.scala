package com.aiso.spark.mllib


import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}

//mllib是老的api，里面的模型都是基于RDD的，模型使用的时候api也是有变化的(model这里是naiveBayes)，
//（1：在模型训练的时候是naiveBayes.run(data: RDD[LabeledPoint])来训练的，run之后的返回值是一个NaiveBayesModel对象，就可以使用NaiveBayesModel.predict(testData: RDD[Vector]): RDD[Double] 里面不仅可以传入一个RDD[Vector] ,里面还可以传入单个Vector，得到单个预测值，然后就可以调用save来进行保存了，具体的可以看官方文档API
//(2：模型使用可以参考（1，模型的读取是使用load方法去读的
//
//ml是新的API，ml包里面的模型是基于dataframe操作的
//（1：在模型训练的时候是使用naiveBayes.fit(dataset: Dataset[]): NaiveBayesModel来训练模型的，返回值是一个naiveBayesModel，可以使用naiveBayesModel.transform(dataset: Dataset[]): DataFrame，进行模型的检验，然后再通过其他的方法来评估这个模型，
//（2：模型的使用可以参考（1： 是使用transform来进行预测的，取预测值可以使用select来取值，使用select的时候可以使用“$”label””的形式来取值
//
//训练的时候是使用的NaiveBayes,使用的时候使用naiveBayesModel
//
object EmailClassification {
  def main(args: Array[String]): Unit = {
    val sc = getSparkCont()
    //每一行都以一封邮件
    val spam = sc.textFile("spam.txt");
    val nomal = sc.textFile("normal.txt")

    //创建一个hashingTF实例来吧邮件文本映射为包含10000个特征的向量
    val tf = new HashingTF(10000)
    //把邮件都被分割为单词，每个单词都被映射成一个向量
    val spamFeatures = spam.map { email => tf.transform(email.split(" ")) }
    val nomalFeatures = nomal.map { email => tf.transform(email.split(" ")) }

    //创建LabelPoint 的数据集
    val positiveExamples = spamFeatures.map { feature => LabeledPoint(1, feature) }
    val negativeExamples = nomalFeatures.map { feature => LabeledPoint(1, feature) }
    val trainingData = positiveExamples.union(negativeExamples)

    //使用SGD算法运行逻辑回归 返回的类型是LogisticRegression 但是这个模型是有save，但是没有load方法，我还在思考，读者如果有什么意见或者看法可以下面评论的
    val model = new LogisticRegressionWithSGD().run(trainingData)

    //创建一个邮件向量进行测试
    val posTest = tf.transform("cheap stuff by sending money to ....".split(" "))
    val prediction = model.predict(posTest)
    println(prediction)

  }

  def getSparkCont(): SparkContext = {
    val conf = new SparkConf().setAppName("email").setMaster("local[4]")
    val sc = new SparkContext(conf)
    return sc
  }

}