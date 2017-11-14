package com.jinfeng.spark.mllib

import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by WangJinfeng on 2017/4/25.
  */
object MLlib {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Book Example").setMaster("local[5]")
    val sc = new SparkContext(conf)

    // Load 2 types of emails from text files: spam and ham (non-spam).
    // Each line has text from one email.
    val spam = sc.textFile("src/main/resources/input/spam.txt")
    val ham = sc.textFile("src/main/resources/input/ham.txt")

    //创建一个HashingTF实例来把邮件文本映射为包含100个特征的向量
    val tf = new HashingTF(numFeatures = 100)
    //各邮件都被切分为单词，每个单词被映射为一个特征
    val spamFeatures = spam.map(email => tf.transform(email.split(" ")))
    val hamFeatures = ham.map(email => tf.transform(email.split(" ")))

    //创建LabeledPoint 数据集分别存放阳性（垃圾邮件）和阴性（正常邮件）的例子
    val positiveExamples = spamFeatures.map(features => LabeledPoint(1, features))
    val negativeExamples = hamFeatures.map(features => LabeledPoint(0, features))
    val trainingData = positiveExamples ++ negativeExamples
    trainingData.cache() // 因为逻辑回归是迭代算法，所以缓存训练数据RDD

    //使用SGD算法运行逻辑回归
    val lrLearner = new LogisticRegressionWithSGD()
    // 训练模型
    val model = lrLearner.run(trainingData)

    // 以垃圾邮件和正常邮件为例子进行测试
    val posTestExample = tf.transform("O M G GET cheap stuff by sending money to ...".split(" "))
    val negTestExample = tf.transform("Hi Dad, I started studying Spark the other ...".split(" "))
    // Now use the learned model to predict spam/ham for new emails.
    //使用模型进行测试
    println(s"Prediction for positive test example: ${model.predict(posTestExample)}")
    println(s"Prediction for negative test example: ${model.predict(negTestExample)}")

    sc.stop()
  }
}
