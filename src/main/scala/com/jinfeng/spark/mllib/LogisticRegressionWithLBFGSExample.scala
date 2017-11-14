package com.jinfeng.spark.mllib

import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Project : Spark
  * Package : com.jinfeng.spark.mllib
  * Author : WangJinfeng
  * Date : 2017-06-09 18:19
  * Email : jinfeng.wang@yoyi.com.cn
  * Phone : 152-1062-7698
  */
object LogisticRegressionWithLBFGSExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LogisticRegressionWithLBFGSExample").setMaster("local[10]")
    val sc = new SparkContext(conf)
    //    Load training data in LIBSVM format
    val data = MLUtils.loadLibSVMFile(sc, "src/main/resources/input/sample_libsvm_data.txt")
    // Split data into training (60%) and test (40%).
    val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)

    // Run training algorithm to build the model
    val model = new LogisticRegressionWithLBFGS()
      .setNumClasses(10)
      .run(training)

    // Compute raw scores on the test set.
    val predictionAndLabels = test.map { case LabeledPoint(label, features) =>
      val prediction = model.predict(features)
      (prediction, label)
    }

    // Get evaluation metrics.
    val metrics = new MulticlassMetrics(predictionAndLabels)
    val accuracy = metrics.precision
    println(s"Accuracy = $accuracy")

    sc.stop()
  }
}
