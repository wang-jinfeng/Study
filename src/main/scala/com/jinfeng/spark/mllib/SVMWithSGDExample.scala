package com.jinfeng.spark.mllib

import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.optimization.L1Updater
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Project : Spark
  * Package : com.jinfeng.spark.mllib
  * Author : WangJinfeng
  * Date : 2017-06-09 16:51
  * Email : jinfeng.wang@yoyi.com.cn
  * Phone : 152-1062-7698
  */
object SVMWithSGDExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SVMWithSGDExample").setMaster("local[10]")
    val sc = new SparkContext(conf)
    //    Load training data in LIBSVM format
    val data = MLUtils.loadLibSVMFile(sc, "src/main/resources/input/sample_libsvm_data.txt")
    //    Split data into training (60%) and test (40%)
    val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)
    //    Run training algorithm to build the model
    val numIteration = 100
    val model = SVMWithSGD.train(training, numIteration)
    //    Clear the default threshold
    model.clearThreshold()
    //    Compute raw scores on the test set
    val scoreAndLabels = test.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }
    //    Get evaluation metrics
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val auROC = metrics.areaUnderROC()
    println("Area under ROC = " + auROC)

    val svmAlg = new SVMWithSGD()
    svmAlg.optimizer.setNumIterations(200)
      .setRegParam(0.1)
      .setUpdater(new L1Updater)
    val modelL1 = svmAlg.run(training)
    println(modelL1.toString())
    //    Save and load model
    //    model.save(sc, "target/tmp/scalaSVMWithSGDModel")
    //    val sameModel = SVMModel.load(sc, "target/tmp/scalaSVMWithSGDModel")
    sc.stop()
  }
}
