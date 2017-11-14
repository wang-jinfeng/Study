package com.jinfeng.spark.ml

import breeze.linalg.max
import org.apache.spark.ml.classification.{BinaryLogisticRegressionSummary, LogisticRegression}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by WangJinfeng on 2017/5/19.
  */
object LogisticRegressionSummaryExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LogisticRegressionSummaryExample").setMaster("local[10]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val data = sqlContext.read.format("libsvm").load("file:/D:/Program Files/JetBrains/IntelliJ IDEA/Workspaces/Spark/src/main/resources/input/sample_libsvm_data.txt")
    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)

    //Fit the model
    val lrModel = lr.fit(data)
    //Extract the summary from the returned LogisticRegressionModel instance trained in the earlier example
    val trainingSummary = lrModel.summary
    //obtain the objective per iteration
    val objectiveHistory = trainingSummary.objectiveHistory
    objectiveHistory.foreach(loss => println(loss))
    // Obtain the metrics useful to judge performance on test data.
    // We cast the summary to a BinaryLogisticRegressionSummary since the problem is a
    // binary classification problem.
    val binarySummary = trainingSummary.asInstanceOf[BinaryLogisticRegressionSummary]

    //obtain the receiver-operating characteristic as a dataframe and areaUnderROC
    val roc = binarySummary.roc
    roc.show()
    println(binarySummary.areaUnderROC)
    //Set the model threshold to maximize F-Measuer
    val fMeasure = binarySummary.fMeasureByThreshold
    //    val maxFMeasure = fMeasure.select(max("F-Measure")).head().getDouble(0)
    //    val bestThreshold = fMeasure.where($"F-Measure" === maxFMeasure)
    //      .select("threshold").head().getDouble(0)
    //    lrModel.setThreshold(bestThreshold)
    //    sc.stop()
  }
}
