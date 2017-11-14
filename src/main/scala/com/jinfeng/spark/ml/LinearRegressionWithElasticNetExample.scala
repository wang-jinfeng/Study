package com.jinfeng.spark.ml

import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by WangJinfeng on 2017/5/23.
  */
object LinearRegressionWithElasticNetExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LinearRegressionWithElasticNetExample").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val dataFrame = sqlContext.read.format("libsvm").load("file:/D:/Program Files/JetBrains/Workspace/Spark/src/main/resources/data/sample_linear_regression_data.txt")
    val lr = new LinearRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)
    //    Fit the model
    val lrModel = lr.fit(dataFrame)
    //    Print the coefficients and intercept for linear regression
    println(s"Coefficients:${lrModel.coefficients} Intercept:${lrModel.intercept}")
    //    Summarize the model over the training set and print out some metrics
    val trainingSummary = lrModel.summary
    println(s"numIteration:${trainingSummary.totalIterations}")
    println(s"objectiveHistory:${trainingSummary.objectiveHistory.toList}")
    trainingSummary.residuals.show()
    println(s"RMSE:${trainingSummary.rootMeanSquaredError}")
    println(s"r2:${trainingSummary.r2}")
  }
}
