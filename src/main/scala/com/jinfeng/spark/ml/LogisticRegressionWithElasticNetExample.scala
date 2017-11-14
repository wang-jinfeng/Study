package com.jinfeng.spark.ml

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Created by WangJinfeng on 2017/5/19.
  */
object LogisticRegressionWithElasticNetExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LogisticRegressionWithElasticNetExample").setMaster("local[10]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val data = sqlContext.read.format("libsvm").load("file:/D:/Program Files/JetBrains/IntelliJ IDEA/Workspaces/Spark/src/main/resources/input/sample_libsvm_data.txt")
    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)

    //Fit the model
    val lrModel = lr.fit(data)
    println(s"Coefficients:${lrModel.coefficients} Intercept:${lrModel.intercept}")
  }
}
