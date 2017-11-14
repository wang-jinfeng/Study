package com.jinfeng.spark.ml

import org.apache.spark.ml.feature.Binarizer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Created by WangJinfeng on 2017/5/16.
  */
object BinarizerExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("BinarizerExample").setMaster("local[10]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val data = Array((0, 0.1), (1, 0.8), (2, 0.2))
    val dataFrame: DataFrame = sqlContext.createDataFrame(data).toDF("label", "feature")

    val binarizer: Binarizer = new Binarizer()
      .setInputCol("feature")
      .setOutputCol("binarized_feature")
      .setThreshold(0.5)
    val binarizedDataFrame = binarizer.transform(dataFrame)
    val binarizedFeature = binarizedDataFrame.select("binarized_feature")
    binarizedFeature.collect().foreach(println)
  }
}
