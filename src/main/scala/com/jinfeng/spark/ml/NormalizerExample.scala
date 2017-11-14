package com.jinfeng.spark.ml

import org.apache.spark.ml.feature.Normalizer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Created by WangJinfeng on 2017/5/17.
  */
object NormalizerExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("NormalizerExample").setMaster("local[10]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val data = sqlContext.read.format("libsvm").load("file:/D:/Program Files/JetBrains/IntelliJ IDEA/Workspaces/Spark/src/main/resources/input/sample_libsvm_data.txt")
    val normalizer = new Normalizer()
      .setInputCol("features")
      .setOutputCol("normFeatures")
      .setP(1.0)

    val l1NormData = normalizer.transform(data)
    l1NormData.show()

    val lInfNormData = normalizer.transform(data, normalizer.p -> Double.PositiveInfinity)
    lInfNormData.show()
  }
}
