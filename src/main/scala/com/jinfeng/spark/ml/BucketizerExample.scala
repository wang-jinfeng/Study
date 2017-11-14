package com.jinfeng.spark.ml

import org.apache.spark.ml.feature.Bucketizer
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by WangJinfeng on 2017/5/18.
  */
object BucketizerExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("BucketizerExample").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val splits = Array(Double.NegativeInfinity, -0.5, 0.0, 0.5, Double.PositiveInfinity)
    val data = Array(-0.5, -0.3, 0.0, 0.2)
    val dataFrame = sqlContext.createDataFrame(data.map(Tuple1.apply)).toDF("features")
    val bucketizer = new Bucketizer()
      .setInputCol("features")
      .setOutputCol("bucketedFeatures")
      .setSplits(splits)

    //transform original data into its bucket index
    val bucketizedData = bucketizer.transform(dataFrame)
    bucketizedData.show()
  }
}
