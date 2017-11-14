package com.jinfeng.spark.ml

import org.apache.spark.ml.feature.QuantileDiscretizer
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by WangJinfeng on 2017/5/19.
  */
object QuantileDiscretizerExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("QuantileDiscretizerExample").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val data = Array((0, 18.0), (1, 19.0), (2, 8.0), (3, 5.0), (4, 2.2))
    val df = sqlContext.createDataFrame(data).toDF("id", "hour")
    val discretizer = new QuantileDiscretizer()
      .setInputCol("hour")
      .setOutputCol("result")
      .setNumBuckets(3)
    val result = discretizer.fit(df).transform(df)
    result.show()
  }
}
