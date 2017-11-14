package com.jinfeng.spark.ml

import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Created by WangJinfeng on 2017/5/15.
  */
object StopWordsRemoverExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("StopWordsRemoverExample").setMaster("local[10]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val remover = new StopWordsRemover()
      .setInputCol("raw")
      .setOutputCol("filtered")

    val dataSet = sqlContext.createDataFrame(Seq(
      (0, Seq("I", "saw", "the", "red", "baloon")),
      (1, Seq("Mary", "had", "a", "little", "lamb"))
    )).toDF("id", "raw")

    remover.transform(dataSet).show()
  }
}
