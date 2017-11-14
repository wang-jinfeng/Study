package com.jinfeng.spark.ml

import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Created by WangJinfeng on 2017/5/17.
  */
object StringIndexerExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DCTExample").setMaster("local[10]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.createDataFrame(
      Seq((0, "a"), (1, "b"), (2, "c"), (3, "a"), (4, "a"), (5, "c"))
    ).toDF("id", "category")
    val indexer = new StringIndexer()
      .setInputCol("category")
      .setOutputCol("categoryIndex")
      .setHandleInvalid("skip")
    val test = sqlContext.createDataFrame(
      Seq((0, "a"), (1, "b"), (2, "c"), (3, "d"))
    ).toDF("id", "category")
    val indexed = indexer.fit(df).transform(test)
    indexed.show()
  }
}
