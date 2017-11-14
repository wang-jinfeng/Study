package com.jinfeng.spark.ml

import org.apache.spark.ml.feature.{IndexToString, OneHotEncoder, StringIndexer}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Created by WangJinfeng on 2017/5/17.
  */
object IndexToStringExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("IndexToStringExample").setMaster("local[10]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.createDataFrame(
      Seq((0, "a"), (1, "b"), (2, "c"), (3, "a"), (4, "a"), (5, "c"))
    ).toDF("id", "category")
    val indexer = new StringIndexer()
      .setInputCol("category")
      .setOutputCol("categoryIndex")
      .fit(df)

    val indexed = indexer.transform(df)
    val converter = new IndexToString()
      .setInputCol("categoryIndex")
      .setOutputCol("originalCategory")

    val converted = converter.transform(indexed)
    println("IndexToStringExample")
    converted.select("id", "originalCategory").show()
    //OneHotEncoder maps a column of label indices to a column of binary vectors
    val encoder = new OneHotEncoder()
      .setInputCol("categoryIndex")
      .setOutputCol("categoryVec")
    val encoded = encoder.transform(indexed)
    println("OneHotEncoderExample")
    encoded.select("id", "categoryVec").show()
  }
}
