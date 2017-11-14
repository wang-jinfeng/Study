package com.jinfeng.spark.ml

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Created by WangJinfeng on 2017/5/15.
  */
object Word2Vec {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Word2Vec").setMaster("local[10]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    //Input data:Each row is a bag of words from a sentence or document.
    val documentDF = sqlContext.createDataFrame(Seq(
      "Hi I heard about spark".split(" "),
      "I wish Java could use case classes".split(" "),
      "Logistic regression models are neat".split(" ")
    ).map(Tuple1.apply)).toDF("text")

    //Learn a mapping from words to Vectors
    val word2Vec = new org.apache.spark.ml.feature.Word2Vec()
      .setInputCol("text")
      .setOutputCol("result")
      .setVectorSize(3)
      .setMinCount(0)
    val model = word2Vec.fit(documentDF)
    val result = model.transform(documentDF)
    result.select("result").take(3).foreach(println)
  }
}
