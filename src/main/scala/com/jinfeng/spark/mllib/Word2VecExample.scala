package com.jinfeng.spark.mllib

import org.apache.spark.mllib.feature.Word2Vec
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * Project : Spark
  * Package : com.jinfeng.spark.mllib
  * Author : WangJinfeng
  * Date : 2017-07-11 10:18
  * Email : jinfeng.wang@yoyi.com.cn
  * Phone : 152-1062-7698
  */
object Word2VecExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Word2VecExample").setMaster("local[10]")
    val sc = new SparkContext(conf)
    //  Load documents (one per line)
    val input = sc.textFile("src/main/resources/input/word2vec.txt")
      .map(line => line.split(" ").toSeq)
    val word2vec = new Word2Vec()
    word2vec.setNumPartitions(10)
    val model = word2vec.fit(input)

    val synonyms = model.findSynonyms("大学", 10)

    for ((synonym, cosineSimilarity) <- synonyms) {
      println(s"$synonym $cosineSimilarity")
    }

    sc.stop()
  }
}
