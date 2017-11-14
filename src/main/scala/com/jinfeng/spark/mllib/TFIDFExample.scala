package com.jinfeng.spark.mllib

import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vector

/**
  * Project : Spark
  * Package : com.jinfeng.spark.mllib
  * Author : WangJinfeng
  * Date : 2017-07-11 9:48
  * Email : jinfeng.wang@yoyi.com.cn
  * Phone : 152-1062-7698
  */
object TFIDFExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TFIDFExample").setMaster("local[10]")
    val sc = new SparkContext(conf)
    //  Load documents (one per line)
    val documents: RDD[Seq[String]] = sc.textFile("src/main/resources/input/kmeans_data.txt")
      .map(_.split(" ").toSeq)

    val hashingTF = new HashingTF()
    val tf: RDD[Vector] = hashingTF.transform(documents)

    //  While applying HashingTF only needs a single pass to the data,applying IDF needs two passes:
    //  First to compute the IDF vector and second to scale the term frequencies by IDF.
    tf.cache()
    val idf = new IDF().fit(tf)
    val tfidf: RDD[Vector] = idf.transform(tf)

    //  spark.mllib IDF implementation provides an option for ignoring terms which occuer in less than
    //  a minimum number of documents.In such cases,the IDF for these terms is set to 0
    //  This feature can be used by passing the  minDocFreq value to IDF constructor
    val idfIgnore = new IDF(minDocFreq = 2).fit(tf)
    val tfidfIgnore: RDD[Vector] = idfIgnore.transform(tf)

    println("tfidf: ")
    tfidf.foreach(x => println(x))

    println("tfidfIgnore: ")
    tfidfIgnore.foreach(x => println(x))

    sc.stop()
  }
}
