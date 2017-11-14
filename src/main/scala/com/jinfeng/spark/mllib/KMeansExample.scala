package com.jinfeng.spark.mllib

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Project : Spark
  * Package : com.jinfeng.spark.mllib
  * Author : WangJinfeng
  * Date : 2017-06-26 17:30
  * Email : jinfeng.wang@yoyi.com.cn
  * Phone : 152-1062-7698
  */
object KMeansExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("KMeansExample").setMaster("local[10]")
    val sc = new SparkContext(conf)
    val output = "target/org/apache/spark/KMeansExample/KMeansModel"
    FileSystem.get(sc.hadoopConfiguration).delete(new Path(output), true)
    val data = sc.textFile("src/main/resources/input/kmeans_data.txt")
    val parsedData = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble))).cache()
    //    Cluster the data into two classes using KMeans
    val numClusters = 2
    val numIterations = 20
    val clusters = KMeans.train(parsedData, numClusters, numIterations)
    //    Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = clusters.computeCost(parsedData)
    println("Within Set Sum of Squared Errors = " + WSSSE)

    clusters.save(sc, "target/org/apache/spark/KMeansExample/KMeansModel")
    val sameModel = KMeansModel.load(sc, "target/org/apache/spark/KMeansExample/KMeansModel")
    sc.stop()
  }
}
