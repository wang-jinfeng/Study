package com.jinfeng.spark.mllib

import org.apache.spark.mllib.clustering.BisectingKMeans
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Project : Spark
  * Package : com.jinfeng.spark.mllib
  * Author : WangJinfeng
  * Date : 2017-07-10 17:16
  * Email : jinfeng.wang@yoyi.com.cn
  * Phone : 152-1062-7698
  */
object BisectingKMeansExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("BisectingKMeansExample").setMaster("local[10]")
    val sc = new SparkContext(conf)

    //  Loads and parses data
    def parse(line: String): Vector = Vectors.dense(line.split(" ").map(_.toDouble))

    val data = sc.textFile("src/main/resources/input/kmeans_data.txt").map(parse).cache()

    //  Clustering the data into 6 clusters by BisectingKMeans
    val bkm = new BisectingKMeans().setK(6)
    val model = bkm.run(data)
    //  show the compute cost and the cluster centers
    println(s"Compute Cost:${model.computeCost(data)}")

    model.clusterCenters.zipWithIndex.foreach {
      case (center, idx) =>
        println(s"Cluster Center ${idx}:${center}")
    }

    sc.stop()
  }
}
