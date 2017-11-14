package com.jinfeng.spark.mllib

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.mllib.clustering.{GaussianMixture, GaussianMixtureModel}
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
object GaussianMixtureExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("GaussianMixtureExample").setMaster("local[10]")
    val sc = new SparkContext(conf)
    val output = "target/org/apache/spark/GaussianMixtureExample/GaussianMixtureModel"
    FileSystem.get(sc.hadoopConfiguration).delete(new Path(output), true)
    val data = sc.textFile("src/main/resources/input/gmm_data.txt")
    val parsedData = data.map(s => Vectors.dense(s.trim.split(' ').map(_.toDouble))).cache()
    //    Cluster the data into two classes using GaussianMixture
    val gmm = new GaussianMixture().setK(2).run(parsedData)
    gmm.save(sc, output)
    val sameModel = GaussianMixtureModel.load(sc, output)
    //    output parameters of max-likelihood model
    for (i <- 0 until gmm.k) {
      println("weight=%f\nmu=%s\nsigma=\n%s\n" format(gmm.weights(i), gmm.gaussians(i).mu, gmm.gaussians(i).sigma))
    }
    sc.stop()
  }
}
