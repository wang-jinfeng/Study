package com.jinfeng.spark.example

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by WangJinfeng on 2017/4/19.
  */
object PairRDD {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Pair RDD").setMaster("local")
    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(List((1, 2), (3, 4), (3, 6)))
    rdd.foreach(println)
    sc.stop()
  }
}