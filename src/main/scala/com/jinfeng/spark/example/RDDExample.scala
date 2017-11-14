package com.jinfeng.spark.examples

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by WangJinfeng on 2017/6/9.
  */
object RDDExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDD Example").setMaster("local")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(1 to 9, 3)
    val rdd2 = rdd1.map(x => x * 2)
    rdd2.collect().foreach(println)
  }
}
