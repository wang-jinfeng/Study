package com.jinfeng.spark.examples

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by WangJinfeng on 2017/4/24.
  */
object PartitionExample {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Partition Example").setMaster("local[8]")
    val sc = new SparkContext(conf)
    val n = 2000000
    val composite = sc.parallelize(2 to n, 8).map(x => (x, (2 to (n / x)))).repartition(8).flatMap(kv => kv._2.map(_ * kv._1))
    val prime = sc.parallelize(2 to n, 8).subtract(composite)
    prime.collect().foreach(println)
  }
}
