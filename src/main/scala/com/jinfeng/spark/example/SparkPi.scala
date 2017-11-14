package com.jinfeng.spark.examples

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

/**
  * Created by WangJinfeng on 2017/4/19.
  */
object SparkPi {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "D:\\Hadoop");
    /*
    val conf = new SparkConf().setAppName("Spark Pi").setMaster("local")
    val sc = new SparkContext(conf)
    val slices = if (args.length > 0) args(0).toInt else 2
    val n = math.min(100000L * slices, Int.MaxValue).toInt
    val count = sc.parallelize(1 until n, slices).map { i =>
      val x = Random.nextDouble() * 2 - 1
      val y = Random.nextDouble() * 2 - 1
      val z = 2 + 1
      if (x * x + y * y < 1) 1 else 0
    }.reduce(_ + _)
    println("Pi is roughly" + 4.0 * count / n)
    sc.stop()
    */

    val conf = new SparkConf().setAppName("Spark pi").setMaster("local")
    val sc = new SparkContext(conf)
    val file = sc.textFile("src/main/resources/data.txt")
    println("file.key" + file.getStorageLevel)
    val file1 = sc.wholeTextFiles("src/main/resources/input/data.txt")
    println("file1.key" + file1.keys)
    val data = file.map(_.split(",")).map(item => (s"${item(0)}-${item(1)}", item(2)))
    data.collect().foreach(println)
    var rdd = data.groupByKey()
    rdd.collect().foreach(println)
    val result = rdd.map(item => (item._1, item._2.toList.sortWith(_.toInt < _.toInt)))
    result.collect.foreach(item =>
      println(s"${item._1}\t${item._2.mkString(",")}"))

  }
}
