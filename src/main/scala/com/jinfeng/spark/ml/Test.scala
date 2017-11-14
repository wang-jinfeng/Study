package com.jinfeng.spark.ml

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by WangJinfeng on 2017/5/19.
  */
object Test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Test").setMaster("local[10]")
    val sc = new SparkContext(conf)
    val file = sc.textFile("src/main/resources/input/data.txt")
    println("file.key" + file.first())
  }
}
