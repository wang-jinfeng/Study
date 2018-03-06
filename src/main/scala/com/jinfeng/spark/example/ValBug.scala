package com.jinfeng.spark.example

import org.apache.commons.lang.StringUtils
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object ValBug {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Map Example").setMaster("local[8]")
    val sc = new SparkContext(conf)

    val data = sc.parallelize(List((1, "www,,,a"), (1, ",iteblog,,b"), (1, ",,com,c"), (2, "bbs,,,a"), (2, ",iteblog," +
      ",b"), (2, ",,com,c"),(3,",,gor,c"))).map(line => (line._1, line._2)).groupByKey().collect()

    val arrayBuffer = data.map(line => {
      val app_cat = new mutable.ArrayBuffer[String]()
      var key = line._1
      var a = ""
      var b = ""
      var c = ""
      val eventSet = mutable.Set("a", "b", "c")
      val lists = line._2
      println("===========")

      lists.filter(_.split(",").size == 4).map(_.split(",")).filter(e => eventSet.contains(e(3))).map(e => {
        a = if (StringUtils.isNotBlank(a)) a else e(0)
        b = if (StringUtils.isNotBlank(b)) b else e(1)
        c = if (StringUtils.isNotBlank(c)) c else e(2)
        println("-------------------")
      })
      lists.filter(_.split(",").size == 4).map(_.split(",")).map(e => {
        println("=========" + key + "," + a + "," + b + "," + c + "," + e(3))
        app_cat += key + "," + a + "," + b + "," + c + "," + e(3)
      })
      app_cat
    })
    println("start========")
    arrayBuffer.foreach(println)
    println("end=======")
  }
}
