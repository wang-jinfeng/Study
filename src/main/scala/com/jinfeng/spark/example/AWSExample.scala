package com.jinfeng.spark.example

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by WangJinfeng on 2017/6/9.
  */
object AWSExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("AWS Example").setMaster("local")
    val sc = new SparkContext(conf)

    val event_tkio = "tkio&select event_tkio"
    val event_tk = "tk&select event_tk"
    val event_game = "game&select event_game"
    val it = Iterator(event_tkio, event_tk, event_game)

    while (it.hasNext) {
      val pro = it.next()
      println(pro)
      val output = "s3://reyunbpu/dmp/event/mid/ds=2018-01-03/product=" + pro.split("&")(0) + "/"
      println(output)
    }

  }
}
