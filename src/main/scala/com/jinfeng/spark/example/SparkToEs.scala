package com.jinfeng.spark.examples

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.rdd.EsSpark

/**
  * Project : Spark
  * Package : com.jinfeng.spark.examples
  * Author : WangJinfeng
  * Date : 2017-07-31 16:39
  * Email : jinfeng.wang@yoyi.com.cn
  * Phone : 152-1062-7698
  */

case class Trip(departure: String, arrival: String)

object SparkToEs {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkToEs").setMaster("local[10]")
    conf.set("es.nodes", "192.168.70.141")
    conf.set("es.port", "9200")
    conf.set("es.index.auto.create", "true")
    conf.set("es.resource", "spark/docs")
    val sc = new SparkContext(conf)

    val upcomingTrip = Trip("OTP", "SFO")
    val lastWeekTrip = Trip("MUC", "OTP")

    val rdd = sc.makeRDD(Seq(upcomingTrip, lastWeekTrip))
    EsSpark.saveToEs(rdd, "spark/docs")
  }
}
