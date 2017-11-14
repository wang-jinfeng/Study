package com.jinfeng.spark.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by WangJinfeng on 2017/4/25.
  */
object SparkSQL {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDDRelation").setMaster("local[5]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val input = sqlContext.jsonFile("D:\\Program Files\\JetBrains\\IntelliJ IDEA\\Workspaces\\Spark\\src\\main\\resources\\input\\people.json")
    input.foreach(println)
  }
}
