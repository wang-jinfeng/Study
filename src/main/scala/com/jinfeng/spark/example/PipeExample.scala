package com.jinfeng.spark.examples

import org.apache.spark.{SparkConf, SparkContext, SparkFiles}

/**
  * Created by WangJinfeng on 2017/4/24.
  */
object PipeExample {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Pipe Example").setMaster("local[5]")
    val sc = new SparkContext(conf)
    val distScript = "file:///D:/R/script/finddistance.R"
    val distScriptName = "finddistance.R"

    val rdd = sc.parallelize(Array(
      "37.75889318222431,-122.42683635321838,37.7614213,-122.4240097",
      "37.7519528,-122.4208689,37.8709087,-122.2688365"))

    sc.addFile(distScript)

    val piped = rdd.pipe(Seq(SparkFiles.get(distScript)),
      Map("SEPARATOR" -> ","))
    val result = piped.collect

    println(result.mkString(" "))
  }
}
