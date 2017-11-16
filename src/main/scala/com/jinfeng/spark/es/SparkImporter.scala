package com.jinfeng.spark.es

//  import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import org.elasticsearch.spark.rdd.EsSpark

/**
  * Project : Spark
  * Package : com.jinfeng.spark.es
  * Author : WangJinfeng
  * Date : 2017-11-16 10:58
  * Email : wangjinfeng@yiche.com
  * Phone : 152-1062-7698
  */

case class Yiche(dt: String,
                 make_id: Int,
                 serial_id: Int,
                 city_id: Int,
                 sourcetype: Int,
                 page_id: Int,
                 pv_ration: String,
                 pv: Int,
                 uv: Int,
                 duration: Int,
                 pt: String)

object SparkImporter {
  private val CTRL_A = '\001'

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("SparkImporter.2017-11-15").setMaster("local[10]").set("spark.default.parallelism", "1000")
    conf.set("es.nodes", "192.168.70.141")
    conf.set("es.port", "9200")
    conf.set("es.index.auto.create", "true")
    val sc = new SparkContext(conf)

    /*
    val output = "/user/dev/wangjinfeng/thirdparty/uv_insight/" + date
    FileSystem.get(sc.hadoopConfiguration).delete(new Path(output), true)

    val hiveContext = new HiveContext(sc)

    hiveContext.setConf("mapreduce.reduce.shuffle.parallelcopies", "7")
    hiveContext.setConf(" mapreduce.reduce.shuffle.input.buffer.percent", "0.1")
    */

    //  process yiche_pcwap.app_index_page_carmodel_city_day
    val yiche_pcwap = sc.textFile("/Users/wangjf/Downloads/query-hive.csv")
    /*
    val yiche_pcwap = hiveContext.sql("SELECT dt," +
      "make_id," +
      "serial_id," +
      "city_id," +
      "sourcetype," +
      "page_id," +
      "pv_ration," +
      "pv," +
      "uv," +
      "duration," +
      "pt " +
      "FROM yiche_pcwap.app_index_page_carmodel_city_day " +
      "WHERE pt = '2017-11-15'"
    )

    val yiche = yiche_pcwap.flatMap(row => {
      var pcwap = new mutable.ArrayBuffer[String]
      val dt = row.getAs("dt")
      val serial_id = row.getAs("serial_id")
      val city_id = row.getAs("city_id")
      val sourcetype = row.getAs("sourcetype")
      val page_id = row.getAs("page_id")
      val pv_ration = row.getAs("pv_ration")
      val pv = row.getAs("pv")
      val uv = row.getAs("uv")
      val duration = row.getAs("duration")
      val pt = row.getAs("pt")
      pcwap += "" + dt + CTRL_A + serial_id + CTRL_A + city_id + CTRL_A + sourcetype +
        CTRL_A + page_id + CTRL_A + pv_ration + CTRL_A + pv + CTRL_A + uv + CTRL_A + duration + CTRL_A + pt
      pcwap
    }).cache()
    */
    // import pv to es
    val yiche_es = yiche_pcwap.map(_.split(","))
      .map(yiche => Yiche(yiche(0), yiche(1).toInt, yiche(2).toInt, yiche(3).toInt, yiche(4).toInt, yiche(5).toInt, yiche(6),
        yiche(7).toInt, yiche(8).toInt, yiche(9).toInt, yiche(10)))

    EsSpark.saveToEs(yiche_es, "yiche_2017-11-15/test")
  }
}