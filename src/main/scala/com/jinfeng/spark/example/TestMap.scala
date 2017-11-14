package com.jinfeng.spark.examples

import org.apache.commons.lang.StringUtils
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by WangJinfeng on 2017/7/24.
  */
object TestMap {
  private val CTRL_D = "\004"

  def main(args: Array[String]): Unit = {
//    System.setProperty("hadoop.home.dir", "D:\\Hadoop");
    val conf = new SparkConf().setAppName("TestMap").setMaster("local[10]")
    val sc = new SparkContext(conf)
    val file = sc.textFile("src/main/resources/input/media_dict.txt")
    var mediaMap = sc.broadcast(file.map(line => {
      val mediaId = StringUtils.splitPreserveAllTokens(line, '\t')
      ("" + mediaId(0) + CTRL_D + mediaId(1) + CTRL_D + mediaId(2), mediaId(4) + CTRL_D + mediaId(5) + CTRL_D + mediaId(6) + CTRL_D + mediaId(7))
    }).collectAsMap())
    val adx_id = "7"
    val site_category = "2802"
    //    println(mediaMap.value.get(adx_id + CTRL_D + category).get)
    val is_app = true
    val site_categories = site_category.split(",")
    var media_category_ids = ""
    println(if (is_app) "0" else "1")
    println("mediaMap has key:" + ((if (is_app) "1" else "0") + CTRL_D + adx_id + CTRL_D + site_category) + "===" + mediaMap.value.keySet.contains((if (is_app) "1" else "0") + CTRL_D + adx_id + CTRL_D + site_category))
    println("mediaMap nonEmpty:" + ((if (is_app) "1" else "0") + CTRL_D + adx_id + CTRL_D + site_category) + "===" + mediaMap.value.get((if (is_app) "1" else "0") + CTRL_D + adx_id + CTRL_D + site_category).nonEmpty)
    println("mediaMap isEmpty:" + ((if (is_app) "1" else "0") + CTRL_D + adx_id + CTRL_D + site_category) + "===" + mediaMap.value.get((if (is_app) "1" else "0") + CTRL_D + adx_id + CTRL_D + site_category).isEmpty)
    //    println("mediaMap length:" + ((if (is_app) "1" else "0") + CTRL_D + adx_id + CTRL_D + site_category) + "===" + mediaMap.value.get((if (is_app) "1" else "0") + CTRL_D + adx_id + CTRL_D + site_category))
    for (category <- site_categories) {
      if (mediaMap.value.get((if (is_app) "1" else "0") + CTRL_D + adx_id + CTRL_D + category).nonEmpty &&
        !mediaMap.value((if (is_app) "1" else "0") + CTRL_D + adx_id + CTRL_D + category).isEmpty &&
        mediaMap.value((if (is_app) "1" else "0") + CTRL_D + adx_id + CTRL_D + category).split(CTRL_D).length == 4) {
        val categories = mediaMap.value((if (is_app) "1" else "0") + CTRL_D + adx_id + CTRL_D + category).split(CTRL_D)
        media_category_ids += categories(2) + "_" + categories(0) + ","
        /*
        media_categories += categories(3) + "_" + categories(1) + ","
        media_type_ids += categories(2) + ","
        media_types += categories(3) + ","
        */
      }
    }

    println("media_type_ids = " + media_category_ids)

    //    val app_category = "3899"
    //    if (mediaMap.value.keySet.contains("100000000")) {
    //      println("true")
    //    }
    //    println(mediaMap.value.get(app_category).get)
  }
}
