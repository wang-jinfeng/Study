package com.jinfeng.spark.sql

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Project: Study
  * Package: com.jinfeng.spark.sql
  * Author: wangjf
  * Date: 2018/4/16
  * Email: wangjinfeng@reyun.com
  */

case class SqlCondition(app_cat: String, tid: String)

case class Tag(logic: Boolean, condition: Array[Object])

case class CrowdTag(crowd_id: String, crowd_name: String, tag: Tag)

case class TagDevGame(device_id: String, app_cat: String, tid: String, ds: String, ctype: String)

object UserCrowd {

  //  创建一个栈（Stack),利用栈 先进后出 的特性
  val stack = new mutable.Stack[DataFrame]

  def main(args: Array[String]): Unit = {

    /*
    val jsonString =
      """
        |{"crowd_id":"id_1","crowd_name":"标签","tag":{"logic":true,"condition":[{"app_cat":"020101","tid":"020201"},{"app_cat":"020102"}
        |]}}
      """.stripMargin
      */

    val jsonString =
      """
        |{"crowd_id":"id_1","crowd_name":"标签","tag":{"app_cat":"020101","tid":"020201"}}
      """.stripMargin

    val crowdTag = JSON.parseObject(jsonString, classOf[CrowdTag])

    val conf = new SparkConf().setAppName("UserCrowd_Id==>>" + crowdTag.crowd_id).setMaster("local[10]")
      .set("spark.network.timeout", "600")
      .set("spark.rpc.askTimeout ", "600")
      .set("spark.rpc.lookupTimeout ", "600")
      .set("spark.shuffle.blockTransferService", "nio")

    val spark = SparkSession
      .builder()
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()

    val sc = spark.sparkContext

    import spark.implicits._

    val dataFrame = sc.textFile("src/main/resources/input/tag_dev_game.txt").filter(_.split(",").length == 5).map(_.split(","))
      .filter(e => StringUtils.isNotBlank(e(0)))
      .map(e => TagDevGame(e(0), e(1), e(2), e(3), e(4))).toDF()

    dataFrame.createOrReplaceTempView("user_crowd")

    val output = "src/main/resources/output/id=" + crowdTag.crowd_id
    FileSystem.get(sc.hadoopConfiguration).delete(new Path(output), true)

    val tag = crowdTag.tag
    //  通过递归实现层级解析数据
    parseJson(spark, sc, tag)
    println(stack)
    //  将栈 stack 中的最终生成的 DataFrame pop() 并存储到 output
    stack.pop().rdd.map(k => {
      k.getAs("device_id").toString
    }).distinct().coalesce(1).saveAsTextFile(output)
  }

  /**
    *
    * 递归实现层级解析JSON
    *
    * @param spark SparkSession
    * @param sc    SparkContext
    * @param tag   Tag(logic: Boolean, condition: Array[Object])
    */
  def parseJson(spark: SparkSession, sc: SparkContext, tag: Tag) {

    val logic = tag.logic
    val tags = tag.condition
    for (i <- tags.indices) {
      var sql = ""
      //  create Schema
      val schema = StructType(Array(
        StructField("device_id", DataTypes.StringType)))
      //  create DataFrame
      var df: DataFrame = spark.createDataFrame(sc.emptyRDD[Row], schema)
      //  判断条件数组中是否包含key 'logic',如果包含,则进行递归操作
      if (JSON.parse(tags(i).toString).asInstanceOf[JSONObject].containsKey("logic")) {
        val tagss = JSON.parseObject(tags(i).toString, classOf[Tag])
        parseJson(spark, sc, tagss)
      } else {
        //  条件数组中不包含key 'logic',进行条件封装
        val sqlCondition = JSON.parseObject(tags(i).toString, classOf[SqlCondition])
        if (StringUtils.isNotBlank(sqlCondition.app_cat)) {
          sql = sql + (if (StringUtils.isNotBlank(sql)) {
            " AND "
          } else {
            ""
          }) + "app_cat IN " + sql_con(sqlCondition.app_cat)
        }
        if (StringUtils.isNotBlank(sqlCondition.tid)) {
          sql = sql + (if (StringUtils.isNotBlank(sql)) {
            " AND "
          } else {
            ""
          }) + "tid IN " + sql_con(sqlCondition.tid)
        }
      }
      //  如果sql不为空则执行,并赋值给df
      if (StringUtils.isNotBlank(sql)) {
        df = spark.sql(sql_exec(sql))
        //  将df push进stack栈中
        stack.push(df)
      }
    }

    //  出栈并赋值给top1
    val top1 = stack.pop()
    //  出栈并赋值给top2
    val top2 = stack.pop()

    //  判断两个条件之间的逻辑关系,如果为true,则为 and 操作，两个 DF 进行 intersect 操作
    val combination = if (logic) {
      top1.intersect(top2)
    } else {
      //  判断两个条件之间的逻辑关系,如果为false,则为 or 操作，两个 DF 进行 union 操作
      top1.union(top2)
    }
    //  将两个 DF 的操作结果 push 进栈中
    stack.push(combination)
  }

  //  封装SQL
  def sql_con(_sql: String): String = {
    var param = ""
    val arr = _sql.split(",")
    for (i <- 0 until arr.length) {
      param += "'" + arr(i) + "',"
    }
    "(" + param.substring(0, param.length - 1) + ")"
  }

  //  执行SQL
  def sql_exec(_sql: String): String = {
    "SELECT device_id FROM user_crowd WHERE " + _sql
  }
}