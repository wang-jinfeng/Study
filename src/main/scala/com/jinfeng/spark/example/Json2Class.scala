package com.jinfeng.spark.example

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.commons.lang.StringUtils

import scala.collection.mutable

/**
  * Project: Study
  * Package: com.jinfeng.spark.example
  * Author: wangjf
  * Date: 2018/4/13
  * Email: wangjinfeng@reyun.com
  */

case class SqlCondition(app_cat: String, tid: String)

//  case class Condition(array: Array[SubCondition])

//  case class RootCondition(condition: Condition, subCondition: SubCondition)

case class Tag(logic: Boolean, condition: Array[Object])

case class CrowdTag(crowd_id: String, crowd_name: String, tag: Tag)

object Json2Class {
  def main(args: Array[String]): Unit = {
    /*
    val jsonString =
      """
        |{"crowd_id":"id_1","crowd_name":"标签","tag":{"logic":false,"condition":[
        |{"logic":true,"condition":[{"app_cat":"020102"},{"app_cat":"020103"}]},{"tid":"020301"}
        |]}}
      """.stripMargin

    val jsonString =
      """
        |{"crowd_id":"id_1","crowd_name":"标签","tag":{"logic":true,"condition":[
        |{"logic":false,"condition":[{"logic":true,"condition":[{"logic":true,"condition":[{"app_cat":"020102"},{"app_cat":"020103"}]},{"tid":"020201"}]},{"app_cat":"020109","tid":"020110"}]},{"tid":"020301"}
        |]}}
      """.stripMargin

    val jsonString =
      """
        |{"crowd_id":"id_1","crowd_name":"标签","tag":{"logic":true,"condition":[
        |{"logic":false,"condition":[{"logic":true,"condition":[{"logic":true,"condition":[{"app_cat":"020102"},{"app_cat":"020103"}]},{"tid":"020201"}]},{"app_cat":"020109","tid":"020110"}]},{"logic":true,"condition":[{"app_cat":"020102"},{"logic":false,"condition":[{"app_cat":"020102"},{"app_cat":"020103"}]}]}
        |]}}
      """.stripMargin

    val jsonString =
      """
        |{"crowd_id":"id_1","crowd_name":"标签","tag":{"logic":true,"condition":[
        |{"logic":false,"condition":[{"logic":true,"condition":[{"tid":"020201"},{"logic":true,"condition":[{"app_cat":"020102"},{"app_cat":"020103"}]}]},{"app_cat":"020109","tid":"020110"}]},{"logic":true,"condition":[{"app_cat":"020102"},{"logic":false,"condition":[{"app_cat":"020102"},{"app_cat":"020103"}]}]}
        |]}}
      """.stripMargin
      */

    val jsonString =
      """
        |{"crowd_id":"id_1","crowd_name":"标签","tag":{"logic":true,"condition":[{"app_cat":"020109","tid":"020110"},
        |{"logic":false,"condition":[{"logic":true,"condition":[{"tid":"020201"},{"logic":true,"condition":[{"app_cat":"020102"},{"app_cat":"020103"}]}]}]},{"logic":true,"condition":[{"app_cat":"020102"},{"logic":false,"condition":[{"app_cat":"020102"},{"app_cat":"020103"}]}]}
        |]}}
      """.stripMargin

    val crowdTag = JSON.parseObject(jsonString, classOf[CrowdTag])
    println("crowd_id" + crowdTag.crowd_id)
    println("crowd_name" + crowdTag.crowd_name)
    val tag = crowdTag.tag
    parseJson(tag)
    println("stack=======>" + stack)
  }

  val stack = new mutable.Stack[Object]

  def parseJson(tag: Tag) {
    val logic = tag.logic
    val tags = tag.condition
    for (i <- 0 to tags.length - 1) {
      var sql = ""
      if (JSON.parse(tags(i).toString).asInstanceOf[JSONObject].containsKey("logic")) {
        val tagss = JSON.parseObject(tags(i).toString, classOf[Tag])
        parseJson(tagss)
      } else {
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
      if (StringUtils.isNotBlank(sql)) {
        println("sql=======>" + sql)
        stack.push(sql)
      }
    }
    val top1 = stack.pop()
    val top2 = stack.pop()
    val combination = "" + top1 + logic + top2
    stack.push(combination)
  }

  def sql_con(_sql: String): String = {
    var param = ""
    val arr = _sql.split(",")
    for (i <- 0 until arr.length) {
      param += "'" + arr(i) + "',"
    }

    "(" + param.substring(0, param.length - 1) + ")"
  }
}