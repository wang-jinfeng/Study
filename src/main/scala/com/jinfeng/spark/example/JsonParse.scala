package com.jinfeng.spark.example

import java.text.SimpleDateFormat
import java.util
import java.util.{Calendar, Date}

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.jinfeng.spark.pojo.AuTag
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ArrayBuffer

/**
  * Project: Study
  * Package: com.jinfeng.spark.example
  * Author: wangjf
  * Date: 2018/3/26
  * Email: wangjinfeng@reyun.com
  */

object JsonParse {

  def main(args: Array[String]): Unit = {

    //  val data = "{\n\t'id':'12345',\n\t'tag':[\n\t\t{\n\t\t\t'_type':true,\n\t\t\t'_type_sql':'('0201')'\n\t\t},\n\t\t{\n\t\t\t'_style':true,\n\t\t\t'_style_sql':'('0202')'\n\t\t},\n\t\t{\n\t\t\t'_subject':true,\n\t\t\t'_subject_sql':'('')'\n\t\t}\n\t],\n        'start':'2017-06-01',\n        'end':'2017-10-01'\n}"

    //    var type_sql = ArrayBuffer
    //  val data = "{\"id\":123,\"id_type\":\"imei\",\"relation\":true,\"tag\":[{\"_type_sql\":[\"020102\",\"020103\"],\"_type\":true},{\"_subject_sql\":[\"020602\",\"020603\"],\"_subject\":true},{\"_style_sql\":[\"020702\",\"020703\"],\"_style\":true},{\"_payment_sql\":[\"020201\",\"020202\",\"020203\"],\"_payment\":true},{\"_active_sql\":[\"020301\"],\"_active\":false}],\"start\":null,\"end\":null}"

    //    val jsonArray = jsonObject.get("tag").asInstanceOf[JSONArray]


    //    println("type_sql ===> " + jsonArray.getJSONObject(0).getString("_type_sql"))

    val date = null


    /*
    var tag_sql = ""
    val text = "(020101,020102)"

    val arr = text.substring(1, text.length - 1).split(",")
    for (i <- 0 until arr.length) {
      tag_sql += "'" + arr(i) + "',"
    }

    println("tag_sql ==> (" + tag_sql.substring(0, tag_sql.length - 1) + ")")

    val id: String = jsonObject.get("id").toString
    println("id==>" + id)

    val format = new SimpleDateFormat("yyyy-MM-dd")
    var start = ""
    var end = ""

    val cal: Calendar = Calendar.getInstance()
    cal.setTime(new Date());
    cal.add(Calendar.DAY_OF_MONTH, -1)
    start = format.format(cal.getTime())
    cal.add(Calendar.MONTH, -3)
    end = format.format(cal.getTime())

    println("start == " + start + ",end == " + end)
    */


  }
}
