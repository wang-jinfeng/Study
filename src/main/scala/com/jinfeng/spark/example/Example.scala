package com.jinfeng.spark.example

/**
  * Project: Study
  * Package: com.jinfeng.spark.example
  * Author: wangjf
  * Date: 2018/4/13
  * Email: wangjinfeng@reyun.com
  */

object Example {
  def main(args: Array[String]): Unit = {
    val text = "'{\"id\":269,\"id_type\":\"idfa\",\"relation\":false,\"tag\":[{\"_type_sql\":\"(020102)\",\"_type\":true},{\"_subject_sql\":\"(020602)\",\"_subject\":false},{\"_style_sql\":\"(020702)\",\"_style\":false},{\"_payment_sql\":\"(020201,020202,020203)\",\"_payment\":false},{\"_active_sql\":\"(020301,020401)\",\"_active\":false}],\"start\":null,\"end\":null}'"
    val data: String = text.substring(1, text.length - 1)
    println(data)
  }

}
