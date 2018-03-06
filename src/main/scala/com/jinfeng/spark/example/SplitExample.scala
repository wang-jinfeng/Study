package com.jinfeng.spark.example

object SplitExample {
  def main(args: Array[String]): Unit = {
    val text = "wangjinfeng,19920627,,,,"
    println("text.size == " + text.split(",").size)

    val text1 = "wangjinfeng,19920627,,,,null"
    println("text1.size == " + text1.split(",").size)
  }
}
