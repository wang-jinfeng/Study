package com.jinfeng.spark.mllib

/**
  * Project : Spark
  * Package : com.jinfeng.spark.mllib
  * Author : WangJinfeng
  * Date : 2017-07-11 18:16
  * Email : jinfeng.wang@yoyi.com.cn
  * Phone : 152-1062-7698
  */
object FPGrowthExample {
  case class Params(
                   input:String=null,
                   minSupport:Double=0.3,
                   numPartition:Int = -1) extends AbstractParams[Params]

  def main(args: Array[String]) {

  }
}
