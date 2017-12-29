package com.jinfeng.spark.sort
import org.apache.spark.Partitioner

/**
  * Project : Spark
  * Package : com.jinfeng.spark.sort
  * Author : WangJinfeng
  * Date : 2017-12-29 10:04
  * Email : wangjinfeng@yiche.com
  * Phone : 152-1062-7698
  */
class SortPartitoner(num: Int) extends Partitioner {
  override def numPartitions: Int = num

  val partitionerSize = Integer.MAX_VALUE / num + 1

  override def getPartition(key: Any): Int = {
    val intKey = key.asInstanceOf[Int]
    intKey / partitionerSize
  }
}
