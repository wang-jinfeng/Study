package com.jinfeng.spark.sort
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Project : Spark
  * Package : com.jinfeng.spark.sort
  * Author : WangJinfeng
  * Date : 2017-12-29 10:08
  * Email : wangjinfeng@yiche.com
  * Phone : 152-1062-7698
  */
object GlobalSort {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[5]").setAppName("GlobalSort")
    val sc = new SparkContext(conf)
    val output = "src/main/resources/output/sort/"
    FileSystem.get(sc.hadoopConfiguration).delete(new Path(output), true)
    //  Persist this RDD with the default storage level (MEMORY_ONLY).
    val numbers = sc.textFile("src/main/resources/input/random.txt").flatMap(_.split(",")).map(x => (x.toInt, 1)).cache()
    val result = numbers.repartitionAndSortWithinPartitions(new SortPartitoner(4)).map(x => x._1)
    result.saveAsTextFile("src/main/resources/output/sort/")
    sc.stop()
  }
}
