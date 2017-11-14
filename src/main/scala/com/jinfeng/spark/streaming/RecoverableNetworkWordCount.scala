package com.jinfeng.spark.streaming

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.{Accumulator, SparkConf, SparkContext}

/**
  * Project : Spark
  * Package : com.jinfeng.spark.streaming
  * Author : WangJinfeng
  * Date : 2017-07-12 14:23
  * Email : jinfeng.wang@yoyi.com.cn
  * Phone : 152-1062-7698
  */

object RecoverableNetworkWordCount {
  def createContext(ip: String, port: Int, checkpointDirectory: String): StreamingContext = {
    println("Create new context")
    //    val outputFile = new File(outputPath)
    //    if(outputFile.exists()) outputFile.delete()
    val sparkConf = new SparkConf().setAppName("RecoverableNetworkWordCount").setMaster("local[5]")
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    ssc.checkpoint("src/main/resources/checkpoint/")
    val lines = ssc.socketTextStream(ip, port)
    val words = lines.flatMap(_.split(" "))
    val wordsCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordsCounts.foreachRDD((rdd: RDD[(String, Int)], time: Time) => {
      val blacklist = WordBlacklist.getInstance(rdd.sparkContext)
      val droppedWordsCounter = DroppedWordsCounter.getInstance(rdd.sparkContext)
      val counts = rdd.filter {
        case (word, count) => {
          if (blacklist.value.contains(word)) {
            droppedWordsCounter += count
            false
          } else {
            true
          }
        }
      }.collect().mkString("[", ", ", "]")
      val output = "Counts at time " + time + " " + counts
      println(output)
      println("Dropped " + droppedWordsCounter.value + "words totally")
    })
    ssc
  }

  def main(args: Array[String]) {
    val args = new Array[String](3)
    args(0) = "192.168.3.99"
    args(1) = "7777"
    args(2) = "src/main/resources/checkpoint/"
    val Array(ip, port, checkpointDirectory) = args
    val ssc = StreamingContext.getOrCreate(checkpointDirectory,
      () => {
        createContext(ip, port.toInt, checkpointDirectory)
      })
    ssc.start()
    ssc.awaitTermination()
  }
}

object WordBlacklist {
  @volatile private var instance: Broadcast[Seq[String]] = null

  def getInstance(sc: SparkContext): Broadcast[Seq[String]] = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          val wordBlacklist = Seq("a", "b", "c")
          instance = sc.broadcast(wordBlacklist)
        }
      }
    }
    instance
  }
}

object DroppedWordsCounter {
  @volatile private var instance: Accumulator[Long] = null

  def getInstance(sc: SparkContext): Accumulator[Long] = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          instance = sc.accumulator(0L, "WordsInBlacklistCounter")
        }
      }
    }
    instance
  }
}