package com.jinfeng.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}

/**
  * Project : Spark
  * Package : com.jinfeng.spark.streaming
  * Author : WangJinfeng
  * Date : 2017-07-12 11:02
  * Email : jinfeng.wang@yoyi.com.cn
  * Phone : 152-1062-7698
  */
object StatefulNetworkWordCount {

  def main(args: Array[String]) {
    val args = new Array[String](2)
    args(0) = "192.168.3.99"
    args(1) = "7777"
    if (args.length < 2) {
      System.err.println("Usage: NetworkWordCount <hostname> <port>")
      System.exit(1)
    }

    StreamingExamples.setStreamingLogLevels()

    // Create the context with a 1 second batch size
    val sparkConf = new SparkConf().setAppName("StatefulNetworkWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    ssc.checkpoint(".")
    //  Initial state RDD for mapWithState operation
    val initialRDD = ssc.sparkContext.parallelize(List(("Hello", 1), ("World", 1)))
    val lines = ssc.socketTextStream(args(0), args(1).toInt)
    val words = lines.flatMap(_.split(" "))
    val wordDstream = words.map(x => (x, 1))
    val windowedWordCount = wordDstream.reduceByKeyAndWindow((a:Int,b:Int)=>(a+b),Seconds(30),Seconds(10))

    //  Update the cumulative count using mapWithState
    //  This will give a DStream made of state (which is the cumulative count of the words)
    val mappingFunc = (word: String, one: Option[Int], state: State[Int]) => {
      val sum = one.getOrElse(0) + state.getOption.getOrElse(0)
      val output = (word, sum)
      state.update(sum)
      output
    }

    val stateDstream = windowedWordCount.mapWithState(
      StateSpec.function(mappingFunc).initialState(initialRDD)
    )
    stateDstream.print()
    ssc.start()
    ssc.awaitTermination()

  }
}
