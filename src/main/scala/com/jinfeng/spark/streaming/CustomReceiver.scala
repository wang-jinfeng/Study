package com.jinfeng.spark.streaming

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Project : Spark
  * Package : com.jinfeng.spark.streaming
  * Author : WangJinfeng
  * Date : 2017-07-12 17:52
  * Email : jinfeng.wang@yoyi.com.cn
  * Phone : 152-1062-7698
  */
object CustomReceiver {
  def main(args: Array[String]) {
    val args = new Array[String](2)
    args(0) = "192.168.3.99"
    args(1) = "7777"
    if (args.length < 2) {
      System.err.println("Usage: CustomReceiver <hostname> <port>")
      System.exit(1)
    }

    StreamingExamples.setStreamingLogLevels()

    // Create the context with a 1 second batch size
    val sparkConf = new SparkConf().setAppName("CustomReceiver").setMaster("local[5]")
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    val lines = ssc.receiverStream(new CustomReceiver(args(0), args(1).toInt))
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}

class CustomReceiver(host: String, port: Int)
  extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) with Logging {
  def onStart(): Unit = {
    new Thread("Socket Receiver") {
      override def run() {
        receive()
      }
    }.start()
  }

  def onStop() {

  }

  private def receive() {
    var socket: Socket = null
    var userInput: String = null

    try {
      logInfo("Connecting to " + host + ":" + port)
      socket = new Socket(host, port)
      logInfo("Connecting to " + host + ":" + port)
      val reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), "UTF-8"))
      userInput = reader.readLine()
      while (!isStopped() && userInput != null) {
        store(userInput)
        userInput = reader.readLine()
      }
      reader.close()
      socket.close()
      logInfo("Stopped receiving")
      restart("Trying to connect again")
    } catch {
      case e: java.net.ConnectException =>
        restart("Error connecting to " + host + ":" + port, e)
      case t: Throwable =>
        restart("Error receiving data", t)
    }
  }
}
