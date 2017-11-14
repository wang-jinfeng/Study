package com.jinfeng.spark.streaming

import java.util

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}


/**
  * Project : Spark
  * Package : com.jinfeng.spark.streaming
  * Author : WangJinfeng
  * Date : 2017-07-13 11:18
  * Email : jinfeng.wang@yoyi.com.cn
  * Phone : 152-1062-7698
  */
object KafkaWorldCount {
  def main(args: Array[String]) {

    val args = new Array[String](4)
    args(0) = "192.168.3.99:2181"
    args(1) = "consumer-group"
    args(2) = "test1"
    args(3) = "5"
    if (args.length < 4) {
      System.err.println("Usage: KafkaWordCount <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }
    StreamingExamples.setStreamingLogLevels()
    val Array(zkQuorum, group, topics, numThreads) = args

    val sparkConf = new SparkConf().setAppName("KafkaWordCount").setMaster("local[10]")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("src/main/resources/checkpoint/")

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)

    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1))
      .reduceByKeyAndWindow(_ + _, Minutes(1))

    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}

object KafkaWordCountProducer {
  def main(args: Array[String]) {
    val args = new Array[String](4)
    args(0) = "192.168.3.99:9092"
    args(1) = "test1"
    args(2) = "10"
    args(3) = "10"
    if (args.length < 4) {
      System.err.println("Usage: KafkaWordCountProducer <metadataBrokerList> <topic> " +
        "<messagesPerSec> <wordsPerMessage>")
      System.exit(1)
    }

    val Array(brokers, topic, messagesPerSec, wordsPerMessage) = args
    val props = new util.HashMap[String, Object]
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    while (true) {
      (1 to messagesPerSec.toInt).foreach { messageNum =>
        val str = (1 to wordsPerMessage.toInt).map(x => scala.util.Random.nextInt(10).toString)
          .mkString(" ")

        val message = new ProducerRecord[String, String](topic, null, str)
        producer.send(message)
      }

      Thread.sleep(1000)
    }
  }
}
