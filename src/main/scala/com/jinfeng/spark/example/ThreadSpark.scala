package com.jinfeng.spark.example

import java.util.concurrent.{ExecutorService, Executors}

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by WangJinfeng on 2017/4/19.
  */
object ThreadSpark {

  def main(args: Array[String]): Unit = {
    val threadPool: ExecutorService = Executors.newFixedThreadPool(1)
    try {
      //提交5个线程
      for (i <- 1 to 5) {
        threadPool.submit(new ThreadSpark("thread" + i))
        //threadPool.execute(new ThreadDemo("thread" + i))
      }
    } finally {
      threadPool.shutdown()
    }
  }
}

class ThreadSpark(threadDemo: String) extends Runnable {

  @Override
  def run() {
    val conf = new SparkConf().setAppName(threadDemo)
    conf.set("spark.driver.allowMultipleContexts", "true").setMaster("local")
    val sc = new SparkContext(conf)
    println("AppName === " + threadDemo + ",ApplicationId === " + sc.applicationId)
    val rdd = sc.parallelize(List((1, 2), (3, 4), (3, 6)))
    rdd.foreach(println)
    sc.stop()
  }
}
