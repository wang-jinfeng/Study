package com.jinfeng.spark.mllib

import org.apache.spark.mllib.feature.{StandardScaler, StandardScalerModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Project : Spark
  * Package : com.jinfeng.spark.mllib
  * Author : WangJinfeng
  * Date : 2017-07-11 17:13
  * Email : jinfeng.wang@yoyi.com.cn
  * Phone : 152-1062-7698
  */
object StandardScalerExample {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("StandardScalerExample").setMaster("local[10]")
    val sc = new SparkContext(conf)

    val data = MLUtils.loadLibSVMFile(sc, "src/main/resources/input/sample_libsvm_data.txt")
    val scaler1 = new StandardScaler().fit(data.map(x => x.features))
    val scaler2 = new StandardScaler(withMean = true, withStd = true).fit(data.map(x => x.features))
    //  scaler3 is an identical model to scaler2,and will produce identical transformations
    val scaler3 = new StandardScalerModel(scaler2.std, scaler2.mean)

    //  data1 will be unit variance
    val data1 = data.map(x => (x.label, scaler1.transform(x.features)))

    //  data2 will be unit variance and zero mean
    val data2 = data.map(x => (x.label, scaler2.transform(Vectors.dense(x.features.toArray))))

    println("data1: ")
    data1.foreach(x => println(x))

    println("data2: ")
    data2.foreach(x => println(x))
    sc.stop()
  }
}
