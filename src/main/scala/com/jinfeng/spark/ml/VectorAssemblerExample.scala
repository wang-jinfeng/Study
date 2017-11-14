package com.jinfeng.spark.ml

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by WangJinfeng on 2017/5/19.
  */
object VectorAssemblerExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("VectorAssemblerExample").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val dataSet = sqlContext.createDataFrame(
      Seq((0,18,1.0,Vectors.dense(0.0,10.0,0.5),1.0))
    ).toDF("id","hour","mobile","userFeatures","clicked")
    val assembler = new VectorAssembler()
      .setInputCols(Array("hour","mobile","userFeatures"))
      .setOutputCol("features")
    val output = assembler.transform(dataSet)
    println(output.select("features","clicked").first())
  }
}
