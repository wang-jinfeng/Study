package com.jinfeng.spark.ml

import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Created by WangJinfeng on 2017/5/17.
  */
object VectorIndexerExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("VectorIndexerExample").setMaster("local[10]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val data = sqlContext.read.format("libsvm").load("file:/D:/Program Files/JetBrains/IntelliJ IDEA/Workspaces/Spark/src/main/resources/input/sample_libsvm_data.txt")
    val indexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexed")
      .setMaxCategories(10)
    val indexerModel = indexer.fit(data)
    val categoricalFeatures: Set[Int] = indexerModel.categoryMaps.keys.toSet
    println(s"Chose ${categoricalFeatures.size} categorical features:" + categoricalFeatures.mkString(","))
    val indexedData = indexerModel.transform(data)
    indexedData.show()
  }
}
