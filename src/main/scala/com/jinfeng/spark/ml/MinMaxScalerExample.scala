package com.jinfeng.spark.ml

import org.apache.spark.ml.feature.MinMaxScaler
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by WangJinfeng on 2017/5/18.
  */
object MinMaxScalerExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MinMaxScalerExample").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val dataFrame = sqlContext.read.format("libsvm").load("file:/D:/Program Files/JetBrains/Workspace/Spark/src/main/resources/data/sample_libsvm_data.txt")
    val scaler = new MinMaxScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")
    //compute summary statistics and generate MinMaxScalerModel
    val scalerModel = scaler.fit(dataFrame)
    //rescale each feature to range[min,max]
    val scaledData = scalerModel.transform(dataFrame)
    scaledData.show()
  }
}
