package com.jinfeng.spark.ml

import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by WangJinfeng on 2017/5/18.
  */
object StandardScalerExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("StandardScalerExample").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val dataFrame = sqlContext.read.format("libsvm").load("file:/D:/Program Files/JetBrains/Workspace/Spark/src/main/resources/data/sample_libsvm_data.txt")
    val scaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")
      .setWithStd(true)
      .setWithMean(false)

    //compute summary statistics by fitting the StanderScaler
    val scalerModel = scaler.fit(dataFrame)
    //Normalize each feature to have unit standard deviation
    val scaledData = scalerModel.transform(dataFrame)
    scaledData.show()
  }
}
