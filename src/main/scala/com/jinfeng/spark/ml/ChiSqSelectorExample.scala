package com.jinfeng.spark.ml

import org.apache.spark.ml.feature.ChiSqSelector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by WangJinfeng on 2017/5/19.
  */
object ChiSqSelectorExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RFormulaExample").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val data = Seq(
      (1, Vectors.dense(0.0, 0.0, 18.0, 1.0), 1.0),
      (2, Vectors.dense(0.0, 1.0, 12.0, 0.0), 0.0),
      (3, Vectors.dense(1.0, 0.0, 15.0, 0.1), 0.0)
    )
    val df = sqlContext.createDataFrame(data).toDF("id", "features", "clicked")
    val selector = new ChiSqSelector()
      .setNumTopFeatures(1)
      .setFeaturesCol("features")
      .setLabelCol("clicked")
      .setOutputCol("selectedFeatures")
    val result = selector.fit(df).transform(df)
    result.show()
  }
}
