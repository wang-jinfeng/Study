package com.jinfeng.spark.ml

import org.apache.spark.ml.feature.PolynomialExpansion
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Created by WangJinfeng on 2017/5/17.
  */
object PolynomialExpansionExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("PolynomialExpansionExample").setMaster("local[10]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val data = Array(
      Vectors.dense(-2.0, 2.3),
      Vectors.dense(0.0, 0.0),
      Vectors.dense(0.6, -1.1)
    )
    val df = sqlContext.createDataFrame(data.map(Tuple1.apply)).toDF("features")

    val polynomialExpansion = new PolynomialExpansion()
      .setInputCol("features")
      .setOutputCol("polyFeatures")
      .setDegree(3)
    val polyDF = polynomialExpansion.transform(df)
    polyDF.select("polyFeatures").take(3).foreach(println)
  }
}
