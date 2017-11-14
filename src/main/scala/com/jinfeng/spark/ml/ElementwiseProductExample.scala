package com.jinfeng.spark.ml

import org.apache.spark.ml.feature.ElementwiseProduct
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by WangJinfeng on 2017/5/18.
  */
object ElementwiseProductExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ElementwiseProductExample").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val dataFrame = sqlContext.createDataFrame(Seq(
      ("a", Vectors.dense(1.0, 2.0, 3.0)),
      ("b", Vectors.dense(4.0, 5.0, 6.0))
    )).toDF("id", "vector")
    val transformingVector = Vectors.dense(0.0, 1.0, 2.0)
    val transformer = new ElementwiseProduct()
      .setScalingVec(transformingVector)
      .setInputCol("vector")
      .setOutputCol("transformedVector")
    //batch transform the vectors to create new column
    transformer.transform(dataFrame).show()
  }
}
