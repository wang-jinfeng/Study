package com.jinfeng.spark.ml

import org.apache.spark.ml.feature.RFormula
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by WangJinfeng on 2017/5/19.
  */
object RFormulaExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RFormulaExample").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val dataset = sqlContext.createDataFrame(Seq(
      (7, "US", 18, 1.0),
      (8, "CA", 12, 0.0),
      (9, "NZ", 15, 0.0)
    )).toDF("id", "country", "hour", "clicked")
    val formula = new RFormula()
      .setFormula("clicked~country+hour")
      .setFeaturesCol("features")
      .setLabelCol("label")
    val output = formula.fit(dataset).transform(dataset)
    output.select("features", "label").show()
  }
}
