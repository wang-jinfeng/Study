package com.jinfeng.spark.ml

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}

/**
  * Created by WangJinfeng on 2017/5/10.
  */
object Estimator {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Estimator").setMaster("local[10]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    //Prepare training data from a list of (label, features) tuples.
    val training = sqlContext.createDataFrame(Seq(
      (1.0, Vectors.dense(0.0, 1.1, 0.1)),
      (0.0, Vectors.dense(2.0, 1.0, -1.0)),
      (0.0, Vectors.dense(2.0, 1.3, 1.0)),
      (1.0, Vectors.dense(0.0, 1.2, -0.5))
    )).toDF("label", "features")
    //Create a logisticRegression instance.This instance is an Estimator.
    val lr = new LogisticRegression()
    //Learn a LogisticRegression model.This uses the parameters stored in lr
    val model1 = lr.fit(training)
    //println("Model 1 was fit using parameters:" + model1.parent.extractParamMap())
    //Print out the parameters,documentation,and any default value
    println("LogisticRegression parameters:\n" + lr.explainParams() + "\n")
    //set parameters using setter methods
    lr.setMaxIter(10).setRegParam(0.01)

    // We may alternatively specify parameters using a ParamMap,
    // which supports several methods for specifying parameters.
    val paramMap = ParamMap(lr.maxIter -> 20)
      .put(lr.maxIter, 30) // Specify 1 Param.  This overwrites the original maxIter.
      .put(lr.regParam -> 0.1, lr.threshold -> 0.55) // Specify multiple Params.

    //One can also combine ParamMaps
    val paramMap2 = ParamMap(lr.probabilityCol -> "myProbability")
    //Change output collumn name
    val paramMapCombined = paramMap ++ paramMap2
    //Now learn a new model using the paramMapCombined parameters
    //paramMapCombined overrides all parameters set earlier via lr.set* methods
    val model2 = lr.fit(training, paramMapCombined)
    //println("Model 1 was fit using parameters:" + model1.parent.extractParamMap())
    //println("Model 2 was fit using parameters:" + model2.parent.extractParamMap())

    val test = sqlContext.createDataFrame(Seq(
      (1.0, Vectors.dense(-1.0, 1.5, 1.3)),
      (0.0, Vectors.dense(3.0, 2.0, -0.1)),
      (1.0, Vectors.dense(0.0, 2.2, -1.5))
    )).toDF("label", "features")
    model1.transform(test)
      .select("features", "label", "probability", "prediction")
      .collect()
      .foreach {
        case Row(features: Vector, label: Double, prob: Vector, prediction: Double) =>
          println(s"($features,$label)->prob=>$prob,prediction=>$prediction")
      }
  }
}
