package com.jinfeng.spark.ml

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{GBTClassificationModel, GBTClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Created by WangJinfeng on 2017/5/23.
  */
object GradientBoostedTreeClassifierExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("GradientBoostedTreeClassifierExample").setMaster("local[10]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val data = sqlContext.read.format("libsvm").load("file:/D:/Program Files/JetBrains/IntelliJ IDEA/Workspaces/Spark/src/main/resources/input/sample_libsvm_data.txt")
    //    Index labels,adding metadata to the label column
    //    Fit on whole dataset to include all labels in index
    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(data)
    //    Automatically identify categorical features,and index them
    //    Set maxCategories so features with > 4 distinct values are treated as continuous
    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .fit(data)
    //    Split the data into training and test sets (30% held out for testing)
    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))
    //    Train a GBT model
    val gbt = new GBTClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")
      .setMaxIter(10)
    //    Convert indexed labels back to original labels
    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)
    //    Chain indexers and GBT in a Pipeline
    val pipeline = new Pipeline()
      .setStages(Array(labelIndexer, featureIndexer, gbt, labelConverter))
    //    Train model.This also runs the indexers
    val model = pipeline.fit(trainingData)
    //    Make prediction
    val predictions = model.transform(testData)
    //    Select example rows to display
    predictions.select("predictedLabel", "label", "features").show()
    //    Select (prediction,true label) and compute test error
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("precision")
    val accuracy = evaluator.evaluate(predictions)
    println("Test Error = " + (1.0 - accuracy))
    val gbtModel = model.stages(2).asInstanceOf[GBTClassificationModel]
    println("Learned classification GBT model :\n" + gbtModel.toDebugString)
  }
}
