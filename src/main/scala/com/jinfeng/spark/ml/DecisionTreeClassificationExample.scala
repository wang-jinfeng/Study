package com.jinfeng.spark.ml

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{BinaryLogisticRegressionSummary, DecisionTreeClassificationModel, DecisionTreeClassifier, LogisticRegression}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by WangJinfeng on 2017/5/19.
  */
object DecisionTreeClassificationExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DecisionTreeClassificationExample").setMaster("local[10]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val data = sqlContext.read.format("libsvm").load("file:/D:/Program Files/JetBrains/IntelliJ IDEA/Workspaces/Spark/src/main/resources/input/sample_libsvm_data.txt")

    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(data)

    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .fit(data)

    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))
    val dt = new DecisionTreeClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")
    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)

    //Chain indexer and tree in a Pipeline
    val pipeline = new Pipeline()
      .setStages(Array(labelIndexer, featureIndexer, dt, labelConverter))
    //Train model.This also runs the indexers
    val model = pipeline.fit(trainingData)
    //Make predictions
    val predictions = model.transform(testData)

    //Select example rows to display
    predictions.select("predictedLabel", "label", "features").show(10)

    //Select (prediction,true label) and compute test error
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("precision")
    val accuracy = evaluator.evaluate(predictions)
    println("Test Error = " + (1.0 - accuracy))
    val treeModel = model.stages(2).asInstanceOf[DecisionTreeClassificationModel]
    println("Learned classification tree model:\n" + treeModel.toDebugString)
    sc.stop()
  }
}
