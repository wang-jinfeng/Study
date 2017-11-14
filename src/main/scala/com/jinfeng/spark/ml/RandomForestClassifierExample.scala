package com.jinfeng.spark.ml

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Created by WangJinfeng on 2017/5/22.
  */
object RandomForestClassifierExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RandomForestClassifierExample").setMaster("local[10]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val data = sqlContext.read.format("libsvm").load("file:/D:/Program Files/JetBrains/IntelliJ IDEA/Workspaces/Spark/src/main/resources/input/sample_libsvm_data.txt")

    //Index labels,adding metadata to the label column.
    //Fit on whole dataset to include all labels in index
    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(data)
    //Automatically identify categorical features,and index them
    //Set maxCategories so features with > 4 distinct values are treated as continuous
    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(4)
      .fit(data)
    //Split the data into training and test sets (30% held out for testing)
    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))
    //train a RandomForest model
    val rf = new RandomForestClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")
      .setNumTrees(10)

    //Convert indexed label back to original labels
    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)
    //Chain indexers and forest in a Pipeline
    val pipeline = new Pipeline()
      .setStages(Array(labelIndexer, featureIndexer, rf, labelConverter))
    //Train model.This also runs the indexers
    val model = pipeline.fit(trainingData)
    //Make predictions
    val predictions = model.transform(testData)
    //Select example rows to distplay
    predictions.select("predictedLabel", "label", "features").show(5)

    //Select (prediction,true label) and compute test error
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("precision")
    val accuracy = evaluator.evaluate(predictions)
    println("Test error = " + (1.0 - accuracy))
    val rfModel = model.stages(2).asInstanceOf[RandomForestClassificationModel]
    println("Learned classification forest model:\n" + rfModel.toDebugString)
  }
}
