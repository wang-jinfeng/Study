package com.jinfeng.spark.mllib

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.clustering.PowerIterationClustering
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser

/**
  * Project : Spark
  * Package : com.jinfeng.spark.mllib
  * Author : WangJinfeng
  * Date : 2017-06-26 18:42
  * Email : jinfeng.wang@yoyi.com.cn
  * Phone : 152-1062-7698
  */
object PowerIterationClusteringExample {

  case class Params(
                     k: Int = 2,
                     numPoints: Int = 10,
                     maxIterations: Int = 15
                   ) extends AbstractParams[Params]

  def main(args: Array[String]) {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("PowerIterationClusteringExample") {
      head("PowerIterationClusteringExample: an example PIC app using concentric circles.")
      opt[Int]('k', "k")
        .text(s"number of circles (clusters), default: ${defaultParams.k}")
        .action((x, c) => c.copy(k = x))
      opt[Int]('n', "n")
        .text(s"number of points in smallest circle, default: ${defaultParams.numPoints}")
        .action((x, c) => c.copy(numPoints = x))
      opt[Int]("maxIterations")
        .text(s"number of iterations, default: ${defaultParams.maxIterations}")
        .action((x, c) => c.copy(maxIterations = x))
    }

    parser.parse(args, defaultParams).map { params =>
      run(params)
    }.getOrElse {
      sys.exit(1)
    }
  }

  def run(params: Params) {
    System.setProperty("hadoop.home.dir", "D:\\Hadoop");
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName(s"PowerIterationClustering with $params")
    val sc = new SparkContext(conf)

    Logger.getRootLogger.setLevel(Level.WARN)

    val circlesRdd = generateCirclesRdd(sc, params.k, params.numPoints)
    val model = new PowerIterationClustering()
      .setK(params.k)
      .setMaxIterations(params.maxIterations)
      .setInitializationMode("degree")
      .run(circlesRdd)

    val clusters = model.assignments.collect().groupBy(_.cluster).mapValues(_.map(_.id))
    val assignments = clusters.toList.sortBy { case (k, v) => v.length }
    val assignmentsStr = assignments
      .map { case (k, v) =>
        s"$k -> ${v.sorted.mkString("[", ",", "]")}"
      }.mkString(", ")
    val sizesStr = assignments.map {
      _._2.length
    }.sorted.mkString("(", ",", ")")
    println(s"Cluster assignments: $assignmentsStr\ncluster sizes: $sizesStr")

    sc.stop()
  }

  def generateCircle(radius: Double, n: Int): Seq[(Double, Double)] = {
    Seq.tabulate(n) { i =>
      val theta = 2.0 * math.Pi * i / n
      (radius * math.cos(theta), radius * math.sin(theta))
    }
  }

  def generateCirclesRdd(
                          sc: SparkContext,
                          nCircles: Int,
                          nPoints: Int): RDD[(Long, Long, Double)] = {
    val points = (1 to nCircles).flatMap { i =>
      generateCircle(i, i * nPoints)
    }.zipWithIndex
    val rdd = sc.parallelize(points)
    val distancesRdd = rdd.cartesian(rdd).flatMap { case (((x0, y0), i0), ((x1, y1), i1)) =>
      if (i0 < i1) {
        Some((i0.toLong, i1.toLong, gaussianSimilarity((x0, y0), (x1, y1))))
      } else {
        None
      }
    }
    distancesRdd
  }

  /**
    * Gaussian Similarity:  http://en.wikipedia.org/wiki/Radial_basis_function_kernel
    */
  def gaussianSimilarity(p1: (Double, Double), p2: (Double, Double)): Double = {
    val ssquares = (p1._1 - p2._1) * (p1._1 - p2._1) + (p1._2 - p2._2) * (p1._2 - p2._2)
    math.exp(-ssquares / 2.0)
  }
}
