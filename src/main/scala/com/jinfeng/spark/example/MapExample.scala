package com.jinfeng.spark.example

import org.apache.commons.lang.StringUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by WangJinfeng on 2017/4/24.
  */
case class schema(
                   id: String,
                   value: String
                 )

object PartitionExample {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Map Example").setMaster("local[8]")
    val sc = new SparkContext(conf)
    //  val n = 2000000
    val sqlContext = new SQLContext(sc)
    //  import sqlContext.implicits._


    val data = sc.parallelize(List((1, "www,,,a"), (1, ",iteblog,,b"), (1, ",,com,c"), (2, "bbs,,,a"), (2, ",iteblog," +
      ",b"), (2, ",,com,c"))).map(line => (line._1, line._2)).groupByKey().collect()

    //  val datas = sc.textFile("src/main/resources/input/data.txt").map(_.split(",")).map(line => schema(line(0), line(1)))

    //  val broadcastVar = sc.broadcast(Array(1, 2, 3))

    //  datas.map(schema => schema(schema.id, schema.value))

    //  datas.first()
    /*
    .reduceByKey(_ + ";" + _)
    .map(line => {
      (line._1, line._2.split(";").iterator)
    }).toDF().rdd
    */

    // val composite = sc.parallelize(2 to n, 8).map(x => (x, (2 to (n / x)))).repartition(8).flatMap(kv => kv._2.map(_ * kv._1))
    // val prime = sc.parallelize(2 to n, 8).subtract(composite)
    // prime.collect().foreach(println)


    val output = "src/main/resources/output/map/"
    FileSystem.get(sc.hadoopConfiguration).delete(new Path(output), true)

    val app_cat = new mutable.ArrayBuffer[String]()
    val arrayBuffer = data.map(line => {

      var key = line._1

      var a = ""
      var b = ""
      var c = ""
      val eventSet = mutable.Set("a", "b", "c")

      val lists = line._2
      println("===========")

      lists.filter(_.split(",").size == 4).map(_.split(",")).filter(e => eventSet.contains(e(3))).map(e => {
        a = if (StringUtils.isNotBlank(a)) a else e(0)
        b = if (StringUtils.isNotBlank(b)) b else e(1)
        c = if (StringUtils.isNotBlank(c)) c else e(2)
        println("-------------------")
      })
      lists.filter(_.split(",").size == 4).map(_.split(",")).map(e => {
        println("=========" + key + "," + a + "," + b + "," + c + "," + e(3))
        app_cat += key + "," + a + "," + b + "," + c + "," + e(3)
      })
      app_cat
    })


    /*
    val arrayBuffer = data.map(line => {
      line._2
      val app_cat = new mutable.ArrayBuffer[String]()
      var key = line._1

      var a = ""
      var b = ""
      var c = ""
      val eventSet = mutable.Set("a", "b", "c")

      line._2.toList.map(e => {
        if (e.split(",").size == 4) {

          val arr = e.split(",")
          val where = arr(3)

          if (eventSet.contains(where)) {
            a = arr(0)
            b = arr(1)
            c = arr(2)
          }
        }
      })

      line._2.toList.map(e => {
        app_cat += key + "," + a + "," + b + "," + c + "," + e.split(",")(3)
      })
      app_cat
    })
    */

    //  arrayBuffer

    import collection.mutable
    val multiValue = new mutable.HashMap[Int, mutable.Set[String]] with mutable.MultiMap[Int, String]

    multiValue.addBinding(1, "www,,,a")
    multiValue.addBinding(1, ",iteblog,,b")
    multiValue.addBinding(1, ",,com,c")
    multiValue.addBinding(2, "bbs,,,a")
    multiValue.addBinding(2, ",iteblog,,b")
    multiValue.addBinding(3, ",,com,c")

    val it = multiValue.keys.iterator
    while (it.hasNext) {
      var a = ""
      var b = ""
      var c = ""
      var id = it.next()
      multiValue.get(id).get.map(line => {
        if (line.split(",")(3).equals("a")) {
          a = line.split(",")(0)
        }
        if (line.split(",")(3).equals("b")) {
          b = line.split(",")(1)
        }
        if (line.split(",")(3).equals("c")) {
          c = line.split(",")(2)
        }
      })

      multiValue.get(id).get.toList.map(line => {
        app_cat += "" + a + "," + b + "," + c + "," + line.split(",")(3)
      })
    }

    println("start========")
    arrayBuffer.foreach(println)
    println("end=======")
    //    arrayBuffer.flatMap(line => line).foreach(println)
    //    for (i <- 0 until arrayBuffer.toArray().length - 1) {

    //    }
    //  arrayBuffer.foreach(println)
    //    arrayBuffer.repartition(1).saveAsTextFile(output)
  }
}
