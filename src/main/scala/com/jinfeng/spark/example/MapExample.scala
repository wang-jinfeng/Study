package com.jinfeng.spark.example

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by WangJinfeng on 2017/4/24.
  */

case class Net(
                id: Int,
                a: String
              )

object PartitionExample {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Map Example").setMaster("local[8]")
    val sc = new SparkContext(conf)
    // val n = 2000000
    val data = sc.parallelize(List((1, "www,,,a"), (1, ",iteblog,,b"), (1, ",,com,c"), (2, "bbs,,,a"), (2, ",iteblog," +
      ",b"), (3, ",,good,c"))).map(line => {
      Net(line._1, line._2)
    })
    // val composite = sc.parallelize(2 to n, 8).map(x => (x, (2 to (n / x)))).repartition(8).flatMap(kv => kv._2.map(_ * kv._1))
    // val prime = sc.parallelize(2 to n, 8).subtract(composite)
    // prime.collect().foreach(println)
    val sqlContext = new SQLContext(sc)

    val output = "src/main/resources/output/map/"
    FileSystem.get(sc.hadoopConfiguration).delete(new Path(output), true)
    /*
    import collection.mutable
    val multiValue = new mutable.HashMap[Int, mutable.Set[String]] with mutable.MultiMap[Int, String]

    multiValue.addBinding(1, "www,,,a")
    multiValue.addBinding(1, ",iteblog,,b")
    multiValue.addBinding(1, ",,com,c")
    multiValue.addBinding(2, "bbs,,,a")
    multiValue.addBinding(2, ",iteblog,,b")
    multiValue.addBinding(3, ",,com,c")
    */

    val app_cat = new ArrayBuffer[String]()

    val it = data.map(rdd => {
      rdd.id
    }).collect().iterator
    while (it.hasNext) {
      var a = ""
      var b = ""
      var c = ""
      var id = it.next()
      data.map(rdd => {
        (rdd.id, rdd.a)
      }).map(line => {
        if (line._2.split(",")(3).equals("a")) {
          a = line._2.split(",")(0)
        }
      })

      //      data.map(rdd=).get.toList.map(line => {
      //        app_cat += "" + id + "," + a + "," + b + "," + c + "," + line.split(",")(3)
      //      })
    }
    import sqlContext.implicits._
    //    app_cat.filter(_.split(",").size == 5).map(_.split(",")).map(e => Net(e(0), e(1))).toDF().rdd.foreach(println)
    //    appDF.
    //  .repartition(1).saveAsTextFile(output)
  }
}
