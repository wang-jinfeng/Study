package com.jinfeng.spark.example

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by WangJinfeng on 2017/4/24.
  */
object PartitionExample {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Map Example").setMaster("local[8]")
    val sc = new SparkContext(conf)
    // val n = 2000000
    val data = sc.parallelize(List((1, "www"), (1, "iteblog"), (1, "com"), (2, "bbs"), (2, "iteblog"), (2, "com"), (3, "good")))

    // val composite = sc.parallelize(2 to n, 8).map(x => (x, (2 to (n / x)))).repartition(8).flatMap(kv => kv._2.map(_ * kv._1))
    // val prime = sc.parallelize(2 to n, 8).subtract(composite)
    // prime.collect().foreach(println)

    val output = "src/main/resources/output/map/"
    FileSystem.get(sc.hadoopConfiguration).delete(new Path(output),true)
    import collection.mutable
    val multiValue = new mutable.HashMap[Int, mutable.Set[String]] with mutable.MultiMap[Int, String]

    multiValue.addBinding(1, "www,,,a")
    multiValue.addBinding(1, ",iteblog,,b")
    multiValue.addBinding(1, ",,com,c")
    multiValue.addBinding(2, "bbs,,,a")
    multiValue.addBinding(2, ",iteblog,,b")
    multiValue.addBinding(3, ",,com,c")

    val app_cat = new ArrayBuffer[String]()

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
    sc.parallelize(app_cat).repartition(1).saveAsTextFile("src/main/resources/output/map/")
  }
}
