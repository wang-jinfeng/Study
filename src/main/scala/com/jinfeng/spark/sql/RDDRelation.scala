package com.jinfeng.spark.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by WangJinfeng on 2017/4/25.
  */

case class Record(key: Int, value: String)

case class Records(key: Int, value: String, text: String)

object RDDRelation {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDDRelation").setMaster("local[5]")
    conf.set("spark.rdd.compress=true", "true")
    conf.set(s"spark.serializer", s"org.apache.spark.serializer.KryoSerializer")
    conf.set(s"spark.kryo.registrationRequired", s"true")
    conf.registerKryoClasses(Array(classOf[scala.collection.mutable.WrappedArray.ofRef[_]],
      classOf[Record], classOf[Array[Record]], classOf[Records], classOf[Array[Records]], classOf[Array[String]]))
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    //val output = "D:\\Program Files\\JetBrains\\IntelliJ IDEA\\Workspaces\\Spark\\src\\main\\resources\\output\\sql\\"
    //FileSystem.get(sc.hadoopConfiguration).delete(new Path(output), true)

    // Importing the SQL context gives access to all the SQL functions and implicit conversions.
    import sqlContext.implicits._

    val df = sc.parallelize((1 to 100).map(i => Record(i, s"val_$i"))).persist(StorageLevel.MEMORY_ONLY_SER).toDF()
    // Any RDD containing case classes can be registered as a table.  The schema of the table is
    // automatically inferred using scala reflection.
    df.registerTempTable("records")

    // Once tables have been registered, you can run SQL queries over them.
    // println("Result of SELECT *:")
    // sqlContext.sql("SELECT * FROM records").collect().foreach(println)

    // Aggregation queries are also supported.
    // val count = sqlContext.sql("SELECT COUNT(*) FROM records").collect().head.getLong(0)
    // println(s"COUNT(*): $count")

    // The results of SQL queries are themselves RDDs and support all normal RDD functions.  The
    // items in the RDD are of type Row, which allows you to access each column by ordinal.

    val rddFromSql = sqlContext.sql("SELECT key, value, value FROM records WHERE key < 10").rdd.mapPartitions(myfuncPerPartition)
    val rddFromSql1 = sqlContext.sql("SELECT key, value, value FROM records WHERE key < 10").rdd.mapPartitions(myfuncPerPartition)

    println("reduceByKey:")
    val rddMap = rddFromSql.union(rddFromSql1).repartition(1).map(records => {
      (records.key, records.value)
    }).reduceByKey(_ + ";" + _).map(row => {
      (row._1, row._2.split(";").iterator)
    })

    println("groupByKey:")
    rddFromSql.union(rddFromSql1).repartition(1).map(records => {
      (records.key, records.value)
    }).groupByKey().foreach(println)
    /*
    val rddFromSql0 = sqlContext.sql("SELECT key, value, key+10 score FROM records WHERE key < 10").rdd.map(row =>
      Records
      (row.getAs("key"), row.getAs("value"), row.getAs("score"))).persist(StorageLevel.MEMORY_ONLY_SER)
    */
    /*
    println("Result of RDD.map:")
    rddFromSql.map(row =>
      // s"Key: ${row(0)}, Value: ${row(1)}"
      row.getAs("value").toString
    ).collect().foreach(println)
    */

    /*
    println("Results of RDD.map:")
    rddFromSql0.toDF().map(row =>
      // s"Key: ${row(0)}, Value: ${row(1)}"
      s"Key:${row(0)},score:${row.getAs("score")}"
    ).collect().foreach(println)
    */

    // Queries can also be written using a LINQ-like Scala DSL.
    // df.where($"key" === 1).orderBy($"value".asc).select($"key").collect().foreach(println)

    /*
    // Write out an RDD as a parquet file.
    df.write.parquet(output)

    // Read in parquet file.  Parquet files are self-describing so the schmema is preserved.
    val parquetFile = sqlContext.read.parquet(output)

    // Queries can be run using the DSL on parequet files just like the original RDD.
    parquetFile.where($"key" === 1).select($"value".as("a")).collect().foreach(println)

    // These files can also be registered as tables.
    parquetFile.registerTempTable("parquetFile")
    sqlContext.sql("SELECT * FROM parquetFile").collect().foreach(println)
    */
    sc.stop()
  }

  def myfuncPerPartition(iter: Iterator[Row]): Iterator[Records] = {
    println("run in partition")
    var res = for (e <- iter) yield Records(e.getAs("key"), e.getAs("value"), "mapPartitons")
    res
  }
}
