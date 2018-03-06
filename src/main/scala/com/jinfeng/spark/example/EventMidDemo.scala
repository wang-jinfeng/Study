package com.jinfeng.spark.example

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

case class EventMidClass(deviceid: String, product: String)

object EventMidDemo {

  def main(args: Array[String]): Unit = {

    val CTRL_A = '\001'
    val CTRL_B = '\002'
    val CTRL_C = '\003'

    val conf = new SparkConf().setAppName("Map Example").setMaster("local[8]")
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    val output = "src/main/resources/output/dmp_mid/"

    val output0 = "src/main/resources/output/mid/"

    FileSystem.get(sc.hadoopConfiguration).delete(new Path(output), true)

    FileSystem.get(sc.hadoopConfiguration).delete(new Path(output0), true)

    //  val event = sc.textFile("src/main/resources/data/tk.log")

    val multiValueMap = new mutable.HashMap[String, mutable.Set[String]] with mutable.MultiMap[String, String]
    multiValueMap.addBinding("869789020000640\0017922212415e5270f5591d190ecf13eba\001tk", "startup\0022018-01-29\001" +
      "tk")

    val event_mid = new ArrayBuffer[String]()

    val keys = multiValueMap.keys.iterator

    while (keys.hasNext) {

      var key = keys.next()
      var ds = ""
      var product = ""
      var where = ""
      val eventSet = mutable.Set("install", "startup", "register", "loggedin", "event", "attribute", "quest")

      println("keys")

      multiValueMap.get(key).get.map(line => {
        if (line.split(CTRL_B)(1).split(CTRL_A).size == 2) {

          where = line.split(CTRL_B)(0)

          val arr = line.split(CTRL_B)(1).split(CTRL_A)

          if (eventSet.contains(where)) {
            ds = arr(0)
            product = arr(1)
          }
        }
      })

      multiValueMap.get(key).get
        .map(line => {
          event_mid += key + CTRL_A + line.split(CTRL_B)(0) + CTRL_A + ds + CTRL_A + product
        })
    }

    event_mid.take(10).filter(_.split(CTRL_A).length == 6).map(_.split(CTRL_A))
      .map(event => (event(0), event(5)))
      .foreach(println)

    import sqlContext.implicits._

    /*
    val eventDF = event_mid
      .filter(_.split(CTRL_A).length == 6)
      .map(_.split(CTRL_A))
      .map(event => EventMidClass(event(0), event(2))).toDF()
      */

    //eventDF.rdd.repartition(1).saveAsTextFile(output)
    //  spark.sql("SET hive.exec.dynamic.partition = true")
    //  spark.sql("SET hive.exec.dynamic.partition.mode = nonstrict ")

    //  eventDF.registerTempTable("event_mid")


    //  spark.sql("INSERT OVERWRITE TABLE dmp.event_mid PARTITION(`ds`,`product`) SELECT * FROM event_mid")

    //  sc.stop()

    /*
    eventDF.write.mode(SaveMode.Overwrite).partitionBy("ds","product").option("path","s3://reyunbpu/dmp/event/mid")
      .saveAsTable("dmp.event_mid")
    */
  }

  val event_tkio =
    """
      |SELECT
      |   CASE WHEN xwho IS NOT NULL AND xwho != 'unknown' THEN xwho ELSE '' END AS xwho,
      |   CASE WHEN xwhen IS NOT NULL AND xwhen != 'unknown' THEN xwhen ELSE '' END AS xwhen,
      |   CASE WHEN xcontext['_deviceid'] IS NOT NULL AND xcontext['_deviceid'] != 'unknown' AND xcontext['_deviceid'] != '' THEN xcontext['_deviceid']
      |   WHEN xcontext['_idfa'] IS NOT NULL AND xcontext['_idfa'] != 'unknown' AND xcontext['_idfa'] != '' THEN xcontext['_idfa']
      |   WHEN xcontext['_imei'] IS NOT NULL AND xcontext['_imei'] != 'unknown' AND xcontext['_imei'] != '' THEN xcontext['_imei']
      |   WHEN xcontext['_androidid'] IS NOT NULL AND xcontext['_androidid'] != 'unknown' AND xcontext['_androidid'] != '' THEN xcontext['_androidid']
      |   ELSE '' END AS device_id,
      |   CASE WHEN xcontext['_deviceid'] IS NOT NULL AND xcontext['_deviceid'] != 'unknown' AND xcontext['_deviceid'] != '' THEN xcontext['_deviceid']
      |   WHEN xcontext['_idfa'] IS NOT NULL AND xcontext['_idfa'] != 'unknown' AND xcontext['_idfa'] != '' THEN xcontext['_idfa']
      |   WHEN xcontext['_imei'] IS NOT NULL AND xcontext['_imei'] != 'unknown' AND xcontext['_imei'] != '' THEN xcontext['_imei']
      |   WHEN xcontext['_androidid'] IS NOT NULL AND xcontext['_androidid'] != 'unknown' AND xcontext['_androidid'] != '' THEN xcontext['_androidid']
      |   ELSE '' END AS ry_id,
      |   CASE WHEN xcontext['_idfa'] IS NOT NULL AND xcontext['_idfa'] != 'unknown' THEN xcontext['_idfa'] ELSE '' END AS idfa,
      |   CASE WHEN xcontext['_imei'] IS NOT NULL AND xcontext['_imei'] != 'unknown' THEN xcontext['_imei'] ELSE '' END AS imei,
      |   CASE WHEN xcontext['_androidid'] IS NOT NULL AND xcontext['_androidid'] != 'unknown' THEN xcontext['_androidid'] ELSE '' END AS androidid,
      |   CASE WHEN xwhere IS NOT NULL AND xwhere != 'unknown' THEN xwhere ELSE '' END AS event_type,
      |   CASE WHEN xcontext['_mac'] IS NOT NULL AND xcontext['_mac'] != 'unknown' THEN xcontext['_mac'] ELSE '' END AS mac,
      |   CASE WHEN xcontext['_campaignid'] IS NOT NULL AND xcontext['_campaignid'] != 'unknown' THEN xcontext['_campaignid'] ELSE '' END AS campaign_id,
      |   CASE WHEN xcontext['_cid'] IS NOT NULL AND xcontext['_cid'] != 'unknown' THEN xcontext['_cid'] ELSE '' END AS cid,
      |   CASE WHEN xcontext['_bundleid'] IS NOT NULL AND xcontext['_bundleid'] != 'unknown' THEN xcontext['_bundleid'] ELSE '' END AS bundle_id,
      |   CASE WHEN xcontext['_manufacturer'] IS NOT NULL AND xcontext['_manufacturer'] != 'unknown' THEN xcontext['_manufacturer'] ELSE '' END AS manufacturer,
      |   CASE WHEN xcontext['_accountid'] IS NOT NULL AND xcontext['_accountid'] != 'unknown' THEN xcontext['_accountid'] ELSE '' END AS account_id,
      |   CASE WHEN xcontext['_network_type'] IS NOT NULL AND xcontext['_network_type'] != 'unknown' THEN xcontext['_network_type'] ELSE '' END AS network_type,
      |   CASE WHEN xcontext['_istablet'] IS NOT NULL AND xcontext['_istablet'] != 'unknown' THEN xcontext['_istablet'] ELSE '' END AS istablet,
      |   CASE WHEN xcontext['_country'] IS NOT NULL AND xcontext['_country'] != 'unknown' THEN xcontext['_country'] ELSE '' END AS country,
      |   CASE WHEN xcontext['_province'] IS NOT NULL AND xcontext['_province'] != 'unknown' THEN xcontext['_province'] ELSE '' END AS province,
      |   CASE WHEN xcontext['_ip'] IS NOT NULL AND xcontext['_ip'] != 'unknown' THEN xcontext['_ip'] ELSE '' END AS ip,
      |   CASE WHEN xcontext['_ryos'] IS NOT NULL AND xcontext['_ryos'] != 'unknown' THEN xcontext['_ryos'] ELSE '' END AS os,
      |   CASE WHEN xcontext['_ryosversion'] IS NOT NULL AND xcontext['_ryosversion'] != 'unknown' THEN xcontext['_ryosversion'] ELSE '' END AS os_version,
      |   CASE WHEN xcontext['_paymenttype'] IS NOT NULL AND xcontext['_paymenttype'] != 'unknown' THEN xcontext['_paymenttype'] ELSE '' END AS payment_type,
      |   CASE WHEN xcontext['_currencyamount'] IS NOT NULL AND xcontext['_currencyamount'] != 'unknown' THEN xcontext['_currencyamount'] ELSE '' END AS currency_amount,
      |   CASE WHEN xcontext['_currencytype'] IS NOT NULL AND xcontext['_currencytype'] != 'unknown' THEN xcontext['_currencytype'] ELSE '' END AS currency_type,
      |   CASE WHEN xcontext['_transactionid'] IS NOT NULL AND xcontext['_transactionid'] != 'unknown' THEN xcontext['_transactionid'] ELSE '' END AS transaction_id,
      |   CASE WHEN xcontext['_virtualcoinamount'] IS NOT NULL AND xcontext['_virtualcoinamount'] != 'unknown' THEN xcontext['_virtualcoinamount'] ELSE '' END AS virtualcoin_amount,
      |   CASE WHEN xcontext['_pkgname'] IS NOT NULL AND xcontext['_pkgname'] != 'unknown' THEN xcontext['_pkgname'] ELSE '' END AS pkgname,
      |   CASE WHEN appkey IS NOT NULL AND appkey != 'unknown' THEN appkey ELSE '' END AS app_key,
      |   CASE WHEN xcontext['_app_version'] IS NOT NULL AND xcontext['_app_version'] != 'unknown' THEN xcontext['_app_version'] ELSE '' END AS app_version,
      |   CASE WHEN xcontext['_model'] IS NOT NULL AND xcontext['_model'] != 'unknown' THEN xcontext['_model'] ELSE '' END AS model,
      |   CASE WHEN xcontext['_resolution'] IS NOT NULL AND xcontext['_resolution'] != 'unknown' THEN xcontext['_resolution'] ELSE '' END AS resolution,
      |   CASE WHEN xcontext['_lib_version'] IS NOT NULL AND xcontext['_lib_version'] != 'unknown' THEN xcontext['_lib_version'] ELSE '' END AS lib_version,
      |   CASE WHEN xcontext['_carrier'] IS NOT NULL AND xcontext['_carrier'] != 'unknown' THEN xcontext['_carrier']
      |   ELSE '' END AS carrier,
      |   'tkio' AS product
      |   FROM dmp.event_tkio
    """.stripMargin

  val event_tk =
    """
      |SELECT
      |   CASE WHEN xwho IS NOT NULL AND xwho != 'unknown' THEN xwho ELSE '' END AS xwho,
      |   CASE WHEN xwhen IS NOT NULL AND xwhen != 'unknown' THEN xwhen ELSE '' END AS xwhen,
      |   CASE WHEN xcontext['deviceid'] IS NOT NULL AND xcontext['deviceid'] != 'unknown' AND xcontext['deviceid'] != '' THEN xcontext['deviceid']
      |   WHEN xcontext['idfa'] IS NOT NULL AND xcontext['idfa'] != 'unknown' AND xcontext['idfa'] != '' THEN xcontext['idfa']
      |   WHEN xcontext['imei'] IS NOT NULL AND xcontext['imei'] != 'unknown' AND xcontext['imei'] != '' THEN xcontext['imei']
      |   WHEN xcontext['androidid'] IS NOT NULL AND xcontext['androidid'] != 'unknown' AND xcontext['androidid'] != '' THEN xcontext['androidid']
      |   ELSE '' END AS device_id,
      |   CASE WHEN xcontext['deviceid'] IS NOT NULL AND xcontext['deviceid'] != 'unknown' AND xcontext['deviceid'] != '' THEN xcontext['deviceid']
      |   WHEN xcontext['idfa'] IS NOT NULL AND xcontext['idfa'] != 'unknown' AND xcontext['idfa'] != '' THEN xcontext['idfa']
      |   WHEN xcontext['imei'] IS NOT NULL AND xcontext['imei'] != 'unknown' AND xcontext['imei'] != '' THEN xcontext['imei']
      |   WHEN xcontext['androidid'] IS NOT NULL AND xcontext['androidid'] != 'unknown' AND xcontext['androidid'] != '' THEN xcontext['androidid']
      |   ELSE '' END AS ry_id,
      |   CASE WHEN xcontext['idfa'] IS NOT NULL AND xcontext['idfa'] != 'unknown' THEN xcontext['idfa'] ELSE '' END AS idfa,
      |   CASE WHEN xcontext['imei'] IS NOT NULL AND xcontext['imei'] != 'unknown' THEN xcontext['imei'] ELSE '' END AS imei,
      |   CASE WHEN xcontext['androidid'] IS NOT NULL AND xcontext['androidid'] != 'unknown' THEN xcontext['androidid'] ELSE '' END AS androidid,
      |   CASE WHEN xwhere IS NOT NULL AND xwhere != 'unknown' THEN xwhere ELSE '' END AS event_type,
      |   CASE WHEN xcontext['mac'] IS NOT NULL AND xcontext['mac'] != 'unknown' THEN xcontext['mac'] ELSE '' END AS mac,
      |   CASE WHEN xcontext['campaignid'] IS NOT NULL AND xcontext['campaignid'] != 'unknown' THEN xcontext['campaignid'] ELSE '' END AS campaign_id,
      |   CASE WHEN xcontext['cid'] IS NOT NULL AND xcontext['cid'] != 'unknown' THEN xcontext['cid'] ELSE '' END AS cid,
      |   CASE WHEN xcontext['bundleid'] IS NOT NULL AND xcontext['bundleid'] != 'unknown' THEN xcontext['bundleid'] ELSE '' END AS bundle_id,
      |   CASE WHEN xcontext['manufacturer'] IS NOT NULL AND xcontext['manufacturer'] != 'unknown' THEN xcontext['manufacturer'] ELSE '' END AS manufacturer,
      |   CASE WHEN xcontext['accountid'] IS NOT NULL AND xcontext['accountid'] != 'unknown' THEN xcontext['accountid'] ELSE '' END AS account_id,
      |   CASE WHEN xcontext['network_type'] IS NOT NULL AND xcontext['network_type'] != 'unknown' THEN xcontext['network_type'] ELSE '' END AS network_type,
      |   CASE WHEN xcontext['istablet'] IS NOT NULL AND xcontext['istablet'] != 'unknown' THEN xcontext['istablet'] ELSE '' END AS istablet,
      |   CASE WHEN xcontext['country'] IS NOT NULL AND xcontext['country'] != 'unknown' THEN xcontext['country'] ELSE '' END AS country,
      |   CASE WHEN xcontext['province'] IS NOT NULL AND xcontext['province'] != 'unknown' THEN xcontext['province'] ELSE '' END AS province,
      |   CASE WHEN xcontext['ip'] IS NOT NULL AND xcontext['ip'] != 'unknown' THEN xcontext['ip'] ELSE '' END AS ip,
      |   CASE WHEN xcontext['ryos'] IS NOT NULL AND xcontext['ryos'] != 'unknown' THEN xcontext['ryos'] ELSE '' END AS os,
      |   CASE WHEN xcontext['ryosversion'] IS NOT NULL AND xcontext['ryosversion'] != 'unknown' THEN xcontext['ryosversion'] ELSE '' END AS os_version,
      |   CASE WHEN xcontext['paymenttype'] IS NOT NULL AND xcontext['paymenttype'] != 'unknown' THEN xcontext['paymenttype'] ELSE '' END AS payment_type,
      |   CASE WHEN xcontext['currencyamount'] IS NOT NULL AND xcontext['currencyamount'] != 'unknown' THEN xcontext['currencyamount'] ELSE '' END AS currency_amount,
      |   CASE WHEN xcontext['currencytype'] IS NOT NULL AND xcontext['currencytype'] != 'unknown' THEN xcontext['currencytype'] ELSE '' END AS currency_type,
      |   CASE WHEN xcontext['transactionid'] IS NOT NULL AND xcontext['transactionid'] != 'unknown' THEN xcontext['transactionid'] ELSE '' END AS transaction_id,
      |   CASE WHEN xcontext['virtualcoinamount'] IS NOT NULL AND xcontext['virtualcoinamount'] != 'unknown' THEN xcontext['virtualcoinamount'] ELSE '' END AS virtualcoin_amount,
      |   CASE WHEN xcontext['pkgname'] IS NOT NULL AND xcontext['pkgname'] != 'unknown' THEN xcontext['pkgname'] ELSE '' END AS pkgname,
      |   CASE WHEN appkey IS NOT NULL AND appkey != 'unknown' THEN appkey ELSE '' END AS app_key,
      |   CASE WHEN xcontext['app_version'] IS NOT NULL AND xcontext['app_version'] != 'unknown' THEN xcontext['app_version'] ELSE '' END AS app_version,
      |   CASE WHEN xcontext['model'] IS NOT NULL AND xcontext['model'] != 'unknown' THEN xcontext['model'] ELSE '' END AS model,
      |   CASE WHEN xcontext['resolution'] IS NOT NULL AND xcontext['resolution'] != 'unknown' THEN xcontext['resolution'] ELSE '' END AS resolution,
      |   CASE WHEN xcontext['lib_version'] IS NOT NULL AND xcontext['lib_version'] != 'unknown' THEN xcontext['lib_version'] ELSE '' END AS lib_version,
      |   CASE WHEN xcontext['carrier'] IS NOT NULL AND xcontext['carrier'] != 'unknown' THEN xcontext['carrier']
      |   ELSE '' END AS carrier,
      |   'tk' AS product
      |   FROM dmp.event_tk
    """.stripMargin

  val event_game =
    """
      |SELECT
      |   CASE WHEN xwho IS NOT NULL AND xwho != 'unknown' THEN xwho ELSE '' END AS xwho,
      |   CASE WHEN xwhen IS NOT NULL AND xwhen != 'unknown' THEN xwhen ELSE '' END AS xwhen,
      |   CASE WHEN xcontext['deviceid'] IS NOT NULL AND xcontext['deviceid'] != 'unknown' AND xcontext['deviceid'] != '' THEN xcontext['deviceid']
      |   WHEN xcontext['idfa'] IS NOT NULL AND xcontext['idfa'] != 'unknown' AND xcontext['idfa'] != '' THEN xcontext['idfa']
      |   WHEN xcontext['imei'] IS NOT NULL AND xcontext['imei'] != 'unknown' AND xcontext['imei'] != '' THEN xcontext['imei']
      |   WHEN xcontext['androidid'] IS NOT NULL AND xcontext['androidid'] != 'unknown' AND xcontext['androidid'] != '' THEN xcontext['androidid']
      |   ELSE '' END AS device_id,
      |   CASE WHEN xcontext['deviceid'] IS NOT NULL AND xcontext['deviceid'] != 'unknown' AND xcontext['deviceid'] != '' THEN xcontext['deviceid']
      |   WHEN xcontext['idfa'] IS NOT NULL AND xcontext['idfa'] != 'unknown' AND xcontext['idfa'] != '' THEN xcontext['idfa']
      |   WHEN xcontext['imei'] IS NOT NULL AND xcontext['imei'] != 'unknown' AND xcontext['imei'] != '' THEN xcontext['imei']
      |   WHEN xcontext['androidid'] IS NOT NULL AND xcontext['androidid'] != 'unknown' AND xcontext['androidid'] != '' THEN xcontext['androidid']
      |   ELSE '' END AS ry_id,
      |   CASE WHEN xcontext['idfa'] IS NOT NULL AND xcontext['idfa'] != 'unknown' THEN xcontext['idfa'] ELSE '' END AS idfa,
      |   CASE WHEN xcontext['imei'] IS NOT NULL AND xcontext['imei'] != 'unknown' THEN xcontext['imei'] ELSE '' END AS imei,
      |   CASE WHEN xcontext['androidid'] IS NOT NULL AND xcontext['androidid'] != 'unknown' THEN xcontext['androidid'] ELSE '' END AS androidid,
      |   CASE WHEN xwhere IS NOT NULL AND xwhere != 'unknown' THEN xwhere ELSE '' END AS event_type,
      |   CASE WHEN xcontext['mac'] IS NOT NULL AND xcontext['mac'] != 'unknown' THEN xcontext['mac'] ELSE '' END AS mac,
      |   CASE WHEN xcontext['campaignid'] IS NOT NULL AND xcontext['campaignid'] != 'unknown' THEN xcontext['campaignid'] ELSE '' END AS campaign_id,
      |   CASE WHEN xcontext['cid'] IS NOT NULL AND xcontext['cid'] != 'unknown' THEN xcontext['cid'] ELSE '' END AS cid,
      |   CASE WHEN xcontext['bundleid'] IS NOT NULL AND xcontext['bundleid'] != 'unknown' THEN xcontext['bundleid'] ELSE '' END AS bundle_id,
      |   CASE WHEN xcontext['manufacturer'] IS NOT NULL AND xcontext['manufacturer'] != 'unknown' THEN xcontext['manufacturer'] ELSE '' END AS manufacturer,
      |   CASE WHEN xcontext['accountid'] IS NOT NULL AND xcontext['accountid'] != 'unknown' THEN xcontext['accountid'] ELSE '' END AS account_id,
      |   CASE WHEN xcontext['network_type'] IS NOT NULL AND xcontext['network_type'] != 'unknown' THEN xcontext['network_type'] ELSE '' END AS network_type,
      |   CASE WHEN xcontext['istablet'] IS NOT NULL AND xcontext['istablet'] != 'unknown' THEN xcontext['istablet'] ELSE '' END AS istablet,
      |   CASE WHEN xcontext['country'] IS NOT NULL AND xcontext['country'] != 'unknown' THEN xcontext['country'] ELSE '' END AS country,
      |   CASE WHEN xcontext['province'] IS NOT NULL AND xcontext['province'] != 'unknown' THEN xcontext['province'] ELSE '' END AS province,
      |   CASE WHEN xcontext['ip'] IS NOT NULL AND xcontext['ip'] != 'unknown' THEN xcontext['ip'] ELSE '' END AS ip,
      |   CASE WHEN xcontext['ryos'] IS NOT NULL AND xcontext['ryos'] != 'unknown' THEN xcontext['ryos'] ELSE '' END AS os,
      |   CASE WHEN xcontext['ryosversion'] IS NOT NULL AND xcontext['ryosversion'] != 'unknown' THEN xcontext['ryosversion'] ELSE '' END AS os_version,
      |   CASE WHEN xcontext['paymenttype'] IS NOT NULL AND xcontext['paymenttype'] != 'unknown' THEN xcontext['paymenttype'] ELSE '' END AS payment_type,
      |   CASE WHEN xcontext['currencyamount'] IS NOT NULL AND xcontext['currencyamount'] != 'unknown' THEN xcontext['currencyamount'] ELSE '' END AS currency_amount,
      |   CASE WHEN xcontext['currencytype'] IS NOT NULL AND xcontext['currencytype'] != 'unknown' THEN xcontext['currencytype'] ELSE '' END AS currency_type,
      |   CASE WHEN xcontext['transactionid'] IS NOT NULL AND xcontext['transactionid'] != 'unknown' THEN xcontext['transactionid'] ELSE '' END AS transaction_id,
      |   CASE WHEN xcontext['virtualcoinamount'] IS NOT NULL AND xcontext['virtualcoinamount'] != 'unknown' THEN xcontext['virtualcoinamount'] ELSE '' END AS virtualcoin_amount,
      |   CASE WHEN xcontext['pkgname'] IS NOT NULL AND xcontext['pkgname'] != 'unknown' THEN xcontext['pkgname'] ELSE '' END AS pkgname,
      |   CASE WHEN appkey IS NOT NULL AND appkey != 'unknown' THEN appkey ELSE '' END AS app_key,
      |   CASE WHEN xcontext['app_version'] IS NOT NULL AND xcontext['app_version'] != 'unknown' THEN xcontext['app_version'] ELSE '' END AS app_version,
      |   CASE WHEN xcontext['model'] IS NOT NULL AND xcontext['model'] != 'unknown' THEN xcontext['model'] ELSE '' END AS model,
      |   CASE WHEN xcontext['resolution'] IS NOT NULL AND xcontext['resolution'] != 'unknown' THEN xcontext['resolution'] ELSE '' END AS resolution,
      |   CASE WHEN xcontext['lib_version'] IS NOT NULL AND xcontext['lib_version'] != 'unknown' THEN xcontext['lib_version'] ELSE '' END AS lib_version,
      |   CASE WHEN xcontext['carrier'] IS NOT NULL AND xcontext['carrier'] != 'unknown' THEN xcontext['carrier']
      |   ELSE '' END AS carrier,
      |   'game' AS product
      |   FROM dmp.event_game
    """.stripMargin

  def sql(product_sql: String, ds: String): String = {
    product_sql + " WHERE ds = '" + ds + "' LIMIT 1000"
  }
}
