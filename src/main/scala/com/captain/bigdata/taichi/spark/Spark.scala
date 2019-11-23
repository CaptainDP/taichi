package com.captain.bigdata.taichi.spark

import com.captain.bigdata.taichi.log.Logging
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

/**
  * Spark
  *
  * @author <a href=mailto:captain_cc_2008@163.com>CaptainDP</a>
  * @date 2017/1/26 9:20
  * @func
  */
object Spark extends Logging {

  var sparkConfMap = mutable.LinkedHashMap[String, String]()

  lazy val session = {

    logger.info("Spark session init")

    val conf = new SparkConf()

    //将sparkConfMap中spark参数设置到SparkConf
    for ((k, v) <- sparkConfMap) {
      conf.set(k, v)
    }

    if (!conf.contains("spark.app.name")) {
      val name = "myapp"
      conf.setAppName(name)
      logger.info(s"set spark app name to: $name")
    }

    if (!conf.contains("spark.master")) {
      val master = "local[*]"
      conf.setMaster(master)
      conf.set("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse")
      logger.warn(s"set spark master url to '$master' by default")
    }

    if (!conf.contains("spark.logConf")) {
      conf.set("spark.logConf", "true")
      logger.info("show spark conf as INFO log")
    }

    if (!conf.contains("spark.logLineage")) {
      conf.set("spark.logLineage", "true")
      logger.info("show rdd lineage in log")
    }

    if (!conf.contains("spark.sql.crossJoin.enabled")) {
      conf.set("spark.sql.crossJoin.enabled", "true")
      logger.info("set spark.sql.crossJoin.enabled=true")
    }

    val s = SparkSession.builder
      .enableHiveSupport
      .config(
        conf
      ).getOrCreate

    logger.info("Spark session initialized")
    s
  }

  lazy val context = session.sparkContext

}

