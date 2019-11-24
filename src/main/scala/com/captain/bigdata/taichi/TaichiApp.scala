package com.captain.bigdata.taichi

import com.captain.bigdata.taichi.config.CustomConfig
import com.captain.bigdata.taichi.constant.Constants
import com.captain.bigdata.taichi.exception.AppException
import com.captain.bigdata.taichi.log.Logging
import com.captain.bigdata.taichi.spark.Spark
import com.captain.bigdata.taichi.util.{ClassUtil, DateUtil}
import org.apache.spark.SparkConf

/**
  * taichi
  *
  * @author <a href=mailto:captain_cc_2008@163.com>CaptainDP</a>
  * @date 2017/1/26 9:56
  * @func 数据处理主入口
  */
object TaichiApp extends Logging {

  def main(args: Array[String]): Unit = {

    var flag = false
    val beginTime = DateUtil.getDateTime()
    logger.info("----------------------taichi start-----------------------")

    try {
      doProcess(args)
      flag = true
    } catch {
      case e: AppException => logger.error("AppException", e)
      case e: RuntimeException => logger.error("RuntimeException", e)
      case e: Exception => logger.error("Exception", e)
      case e: Error => logger.error("Error", e)
    } finally {
      val endTime = DateUtil.getDateTime()
      val diffTime = DateUtil.getTimeDiff(endTime, beginTime)

      if (!flag) {
        logger.error("----------------------error cost time[" + diffTime + "]s-----------------------")
        System.exit(1)
      } else {
        logger.info("----------------------success cost time[" + diffTime + "]s-----------------------")
      }
    }
  }

  def doProcess(args: Array[String]): Unit = {

    if (args.length < 2) {
      println("usage:com.captain.bigdata.taichi.taichi.main date confFile [jsonParam]")
      System.exit(1)
    }

    val dt = args(0)
    val confFile = args(1)
    var jsonParam = "{}"
    if (args.length >= 3) {
      jsonParam = args(2)
    }

    val date = DateUtil.toDate(dt)

    logger.info("confFile=" + confFile + ",date=" + dt + ",jsonParam=" + jsonParam)

    val conf = new SparkConf()
    val master = conf.get("spark.master", null)
    logger.info("master=" + master)
    val mode = conf.get("spark.submit.deployMode", "")
    logger.info("deployMode=" + mode)
    if (mode.equalsIgnoreCase("cluster")) {
      Constants.CLUSTER_MODE = true
    }
    logger.info("Constants.CLUSTER_MODE=" + Constants.CLUSTER_MODE)

    //init conf
    val customConfig = new CustomConfig(date, confFile, jsonParam)
    logger.info("config init ok")

    //init spark
    Spark.sparkConfMap = customConfig.sparkConfMap
    val spark = Spark.session
    val context = Spark.context
    logger.info("spark init ok")

    var transferMap = Map[String, Any]()
    // process
    for (map <- customConfig.processList) {
      val clazz = map.getOrElse(Constants.CLASS, "com.captain.bigdata.taichi.process.BaseProcess")
      val process = ClassUtil.getClass(clazz)
      process.context.session = spark
      process.context.inputMap = customConfig.baseMap.toMap
      process.context.inputMap ++= map
      process.context.transferMap = transferMap
      logger.info("before process transferMap=" + transferMap)
      process.execute()
      transferMap = transferMap ++ process.context.transferMap
    }

    logger.info("process all ok")

    // stop spark
    context.stop()

  }

}
