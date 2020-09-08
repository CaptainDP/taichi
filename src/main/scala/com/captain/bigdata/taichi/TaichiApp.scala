package com.captain.bigdata.taichi

import com.captain.bigdata.taichi.bean.ArgsBean
import com.captain.bigdata.taichi.config.CustomConfig
import com.captain.bigdata.taichi.constant.Constants
import com.captain.bigdata.taichi.exception.AppException
import com.captain.bigdata.taichi.log.Logging
import com.captain.bigdata.taichi.spark.Spark
import com.captain.bigdata.taichi.util.{ClassUtil, DateUtil}
import org.apache.commons.cli._
import org.apache.spark.SparkConf

import scala.collection.mutable.HashMap

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
      logger.info("args=" + args.toList)

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

  def getArgs(args: Array[String]): ArgsBean = {

    if (args != null && args.length > 0) {
      logger.info("argsStr=[" + args.mkString(" ") + "]")
    }

    val options = new Options
    options.addOption("d", true, "date yyyyMMdd [default yesterday]")
    options.addOption("a", true, "addition json")

    val customerGroup = new OptionGroup
    customerGroup.addOption(new Option("cf", true, "customer file"))
    customerGroup.addOption(new Option("cs", true, "customer json"))
    customerGroup.setRequired(true)
    options.addOptionGroup(customerGroup)

    val taichiGroup = new OptionGroup
    taichiGroup.addOption(new Option("pf", true, "taichi file [default taichi.json in jar]"))
    taichiGroup.addOption(new Option("ps", true, "taichi json [default taichi.json in jar]"))
    options.addOptionGroup(taichiGroup)

    val parser = new DefaultParser

    var date = DateUtil.toDate(DateUtil.getYesterday())
    var additionJson = ""
    var customerJson = ""
    var customerFile = ""
    var taichiFile = ""
    var taichiJson: String = null

    try {
      val cmd = parser.parse(options, args)

      //date
      if (cmd.hasOption("d")) {
        val dt = cmd.getOptionValue("d")
        date = DateUtil.toDate(dt)
      }

      //addition
      if (cmd.hasOption("a")) {
        additionJson = cmd.getOptionValue("a")
      }

      //customer
      if (cmd.hasOption("cf")) {
        val cf = cmd.getOptionValue("cf")
        customerFile = cf
      } else {
        customerJson = cmd.getOptionValue("cs")
      }

      //taichi
      if (cmd.hasOption("pf")) {
        val pf = cmd.getOptionValue("pf")
        taichiFile = pf
      } else if (cmd.hasOption("ps")) {
        taichiJson = cmd.getOptionValue("ps")
      }

    } catch {
      case e: AppException =>
        val formatter = new HelpFormatter
        formatter.printHelp("java com.captain.bigdata.taichi.TaichiApp", options)
        throw e;
    }

    ArgsBean(date, additionJson, customerJson, taichiJson, customerFile, taichiFile, "")
  }

  def doProcess(args: Array[String]): Unit = {

    val argsBean = getArgs(args)

    logger.info("argsBean=" + argsBean.toString)

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
    val customConfig = new CustomConfig(argsBean)
    logger.info("config init ok")

    //init spark
    Spark.sparkConfMap = customConfig.sparkConfMap
    val spark = Spark.session
    val context = Spark.context
    logger.info("spark init ok")

    var transferMap = HashMap[String, Any]()
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
      transferMap ++= process.context.transferMap
    }

    logger.info("process all ok")

    // stop spark
    context.stop()

  }

}
