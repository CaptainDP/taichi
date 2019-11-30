package com.captain.bigdata.taichi.config

import java.io.File

import com.captain.bigdata.taichi.bean.{ArgsBean, CustomBean}
import com.captain.bigdata.taichi.log.Logging
import com.captain.bigdata.taichi.util.{FileUtil, JsonUtil, StringUtil, UrlUtil}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


/**
  * CustomConfig
  *
  * @author <a href=mailto:captain_cc_2008@163.com>CaptainDP</a>
  * @date 2017/1/30 16:24
  * @func
  */
class CustomConfig(argsBean: ArgsBean) extends TaichiConfig with Logging {

  val customConfig = getCustomConfig(argsBean)
  val customTaichiFile = customConfig.taichi
  val customAnnotation = customConfig.annotation
  val sparkConf = customConfig.sparkConf
  val common = customConfig.common
  val process = customConfig.process
  val sparkConfMap = mutable.LinkedHashMap[String, String]()
  var processList = ArrayBuffer[Map[String, String]]()

  val commomConf = new TaichiConfig().init(customTaichiFile, argsBean)
  baseMap = commomConf.baseMap

  if (null != customAnnotation) {
    for (entry <- customAnnotation.entrySet) {
      val key = entry.getKey.toString
      val value = entry.getValue.toString
      logger.info(key + " -> " + value)
    }
  }

  if (null != sparkConf) {
    for (map <- sparkConf) {
      for (entry <- map.entrySet) {
        val key = entry.getKey.toString
        val value = entry.getValue.toString
        logger.info(key + ": " + value)
        sparkConfMap(key) = value
      }
    }
  }

  if (null != common) {
    for (map <- common) {
      for (entry <- map.entrySet) {
        val key = entry.getKey.toString
        val value = entry.getValue.toString
        val newValue = StringUtil.stringReplace(value, baseMap)
        logger.info(key + ": " + value + " -> " + newValue)
        baseMap(key) = newValue
      }
    }
  }

  if (null != process) {
    for (map <- process) {
      var tmp = Map[String, String]()
      for (entry <- map.entrySet) {
        val key = entry.getKey.toString
        val value = entry.getValue.toString
        val newValue = StringUtil.stringReplace(value, baseMap)
        logger.info(key + ": " + value + " -> " + newValue)
        tmp += (key -> newValue)
      }
      processList += tmp
    }
  }

  def getCustomConfig(argsBean: ArgsBean) = {

    var customerJson = argsBean.customerJson

    if (argsBean.customerJson == null || argsBean.customerJson.trim.equals("")) {
      val url = UrlUtil.get(argsBean.customerFile)
      logger.info("customerJsonPath=" + url)
      customerJson = FileUtil.readFileAll(url)
      val parentPath = new File(url.toURI).getParent
      argsBean.customerPath = parentPath
    }

    val customConfig = JsonUtil.getBeanFromJson(customerJson, classOf[CustomBean])
    customConfig

  }

}