package com.captain.bigdata.taichi.config

import java.io.File
import java.util.Date

import com.captain.bigdata.taichi.bean.CustomBean
import com.captain.bigdata.taichi.log.Logging
import com.captain.bigdata.taichi.util.{JsonUtil, StringUtil, UrlUtil}

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
class CustomConfig(date: Date, confFile: String, jsonParam: String) extends taichiConfig with Logging {

  val url = UrlUtil.get(confFile)
  val customConfig = JsonUtil.getBeanFromUrl(url, classOf[CustomBean])
  val taichi = customConfig.taichi
  val customAnnotation = customConfig.annotation
  val sparkConf = customConfig.sparkConf
  val common = customConfig.common
  val process = customConfig.process
  val sparkConfMap = mutable.LinkedHashMap[String, String]()
  var processList = ArrayBuffer[Map[String, String]]()

  var path = new File(taichi).toURI.toURL.toString

  if (taichi.startsWith(".")) {
    val parentPath = new File(url.toString).getParent
    path = parentPath + "/" + taichi
  }

  val commomConf = new taichiConfig().init(date, path, jsonParam)
  baseMap = commomConf.baseMap

  for (entry <- customAnnotation.entrySet) {
    val key = entry.getKey.toString
    val value = entry.getValue.toString
    logger.info(key + " -> " + value)
  }

  for (map <- sparkConf) {
    for (entry <- map.entrySet) {
      val key = entry.getKey.toString
      val value = entry.getValue.toString
      logger.info(key + ": " + value)
      sparkConfMap(key) = value
    }
  }

  for (map <- common) {
    for (entry <- map.entrySet) {
      val key = entry.getKey.toString
      val value = entry.getValue.toString
      val newValue = StringUtil.stringReplace(value, baseMap)
      logger.info(key + ": " + value + " -> " + newValue)
      baseMap(key) = newValue
    }
  }

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