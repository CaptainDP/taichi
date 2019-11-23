package com.captain.bigdata.taichi.config

import java.io.File
import java.net.URL
import java.util
import java.util.Date

import com.captain.bigdata.taichi.bean.taichiBean
import com.captain.bigdata.taichi.log.Logging
import com.captain.bigdata.taichi.util.{DateUtil, JsonUtil}
import org.apache.commons.lang3.StringUtils

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * taichiConfig
  *
  * @author <a href=mailto:captain_cc_2008@163.com>CaptainDP</a>
  * @date 2017/1/30 16:23
  * @func
  */
class taichiConfig extends Logging {

  var baseMap = new mutable.LinkedHashMap[String, String]

  def init(date: Date, confFile: String, jsonParam: String): taichiConfig = {

    var file: URL = null
    if (!StringUtils.isBlank(confFile)) {
      file = new URL(confFile)
    } else {
      file = new File("/taichi.json").toURI.toURL
    }

    val param = JsonUtil.getBeanFromJson(jsonParam, classOf[util.HashMap[String, String]])

    for (entry <- param.entrySet()) {
      val key = entry.getKey.toString
      val value = entry.getValue.toString
      logger.info(key + " -> " + value)
      baseMap(key) = value
    }

    val taichiBean = JsonUtil.getBeanFromUrl(file, classOf[taichiBean])
    val annotation = taichiBean.annotation
    val dateType = taichiBean.dateType
    val base = taichiBean.base
    val clazz = taichiBean.clazz

    for (entry <- annotation.entrySet) {
      val key = entry.getKey.toString
      val value = entry.getValue.toString
      logger.info(key + " -> " + value)
      baseMap(key) = value
    }


    for (map <- dateType) {
      for (entry <- map.entrySet) {
        val key = entry.getKey.toString
        val value = DateUtil.calcDateByFormat(date, entry.getValue.toString)
        logger.info(key + " " + entry.getValue + " -> " + value)
        baseMap(key) = value
      }
    }

    for (map <- base) {
      for (entry <- map.entrySet) {
        val key = entry.getKey.toString
        val value = entry.getValue.toString
        logger.info(key + " -> " + value)
        baseMap(key) = value
      }
    }

    for (map <- clazz) {
      for (entry <- map.entrySet) {
        val key = entry.getKey.toString
        val value = entry.getValue.toString
        logger.info(key + " -> " + value)
        baseMap(key) = value
      }
    }

    this

  }

}
