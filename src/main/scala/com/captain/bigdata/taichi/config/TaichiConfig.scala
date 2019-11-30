package com.captain.bigdata.taichi.config

import java.util

import com.captain.bigdata.taichi.bean.{ArgsBean, TaichiBean}
import com.captain.bigdata.taichi.log.Logging
import com.captain.bigdata.taichi.util.{DateUtil, FileUtil, JsonUtil, UrlUtil}

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * TaichiConfig
  *
  * @author <a href=mailto:captain_cc_2008@163.com>CaptainDP</a>
  * @date 2017/1/30 16:23
  * @func
  */
class TaichiConfig extends Logging {

  var baseMap = new mutable.LinkedHashMap[String, String]

  def init(customTaichiFile: String, argsBean: ArgsBean): TaichiConfig = {

    //additionJson
    val addition = getAdditionConfig(argsBean)
    if (null != addition) {
      for (entry <- addition.entrySet()) {
        val key = entry.getKey.toString
        val value = entry.getValue.toString
        logger.info(key + " -> " + value)
        baseMap(key) = value
      }
    }

    //taichiJson
    var taichiBean: TaichiBean = getTaichiConfig(argsBean, customTaichiFile)

    val annotation = taichiBean.annotation
    val dateType = taichiBean.dateType
    val base = taichiBean.base
    val clazz = taichiBean.clazz
    val date = argsBean.date

    if (null != annotation) {
      for (entry <- annotation.entrySet) {
        val key = entry.getKey.toString
        val value = entry.getValue.toString
        logger.info(key + " -> " + value)
        baseMap(key) = value
      }
    }


    if (null != dateType) {
      for (map <- dateType) {
        for (entry <- map.entrySet) {
          val key = entry.getKey.toString
          val value = DateUtil.calcDateByFormat(date, entry.getValue.toString)
          logger.info(key + " " + entry.getValue + " -> " + value)
          baseMap(key) = value
        }
      }
    }

    if (null != base) {

      for (map <- base) {
        for (entry <- map.entrySet) {
          val key = entry.getKey.toString
          val value = entry.getValue.toString
          logger.info(key + " -> " + value)
          baseMap(key) = value
        }
      }
    }

    if (null != clazz) {
      for (map <- clazz) {
        for (entry <- map.entrySet) {
          val key = entry.getKey.toString
          val value = entry.getValue.toString
          logger.info(key + " -> " + value)
          baseMap(key) = value
        }
      }
    }

    this

  }

  def getAdditionConfig(argsBean: ArgsBean) = {

    var additionMap = new util.HashMap[String, String]()

    if (argsBean.additionJson == null || argsBean.additionJson.trim.equals("")) {
      additionMap = JsonUtil.getBeanFromJson(argsBean.additionJson, classOf[util.HashMap[String, String]])
    }

    additionMap

  }

  def getTaichiConfig(argsBean: ArgsBean, customTaichiFile: String) = {

    var taichiJson = argsBean.taichiJson

    if (argsBean.taichiJson == null || argsBean.taichiJson.trim.equals("")) {

      logger.info("customTaichiFile=" + customTaichiFile)

      var taichiFile = customTaichiFile

      if (argsBean.taichiFile != null && !argsBean.taichiFile.trim.equals("")) {
        taichiFile = argsBean.taichiFile
      }

      if (taichiFile == null || taichiFile.trim.equals("")) {
        taichiFile = "resources/taichi.json"
      }

      logger.info("taichiFile=" + taichiFile)

      val url = UrlUtil.get(taichiFile, argsBean.customerPath)

      logger.info("taichiFile available path=" + url)
      taichiJson = FileUtil.readFileAll(url)

    }

    val taichiConfig = JsonUtil.getBeanFromJson(taichiJson, classOf[TaichiBean])
    taichiConfig

  }

}
