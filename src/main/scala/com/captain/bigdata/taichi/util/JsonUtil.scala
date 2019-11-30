package com.captain.bigdata.taichi.util

import java.io.{File, FileReader}
import java.net.URL

import com.captain.bigdata.taichi.exception.AppException
import com.google.gson.Gson
import com.google.gson.stream.JsonReader

/**
  * JsonUtil
  *
  * @author <a href=mailto:captain_cc_2008@163.com>CaptainDP</a>
  * @date 2017/3/7 18:54
  * @func
  */
object JsonUtil {

  def getBeanFromUrl[T](url: URL, clazz: Class[T]): T = {

    val urlString = url.toURI

    if (!new File(urlString).exists()) {
      throw new AppException("url " + urlString + " is not exists")
    }
    val reader = new JsonReader(new FileReader(url.getFile))
    new Gson().fromJson(reader, clazz)
  }

  def getBeanFromFile[T](fileName: String, clazz: Class[T], customerPath: String = "."): T = {
    val url = UrlUtil.get(fileName, customerPath)
    val reader = new JsonReader(new FileReader(url.getFile))
    new Gson().fromJson(reader, clazz)
  }

  def getBeanFromJson[T](json: String, clazz: Class[T]) = {
    new Gson().fromJson(json, clazz)
  }

}
