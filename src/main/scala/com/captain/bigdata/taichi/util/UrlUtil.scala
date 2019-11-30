package com.captain.bigdata.taichi.util

import java.io.File
import java.net.URL
import java.nio.file.{Files, Paths}

import com.captain.bigdata.taichi.constant.Constants
import com.captain.bigdata.taichi.exception.AppException
import com.captain.bigdata.taichi.log.Logging


/**
  * UrlUtil
  *
  * @author <a href=mailto:captain_cc_2008@163.com>CaptainDP</a>
  * @date 2017/1/25 15:15
  * @func URL路径处理类
  */
object UrlUtil extends Logging {

  /**
    * 获取文件的可读路径
    *
    * @param inputFileName
    * @return
    */
  def get(inputFileName: String, customerPath: String = "."): URL = {

    if (null == inputFileName) {
      throw new AppException("inputFileName is null")
    }

    var fileName = inputFileName
    var url: URL = null
    var p: URL = null

    if (Constants.CLUSTER_MODE) {
      var index = fileName.lastIndexOf("/").max(fileName.lastIndexOf("\\"))
      logger.info("index=[" + index + "]")
      if (index > 0) {
        index += 1
      } else {
        index = 0
      }
      fileName = fileName.substring(index)
      logger.info("cluster mode inputFileName=[" + inputFileName + "] to [" + fileName + "]")
    }

    if (inputFileName.startsWith(".")) {
      logger.info("try load 0 file in customer path " + customerPath + " or absolute path")
      val f = customerPath + "/" + inputFileName
      val path = Paths.get(f)
      if (Files.exists(path)) {
        url = new File(f).toURI.toURL
      }
    }

    if (url == null) {

      p = new File("").toURI.toURL

      logger.info("try load 1 file in relative path " + p + " or absolute path")

      val path = Paths.get(fileName)
      if (Files.exists(path)) {
        url = new File(fileName).toURI.toURL
      }
    }

    if (url == null) {
      p = this.getClass.getResource("")
      logger.info("try 2.1 load file in path " + p)
      p = this.getClass.getResource("/")
      logger.info("try 2.2 load file in path " + p)
      url = this.getClass.getResource(fileName)
    }

    if (url == null) {
      p = Thread.currentThread().getContextClassLoader.getResource("")
      logger.info("try 3 load file in path" + p)
      url = Thread.currentThread().getContextClassLoader.getResource(fileName)
    }

    if (url == null) {
      p = ClassLoader.getSystemResource("")
      logger.info("try 4 load file in path" + p)
      url = ClassLoader.getSystemResource(fileName)
    }

    if (null == url) {
      throw new AppException("can not find file " + inputFileName)
    }

    logger.info("load inputFileName=[" + inputFileName + "],url=[" + url + "] success")

    url
  }
}
