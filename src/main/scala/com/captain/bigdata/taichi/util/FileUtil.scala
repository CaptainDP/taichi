package com.captain.bigdata.taichi.util

import java.io.File
import java.net.URL

import scala.io.Source


/**
  * FileUtil
  *
  * @author <a href=mailto:captain_cc_2008@163.com>CaptainDP</a>
  * @date 2017/1/25 15:15
  * @func 文件处理类
  */

object FileUtil {

  def readFileAll(fileName: URL): String = {

    val file = new File(fileName.toURI)

    var sb = new StringBuilder()
    Source.fromFile(file)("UTF-8").foreach(
      s => {
        sb.append(s)
      }
    )

    sb.toString()

  }

}
