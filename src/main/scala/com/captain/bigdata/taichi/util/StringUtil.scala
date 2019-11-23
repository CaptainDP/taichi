package com.captain.bigdata.taichi.util

import java.util.regex.{Matcher, Pattern}

import scala.collection.mutable.LinkedHashMap

/**
  * StringUtil
  *
  * @author <a href=mailto:captain_cc_2008@163.com>CaptainDP</a>
  * @date 2017/1/25 15:45
  * @func
  */
object StringUtil {

  /**
    * 把传入的字符串中包含{}的部分,替换成映射中的kv对应的新值
    *
    * @param input
    * @param baseMap
    * @return
    */
  def stringReplace(input: String, baseMap: LinkedHashMap[String, String]): String = {

    if (input == null || input.trim.eq("")) {
      return ""
    }

    var result = input
    val matcherDate: Matcher = Pattern.compile("\\$\\{([^\\}]+)\\}")
      .matcher(result)
    while (matcherDate.find()) {
      val key: String = matcherDate.group(1)
      val str: String = matcherDate.group()
      if (baseMap.contains(key)) {
        result = result.replace(str, baseMap(key))
      }
    }
    result
  }

}
