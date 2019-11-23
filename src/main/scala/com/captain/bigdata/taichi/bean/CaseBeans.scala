package com.captain.bigdata.taichi.bean

import java.util

/**
  * CaseBeans
  *
  * @author <a href=mailto:captain_cc_2008@163.com>CaptainDP</a>
  * @date 2017/3/7 16:31
  * @func
  */
case class taichiBean(val annotation: util.Map[String, String], val dateType: util.List[util.Map[String, String]], val base: util.List[util.Map[String, String]], val clazz: util.List[util.Map[String, String]])

case class CustomBean(val taichi: String, val annotation: util.Map[String, String], val sparkConf: util.List[util.Map[String, String]], val common: util.List[util.Map[String, String]], val process: util.List[util.Map[String, Object]])

case class TextBean(val columns: util.List[util.Map[String, String]])
