package com.captain.bigdata.taichi.bean

import java.util
import java.util.Date

/**
  * CaseBeans
  *
  * @author <a href=mailto:captain_cc_2008@163.com>CaptainDP</a>
  * @date 2017/3/7 16:31
  * @func
  */
case class TaichiBean(var annotation: util.Map[String, String], var dateType: util.List[util.Map[String, String]], var base: util.List[util.Map[String, String]], var clazz: util.List[util.Map[String, String]])

case class CustomBean(var taichi: String, var annotation: util.Map[String, String], var sparkConf: util.List[util.Map[String, String]], var common: util.List[util.Map[String, String]], var process: util.List[util.Map[String, Object]])

case class TextBean(var columns: util.List[util.Map[String, String]])

case class ArgsBean(date: Date, additionJson: String, customerJson: String, taichiJson: String, customerFile: String, taichiFile: String, var customerPath: String)
