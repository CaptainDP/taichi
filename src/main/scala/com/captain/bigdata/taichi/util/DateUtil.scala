package com.captain.bigdata.taichi.util

import java.util.Date

import com.captain.bigdata.taichi.log.Logging
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, Duration, Period, PeriodType}

/**
  * DateUtil
  *
  * @author <a href=mailto:captain_cc_2008@163.com>CaptainDP</a>
  * @date 2017/1/25 15:13
  * @func 日期处理类
  */
object DateUtil extends Logging {

  /**
    * 获取Date类型的日期
    *
    * @param date
    * @param sourceFormat
    * @return
    */
  def toDate(date: String, sourceFormat: String = "yyyyMMdd"): Date = {
    DateTime.parse(date, DateTimeFormat.forPattern(sourceFormat)).toDate
  }

  /**
    * 获取日期
    *
    * @param date
    * @param targetFormat
    * @return
    */
  def getDate(date: Date = new Date(), targetFormat: String = "yyyyMMdd"): String = {
    val dateTime = new DateTime(date)
    dateTime.toString(targetFormat)
  }

  /**
    * 获取昨天
    *
    * @param targetFormat
    * @return
    */
  def getYesterday(targetFormat: String = "yyyyMMdd"): String = {
    val now = new DateTime()
    val dateTime = new DateTime(now.minusDays(1).toDate)
    dateTime.toString(targetFormat)
  }

  /**
    * 获取时间戳
    *
    * @param date
    * @param targetFormat
    * @return
    */
  def getDateTime(date: Date = new Date(), targetFormat: String = "yyyyMMddHHmmss"): String = {
    val dateTime = new DateTime(date)
    dateTime.toString(targetFormat)
  }

  /**
    * 获取时间戳
    *
    * @param date
    * @param targetFormat
    * @return
    */
  def getDateTimeMillis(date: Date = new Date(), targetFormat: String = "yyyyMMddHHmmssS"): String = {
    val dateTime = new DateTime(date)
    dateTime.toString(targetFormat)
  }

  /**
    * 计算两个日期差(天)
    *
    * @param date1
    * @param date2
    * @param sourceFormat 默认格式:yyyyMMdd
    * @return
    */
  def getDateDiff(date1: String, date2: String, sourceFormat: String = "yyyyMMdd"): Int = {

    val begin = DateTime.parse(date1, DateTimeFormat.forPattern(sourceFormat))
    val end = DateTime.parse(date2, DateTimeFormat.forPattern(sourceFormat))

    val p = new Period(end, begin, PeriodType.days())
    val days = p.getDays()
    days

  }

  /**
    * 计算两个时间差(秒)
    *
    * @param time1
    * @param time2
    * @param sourceFormat 默认格式:yyyyMMddHHmmss
    * @return
    */
  def getTimeDiff(time1: String, time2: String, sourceFormat: String = "yyyyMMddHHmmss"): Long = {

    val begin = DateTime.parse(time1, DateTimeFormat.forPattern(sourceFormat))
    val end = DateTime.parse(time2, DateTimeFormat.forPattern(sourceFormat))

    val diff = new Duration(end, begin)
    val time = diff.getMillis / 1000
    time

  }

  /**
    * 计算两个时间毫秒差
    *
    * @param time1
    * @param time2
    * @param sourceFormat 默认格式:yyyyMMddHHmmss
    * @return
    */
  def getMillisTimeDiff(time1: String, time2: String, sourceFormat: String = "yyyyMMddHHmmssS"): Long = {

    val begin = DateTime.parse(time1, DateTimeFormat.forPattern(sourceFormat))
    val end = DateTime.parse(time2, DateTimeFormat.forPattern(sourceFormat))

    val diff = new Duration(end, begin)
    val time = diff.getMillis
    time

  }

  /**
    * 根据指定的日期格式计算目标日期
    *
    * @param date
    * @param format 日期格式例如：yyyyMMdd(-1M)或者yyyy-MM-dd
    * @return
    */
  def calcDateByFormat(date: Date, format: String = "yyyyMMdd"): String = {

    val dateTime = new DateTime(date)
    var targetFormat: String = format
    var diff: Int = 0
    var unit: String = "D"

    if (format.contains("(") && format.contains(")")) {
      val str = format.substring(format.indexOf('('), format.indexOf(')'))
      targetFormat = format.substring(0, format.indexOf('('))
      diff = str.substring(1, str.length - 1).toInt
      unit = str.substring(str.length - 1)
    }

    logger.info(targetFormat + "," + diff + "," + "," + unit)
    val targetDateTime = unit.toUpperCase() match {
      case "Y" => dateTime.plusYears(diff)
      case "M" => dateTime.plusMonths(diff)
      case "W" => dateTime.plusWeeks(diff)
      case "D" => dateTime.plusDays(diff)
      case _ => throw new Exception("no match exception")
    }

    targetDateTime.toString(targetFormat)
  }

}
