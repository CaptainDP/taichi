package com.captain.bigdata.taichi.common

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

/**
  * AppContext
  *
  * @author <a href=mailto:captain_cc_2008@163.com>CaptainDP</a>
  * @date 2017/1/31 10:33
  * @func
  */
class AppContext extends Serializable {

  /**
    * 输入参数
    */
  var inputMap = Map[String, String]()

  /**
    * spark session
    */
  var session: SparkSession = null

  /**
    * DataFrame
    */
  var df: DataFrame = null

  /**
    * transferMap
    */
  var transferMap = mutable.HashMap[String, Any]()

}
