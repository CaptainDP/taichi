package com.captain.bigdata.taichi.process.custom

import com.captain.bigdata.taichi.constant.Constants
import com.captain.bigdata.taichi.process.BaseProcess
import org.apache.spark.rdd.RDD

/**
  * CustomProcess
  *
  * @author <a href=mailto:captain_cc_2008@163.com>CaptainDP</a>
  * @date 2017/2/11 10:40
  * @func
  */
class CustomProcess extends BaseProcess {

  override def process: Unit = {

    logger.info("CustomProcess process...")
    val inputMap = context.inputMap

    val tableName = inputMap.getOrElse("rdd", null)
    if (null != tableName) {
      val rdd = context.transferMap.getOrElse(Constants.RDD + tableName, null)
      if (null != rdd && rdd.isInstanceOf[RDD[Array[String]]]) {
        rdd.asInstanceOf[RDD[Array[String]]]
          .map(line => {
            println(line.mkString(","))
          }).count()
      }
    }
  }
}
