package com.captain.bigdata.taichi.process

import com.captain.bigdata.taichi.common.AppContext
import com.captain.bigdata.taichi.constant.Constants
import com.captain.bigdata.taichi.log.Logging

/**
  * BaseProcess
  *
  * @author <a href=mailto:captain_cc_2008@163.com>CaptainDP</a>
  * @date 2017/1/31 10:07
  * @func
  */
class BaseProcess extends Logging with Serializable {

  var context: AppContext = new AppContext()

  def execute() = {

    val inputMap = context.inputMap

    logger.info("before_process inputMap=" + inputMap.toString())

    val flag = inputMap.getOrElse(Constants.FLAG, "true")
    if (flag.toString.toBoolean) {

      //动态设置 Shuffle Partition
      val shuffleInputSize = inputMap.getOrElse(Constants.SHUFFLE_INPUT_SIZE, null)
      if (null != shuffleInputSize && null != shuffleInputSize) {
        if (shuffleInputSize.equals("0")) {
          context.session.sql("set spark.sql.adaptive.enabled=false")
        } else {
          context.session.sql("set spark.sql.adaptive.enabled=true")
          context.session.sql("set spark.sql.adaptive.shuffle.targetPostShuffleInputSize=" + shuffleInputSize + "")
        }
      }

      //运行
      process()

      var df = context.df

      //重分区
      val partitionNum = inputMap.getOrElse(Constants.PARTITION_NUM, null)
      if (null != partitionNum && null != partitionNum) {
        df = df.repartition(partitionNum.toInt)
      }

      //创建临时表
      val tableName = inputMap.getOrElse(Constants.TABLE_NAME, null)
      if (null != tableName && null != df) {
        df.createOrReplaceTempView(tableName)
        logger.info("createOrReplaceTempView " + tableName + " schema:" + df.schema.toBuffer.toString())
      }

      //临时表落地hive
      val debug = inputMap.getOrElse(Constants.DEBUG, null)
      val debugTableName = inputMap.getOrElse(Constants.DEBUG_TABLE_NAME, null)
      if (null != debug && debug.toLowerCase.equals("true") && null != df && null != debugTableName) {
        logger.warn("debug mode on,data will save as table:" + debugTableName)
        context.session.sql("drop table `" + tableName + "`")
        df.write.saveAsTable(tableName);
      }

      //创建RDD
      val rddName = inputMap.getOrElse(Constants.RDD_NAME, null)
      if (null != rddName && null != df) {
        context.transferMap += (rddName -> df.rdd)
        logger.info("createRDD " + rddName)
      }

      //debug展示记录
      val show = inputMap.getOrElse(Constants.SHOW, null)
      if (null != show && show.toLowerCase.equals("true") && null != df) {
        logger.warn("show table has an impact on performance and does not recommend the production environment!!!")
        df.show(false)
      }


    } else {
      logger.warn("jump this process")
    }

    logger.info("after_process")

  }

  def process() = {
    logger.info("BaseProcess default process ...")
  }


}
