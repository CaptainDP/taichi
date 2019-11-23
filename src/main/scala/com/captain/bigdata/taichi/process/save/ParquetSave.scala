package com.captain.bigdata.taichi.process.save

import com.captain.bigdata.taichi.constant.Constants
import com.captain.bigdata.taichi.exception.AppException
import com.captain.bigdata.taichi.process.BaseProcess
import com.captain.bigdata.taichi.util.HdfsUtil

/**
  * ParquetSave
  *
  * @author <a href=mailto:captain_cc_2008@163.com>CaptainDP</a>
  * @date 2017/2/1 17:47
  * @func
  */
class ParquetSave extends BaseProcess {

  override def process: Unit = {

    logger.info("ParquetSave process...")
    val inputMap = context.inputMap

    implicit val hadoopConfig = context.session.sparkContext.hadoopConfiguration
    val filePath = inputMap.getOrElse(Constants.FILE_PATH, null)
    val fileName = inputMap.getOrElse(Constants.FILE_NAME, null)
    val sql = inputMap.getOrElse(Constants.SQL, null)

    if (null == filePath) {
      throw new AppException("filePath is null")
    }

    if (null == sql) {
      throw new AppException("sql is null")
    }

    val path = filePath + fileName
    logger.info("file_save_path_name=" + path)
    HdfsUtil.remove(path)
    val df = context.session.sql(sql)
    df.write.parquet(path)

    context.df = df

  }

}
