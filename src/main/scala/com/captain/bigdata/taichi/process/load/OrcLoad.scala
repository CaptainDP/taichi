package com.captain.bigdata.taichi.process.load

import com.captain.bigdata.taichi.constant.Constants
import com.captain.bigdata.taichi.exception.AppException
import com.captain.bigdata.taichi.process.BaseProcess

/**
  * OrcLoad
  *
  * @author <a href=mailto:captain_cc_2008@163.com>CaptainDP</a>
  * @date 2017/2/11 17:47
  * @func
  */
class OrcLoad extends BaseProcess {

  override def process: Unit = {

    logger.info("OrcLoad process...")
    val inputMap = context.inputMap

    val filePath = inputMap.getOrElse(Constants.FILE_PATH, null)
    val fileName = inputMap.getOrElse(Constants.FILE_NAME, null)
    val tableName = inputMap.getOrElse(Constants.TABLE_NAME, null)

    if (null == filePath) {
      throw new AppException("filePath is null")
    }

    if (null == tableName) {
      throw new AppException("tableName is null")
    }

    val path = filePath + fileName
    logger.info("file_load_path_name=" + path)
    val df = context.session.read.orc(path).toDF()

    context.df = df

  }

}
