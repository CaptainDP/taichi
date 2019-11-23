package com.captain.bigdata.taichi.process.transfer

import com.captain.bigdata.taichi.constant.Constants
import com.captain.bigdata.taichi.exception.AppException
import com.captain.bigdata.taichi.process.BaseProcess

/**
  * SqlTransfer
  *
  * @author <a href=mailto:captain_cc_2008@163.com>CaptainDP</a>
  * @date 2017/1/31 10:44
  * @func
  */
class SqlTransfer extends BaseProcess {

  override def process: Unit = {

    logger.info("SqlTransfer process...")
    val inputMap = context.inputMap
    val sql = inputMap.getOrElse(Constants.SQL, null)

    if (null != sql) {
      logger.info("sql=" + sql)
      try {
        context.df = context.session.sql(sql)

      } catch {
        case e: java.lang.InterruptedException => throw new AppException("error_sql=" + sql, e)
      }
    }

    extProcess
  }

  def extProcess() = {
    logger.info("SqlTransfer default extProcess ...")
  }

}
