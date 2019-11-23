package com.captain.bigdata.taichi.util

import com.captain.bigdata.taichi.exception.AppException
import com.captain.bigdata.taichi.log.Logging
import com.captain.bigdata.taichi.process.BaseProcess
import org.apache.commons.lang.StringUtils

/**
  * Class
  *
  * @author <a href=mailto:captain_cc_2008@163.com>CaptainDP</a>
  * @date 2017/2/1 17:33
  * @func
  */
object ClassUtil extends Logging {

  def getClass(clazz: String): BaseProcess = {

    logger.info("clazz=" + clazz)

    if (!StringUtils.isNotBlank(clazz)) {
      throw new AppException("clazz " + clazz + " is null")
    }

    val con = Class.forName(clazz).getConstructors
    val obj = con(0).newInstance()
    if (!obj.isInstanceOf[BaseProcess]) {
      throw new AppException("clazz " + clazz + " is not instance of Loader")
    }
    val loader = obj.asInstanceOf[BaseProcess]
    loader
  }

}
