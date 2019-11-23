package com.captain.bigdata.taichi.log

import org.slf4j.LoggerFactory

/**
  * Logging
  *
  * @author <a href=mailto:captain_cc_2008@163.com>CaptainDP</a>
  * @date 2017/1/25 15:11
  * @func 日志处理基础类
  */
trait Logging {

  protected val logger = LoggerFactory.getLogger(this.getClass.getCanonicalName.stripSuffix("$"))

}
