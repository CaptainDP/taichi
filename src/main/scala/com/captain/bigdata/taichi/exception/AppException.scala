package com.captain.bigdata.taichi.exception

/**
  * AppException
  *
  * @author <a href=mailto:captain_cc_2008@163.com>CaptainDP</a>
  * @date 2017/1/25 15:09
  * @func 异常处理基础类
  */
class AppException(message: String, e: Throwable) extends RuntimeException(message: String, e: Throwable) {

  def this(message: String) {
    this(message, null)
  }
  
}
