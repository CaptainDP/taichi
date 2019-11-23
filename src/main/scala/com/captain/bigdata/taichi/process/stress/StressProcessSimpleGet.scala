package com.captain.bigdata.taichi.process.stress

import java.util

import com.captain.bigdata.taichi.exception.AppException
import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet}


/**
  * StressProcessSimpleGet
  *
  * @author <a href=mailto:captain_cc_2008@163.com>CaptainDP</a>
  * @date 2017/10/8 18:03
  * @func
  */
class StressProcessSimpleGet extends StressProcess {

  override def callService() = {
    val rspCode = call()
    rspCode
  }

  def call(): Int = {

    val url = context.inputMap.getOrElse("url", null).toString
    if (url == null) {
      throw new AppException("url is null")
    }

    val httpGet = new HttpGet(url)
    var map = new util.LinkedHashMap
    var response: CloseableHttpResponse = null
    var rspCode = -1

    try {

      // send the post request
      response = httpClient.execute(httpGet)

      rspCode = getRspCode(response)

    } catch {
      case e: Exception => {
        rspCode = 1
      }
    } finally {
      if (response != null) {
        response.close()
      }
    }
    rspCode
  }

  def getRspCode(response: CloseableHttpResponse): Int = {
    var rspCode = -1
    if (response.getStatusLine().getStatusCode() == 200) {
      rspCode = 0
    } else {
      rspCode = 2
    }
    rspCode
  }

}
