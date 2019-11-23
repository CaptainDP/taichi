package com.captain.bigdata.taichi.process.stress

import com.captain.bigdata.taichi.exception.AppException
import org.apache.http.client.methods.{CloseableHttpResponse, HttpPost}
import org.apache.http.entity.StringEntity


/**
  * StressProcessSimplePost
  *
  * @author <a href=mailto:captain_cc_2008@163.com>CaptainDP</a>
  * @date 2018/4/28 18:03
  * @func
  */
class StressProcessSimplePost extends StressProcess {

  override def callService() = {
    val rspCode = call()
    rspCode
  }

  def call(): Int = {

    val url = context.inputMap.getOrElse("url", null).toString
    if (url == null) {
      throw new AppException("url is null")
    }

    var body = context.inputMap.getOrElse("body", null).toString
    if (body == null) {
      body = ""
    }

    val httpPost = new HttpPost(url)
    var response: CloseableHttpResponse = null
    var rspCode = -1

    try {

      httpPost.setEntity(new StringEntity(body))

      // send the post request
      response = httpClient.execute(httpPost)

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
