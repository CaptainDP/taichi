package com.captain.bigdata.taichi.process.stress

import org.apache.http.impl.client.CloseableHttpClient

/**
  * StressBaseProcess
  *
  * @author <a href=mailto:captain_cc_2008@163.com>CaptainDP</a>
  * @date 2017/10/ 1018:03
  * @func
  */
trait StressBaseProcess {

  var httpClient: CloseableHttpClient = null

  def callService() = {
    0
  }

}