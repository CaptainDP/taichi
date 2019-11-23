package com.captain.bigdata.taichi.process.udf

import org.apache.hadoop.hive.ql.exec.UDF

/**
  * UDFSample
  *
  * @author <a href=mailto:captain_cc_2008@163.com>CaptainDP</a>
  * @date 2017/12/4 10:44
  * @func
  */
class UDFSample extends UDF {

  def evaluate(record: String): String = {

    var result: String = ""
    if (record != null) {
      result = record.reverse
    }

    result
  }

}
