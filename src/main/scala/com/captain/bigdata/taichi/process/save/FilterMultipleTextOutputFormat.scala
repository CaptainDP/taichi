package com.captain.bigdata.taichi.process.save

import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat

/**
  * FilterMultipleTextOutputFormat
  *
  * @author <a href=mailto:captain_cc_2008@163.com>CaptainDP</a>
  * @date 2017/2/9 10:44
  * @func
  */

class FilterMultipleTextOutputFormat extends MultipleTextOutputFormat[Any, Any] {

  override def generateActualKey(key: Any, value: Any) = NullWritable.get()

  //修改默认名称
  override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String = {
    key.toString
  }
}