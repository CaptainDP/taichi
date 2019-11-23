package com.captain.bigdata.taichi.process.save

import org.apache.spark.Partitioner

/**
  * KeyPartitioner
  *
  * @author <a href=mailto:captain_cc_2008@163.com>CaptainDP</a>
  * @date 2017/2/9 10:44
  * @func
  */
class KeyPartitioner(num: Int) extends Partitioner {

  override def numPartitions: Int = num

  override def getPartition(key: Any): Int = {
    num - 1
  }
}