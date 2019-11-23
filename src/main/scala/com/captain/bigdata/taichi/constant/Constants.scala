package com.captain.bigdata.taichi.constant

/**
  * Constants
  *
  * @author <a href=mailto:captain_cc_2008@163.com>CaptainDP</a>
  * @date 2017/1/25 15:20
  * @func 全局常量类
  */
object Constants {

  // 是否集群模式运行
  var CLUSTER_MODE: Boolean = false

  //此阶段处理的类
  val CLASS = "clazz"

  //是否运行此阶段
  val FLAG = "flag"

  //运行的命令
  val CMD = "cmd"

  //文件路径
  val FILE_PATH = "filePath"

  //文件名
  val FILE_NAME = "fileName"

  //表名
  val TABLE_NAME = "tableName"

  //分区个数
  val PARTITION_NUM = "partitionNum"

  //动态设置 Shuffle Partition
  val SHUFFLE_INPUT_SIZE = "shuffleInputSize"

  //rdd名称
  val RDD_NAME = "rddName"

  //sql
  val SQL = "sql"

  //分隔符
  val SPLIT = "split"

  //是否展示（仅用于debug）
  val SHOW = "show"

  //是否落地hive（仅用于debug）
  val DEBUG = "debug"

  //落地hive临时表名（仅用于debug）
  val DEBUG_TABLE_NAME = "debugTableName"

  //rdd表
  val RDD = "RDD_"

  //默认分隔符0x7F,0x5E
  var DEFAULT_SPLIT_CHAR = Array[Byte](127, 94)

}
