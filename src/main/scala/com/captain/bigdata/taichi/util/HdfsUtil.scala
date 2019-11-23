package com.captain.bigdata.taichi.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}


/**
  * HdfsUtil
  *
  * @author <a href=mailto:captain_cc_2008@163.com>CaptainDP</a>
  * @date 2017/1/26 9:30
  * @func HFDS文件系统的常用工具类
  */

object HdfsUtil {

  /**
    * 判断目录或文件是否存在
    *
    * @param path
    * @param hadoopConfig
    * @return
    */
  def exists(path: String)(implicit hadoopConfig: Configuration): Boolean = {
    val fs = FileSystem.get(hadoopConfig)
    fs.exists(new Path(path))
  }

  /**
    * 移除目录或文件
    *
    * @param path
    * @param recursive
    * @param hadoopConfig
    * @return
    */
  def remove(path: String, recursive: Boolean = true)(implicit hadoopConfig: Configuration): Boolean = {
    val fs = FileSystem.get(hadoopConfig)
    fs.delete(new Path(path), recursive)
  }

  /**
    * 创建一个空文件
    *
    * @param path
    * @param recursive
    * @param hadoopConfig
    */
  def touch(path: String, recursive: Boolean = true)(implicit hadoopConfig: Configuration): Unit = {
    val fs = FileSystem.get(hadoopConfig)
    fs.create(new Path(path)).close()
  }

  /**
    * 创建多层目录
    *
    * @param path
    * @param hadoopConfig
    * @return
    */
  def mkdirs(path: String)(implicit hadoopConfig: Configuration): Boolean = {
    val fs = FileSystem.get(hadoopConfig)
    fs.mkdirs(new Path(path))
  }

}
