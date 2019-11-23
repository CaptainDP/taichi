package com.captain.bigdata.taichi.process.cmd

import com.captain.bigdata.taichi.constant.Constants
import com.captain.bigdata.taichi.exception.AppException
import com.captain.bigdata.taichi.process.BaseProcess
import com.captain.bigdata.taichi.util.HdfsUtil

/**
  * HdfsCmd
  *
  * @author <a href=mailto:captain_cc_2008@163.com>CaptainDP</a>
  * @date 2017/2/2 10:44
  * @func
  */
class HdfsCmd extends BaseProcess {

  val RM = "rm"
  val MK_DIR = "mkdir"
  val TOUCH = "touch"

  override def process: Unit = {
    logger.info("HdfsCmd process...")
    val inputMap = context.inputMap
    implicit val hadoopConfig = context.session.sparkContext.hadoopConfiguration
    val cmd = inputMap.getOrElse(Constants.CMD, null)
    if (null != cmd) {
      logger.info("cmd=" + cmd)
      val opts = cmd.trim.split("\\s+")
      if (opts.length > 1) {
        opts(0) match {
          case RM => HdfsUtil.remove(opts(1))
          case MK_DIR => HdfsUtil.mkdirs(opts(1))
          case TOUCH => HdfsUtil.touch(opts(1))
          case _ => throw new AppException("cannot support cmd " + cmd)
        }
      }
    }
  }
}
