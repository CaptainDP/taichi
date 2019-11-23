package com.captain.bigdata.taichi.process.cmd

import com.captain.bigdata.taichi.constant.Constants
import com.captain.bigdata.taichi.exception.AppException
import com.captain.bigdata.taichi.process.BaseProcess

import scala.sys.process._

/**
  * SysCmd
  *
  * @author <a href=mailto:captain_cc_2008@163.com>CaptainDP</a>
  * @date 2017/2/2 9:52
  * @func
  */
class SysCmd extends BaseProcess {

  override def process: Unit = {
    logger.info("SysCmd process...")
    val inputMap = context.inputMap
    val cmd = inputMap.getOrElse(Constants.CMD, null)
    if (null != cmd) {
      logger.info("cmd=" + cmd)

      val result = run(cmd)
      for (out <- result._1) {
        logger.info("result stdout=" + out)
      }

      for (err <- result._2) {
        logger.info("result stderr=" + err)
      }

      if (result._3 != 0) {
        throw new AppException("cmd " + cmd + " run error code " + result._3)
      }
    }
  }

  def run(in: String): (List[String], List[String], Int) = {
    val qb = Process(in)
    var out = List[String]()
    var err = List[String]()

    val exit = qb ! ProcessLogger((s) => out ::= s, (s) => err ::= s)

    (out.reverse, err.reverse, exit)
  }

}
