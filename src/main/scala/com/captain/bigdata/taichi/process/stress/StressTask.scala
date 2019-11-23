package com.captain.bigdata.taichi.process.stress

import com.captain.bigdata.taichi.util.DateUtil

import scala.collection.mutable
import scala.concurrent.forkjoin.RecursiveTask

/**
  * StressTask
  *
  * @author <a href=mailto:captain_cc_2008@163.com>CaptainDP</a>
  * @date 2017/10/10 18:03
  * @func
  */
class StressTask(groupCycle: GroupCycleBean, baseProcess: StressBaseProcess) extends RecursiveTask[Seq[(String, String)]] {

  override def compute(): Seq[(String, String)] = {

    getDetailMap(groupCycle)

  }

  def getDetailMap(groupCycle: GroupCycleBean) = {

    val groupId = groupCycle.groupId
    val cycleNum = groupCycle.cycleNum
    var successNum = 0L
    var errorNum = 0L
    var totalNum = cycleNum
    val startTime = DateUtil.getDateTime()
    var level_50 = 0L
    var level_100 = 0L
    var level_200 = 0L
    var qps = 0L

    for (i <- 1 to cycleNum) {
      val startTime = DateUtil.getDateTimeMillis()
      val rspCode = baseProcess.callService()
      val endTime = DateUtil.getDateTimeMillis()
      val diffTimeMills = DateUtil.getMillisTimeDiff(endTime, startTime)
      if (rspCode == 0) {
        successNum += 1
      } else {
        errorNum += 1
      }

      if (diffTimeMills <= 50) {
        level_50 += 1
      }
      if (diffTimeMills <= 100) {
        level_100 += 1
      }
      if (diffTimeMills <= 200) {
        level_200 += 1
      }
    }

    val endTime = DateUtil.getDateTime()
    var diffTime = DateUtil.getTimeDiff(endTime, startTime)
    if (diffTime == 0) {
      diffTime = 1
    }
    qps = successNum / diffTime

    var map = new mutable.HashMap[String, String]()
    map("seq") = groupId.toString
    map("totalNum") = totalNum.toString
    map("successNum") = successNum.toString
    map("errorNum") = errorNum.toString
    map("firstStartTime") = startTime.toString
    map("lastStartTime") = startTime.toString
    map("firstEndTime") = endTime.toString
    map("lastEndTime") = endTime.toString
    map("diffTime") = diffTime.toString
    map("qps") = qps.toString
    map("level_50") = level_50.toString
    map("level_100") = level_100.toString
    map("level_200") = level_200.toString

    map.toSeq
  }

}
