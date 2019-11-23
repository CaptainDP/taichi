package com.captain.bigdata.taichi.process.stress

import com.captain.bigdata.taichi.process.BaseProcess
import com.captain.bigdata.taichi.util.DateUtil
import org.apache.http.impl.client.HttpClients
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * StressProcess
  *
  * @author <a href=mailto:captain_cc_2008@163.com>CaptainDP</a>
  * @date 2017/10/9 10:40
  * @func
  */
class StressProcess extends BaseProcess with StressBaseProcess {

  override def process: Unit = {

    val sc = context.session.sparkContext

    logger.info("StressProcess process...")
    val inputMap = context.inputMap

    val groupNum = inputMap.getOrElse("groupNum", 1).toString.toInt
    val cycleNum = inputMap.getOrElse("cycleNum", 1).toString.toInt
    val threadNum = inputMap.getOrElse("threadNum", 1).toString.toInt

    var groupCycle = new ArrayBuffer[GroupCycleBean]()
    for (i <- 1 to groupNum) {
      groupCycle += GroupCycleBean(groupNum, i, cycleNum, 0, threadNum, 0)
    }

    val groupCycleRDD = sc.parallelize(groupCycle, groupNum)
    val detail = groupCycleRDD.map(x => {


      lazy val connectionManager = new PoolingHttpClientConnectionManager()
      // 将最大连接数增加到500-需要根据业务量评估
      connectionManager.setMaxTotal(500)
      // 将每个路由基础的连接增加到100-需要根据业务量评估
      connectionManager.setDefaultMaxPerRoute(100)

      var httpClient = HttpClients.custom()
        .setConnectionManager(connectionManager)
        .build()

      val taskList = new ArrayBuffer[StressTask]()
      //thread
      for (i <- 1 to threadNum) {

        this.httpClient = httpClient
        val task = new StressTask(x, this)
        task.fork()
        taskList += task
      }

      var taskResult: ArrayBuffer[Seq[(String, String)]] = new ArrayBuffer[Seq[(String, String)]]()

      for (task <- taskList) {
        val result: Seq[(String, String)] = task.join()
        println(result)
        taskResult += result
      }

      val stressSumMap = getSumMap(taskResult.toArray)

      httpClient.close()
      connectionManager.close()

      stressSumMap.toSeq

    }).collect()

    val stressSumMap = getSumMap(detail)

    val stressPerSumMap = getPerSumMap(stressSumMap.toSeq)

    val groupCycleMap = GroupCycleBean(groupNum, 0, cycleNum, 0, threadNum, 0).toMap()

    printSumMap(groupCycleMap, stressPerSumMap)

    context.transferMap += ("groupCycleMap" -> groupCycleMap)
    context.transferMap += ("StressSumMap" -> stressSumMap)
    context.transferMap += ("StressDetailMap" -> detail)

  }


  def getSumMap(detail: Array[Seq[(String, String)]]) = {

    var newFirstStartTime = "99991231000000"
    var newLastStartTime = "00000000000000"
    var newFirstEndTime = "99991231000000"
    var newLastEndTime = "00000000000000"
    var qps = 0L
    var maxDiffTime = 0L
    var totalNum = 0L
    var successNum = 0L
    var errorNum = 0L
    var level_50 = 0L
    var level_100 = 0L
    var level_200 = 0L
    var crossTime = 0D
    var noCrossTime = 0D

    detail.foreach(x => {

      var detailMap = x

      printDetailMap(detailMap)

      val map = detailMap.toMap[String, String]

      totalNum += map("totalNum").toString.toLong
      successNum += map("successNum").toString.toLong
      errorNum += map("errorNum").toString.toLong
      level_50 += map("level_50").toString.toLong
      level_100 += map("level_100").toString.toLong
      level_200 += map("level_200").toString.toLong
      qps += map("qps").toString.toLong

      val firstStartTime = map("firstStartTime").toString
      val lastStartTime = map("lastStartTime").toString
      val firstEndTime = map("firstEndTime").toString
      val lastEndTime = map("lastEndTime").toString

      if (newFirstStartTime > firstStartTime) {
        newFirstStartTime = firstStartTime
      }
      if (newLastStartTime < lastStartTime) {
        newLastStartTime = lastStartTime
      }
      if (newFirstEndTime > firstEndTime) {
        newFirstEndTime = firstEndTime
      }
      if (newLastEndTime < lastEndTime) {
        newLastEndTime = lastEndTime
      }

    })

    maxDiffTime = DateUtil.getTimeDiff(newLastEndTime, newFirstStartTime)
    crossTime = DateUtil.getTimeDiff(newFirstEndTime, newLastStartTime)
    noCrossTime = maxDiffTime - crossTime

    var stressSumMap = new mutable.HashMap[String, String]()
    stressSumMap("totalNum") = totalNum.toString
    stressSumMap("successNum") = successNum.toString
    stressSumMap("errorNum") = errorNum.toString
    stressSumMap("firstStartTime") = newFirstStartTime.toString
    stressSumMap("lastStartTime") = newLastStartTime.toString
    stressSumMap("firstEndTime") = newFirstEndTime.toString
    stressSumMap("lastEndTime") = newLastEndTime.toString
    stressSumMap("maxDiffTime") = maxDiffTime.toString
    stressSumMap("crossTime") = crossTime.toString
    stressSumMap("noCrossTime") = noCrossTime.toString
    stressSumMap("qps") = qps.toString
    stressSumMap("level_50") = level_50.toString
    stressSumMap("level_100") = level_100.toString
    stressSumMap("level_200") = level_200.toString

    stressSumMap
  }

  def getPerSumMap(detailMap: Seq[(String, String)]) = {

    printDetailMap(detailMap)

    val map = detailMap.toMap[String, String]

    val firstStartTime = map("firstStartTime").toString
    val lastStartTime = map("lastStartTime").toString
    val firstEndTime = map("firstEndTime").toString
    val lastEndTime = map("lastEndTime").toString
    val totalNum = map("totalNum").toString.toLong
    val successNum = map("successNum").toString.toLong
    val errorNum = map("errorNum").toString.toLong
    val level_50 = map("level_50").toString.toLong
    val level_100 = map("level_100").toString.toLong
    val level_200 = map("level_200").toString.toLong
    val qps = map("qps").toString.toLong

    val level_50_% = level_50.toDouble * 100 / totalNum.toDouble
    val level_100_% = level_100.toDouble * 100 / totalNum.toDouble
    val level_200_% = level_200.toDouble * 100 / totalNum.toDouble
    val success_% = successNum.toDouble * 100 / totalNum.toDouble

    val maxDiffTime = DateUtil.getTimeDiff(lastEndTime, firstStartTime)
    val crossTime = DateUtil.getTimeDiff(firstEndTime, lastStartTime)
    val noCrossTime = maxDiffTime - crossTime
    var trust_% = 0D
    if (crossTime <= 0 || maxDiffTime.toLong == 0) {
      trust_% = 0D
    } else {
      trust_% = crossTime.toDouble * 100 / maxDiffTime.toDouble
    }

    var stressSumMap = new mutable.HashMap[String, String]()
    stressSumMap("totalNum") = totalNum.toString
    stressSumMap("successNum") = successNum.toString
    stressSumMap("errorNum") = errorNum.toString
    stressSumMap("success_%") = success_%.toString
    stressSumMap("firstStartTime") = firstStartTime.toString
    stressSumMap("lastStartTime") = lastStartTime.toString
    stressSumMap("firstEndTime") = firstEndTime.toString
    stressSumMap("lastEndTime") = lastEndTime.toString
    stressSumMap("maxDiffTime") = maxDiffTime.toString
    stressSumMap("crossTime") = crossTime.toString
    stressSumMap("noCrossTime") = noCrossTime.toString
    stressSumMap("trust_%") = trust_%.toString
    stressSumMap("qps") = qps.toString
    stressSumMap("level_50") = level_50.toString
    stressSumMap("level_100") = level_100.toString
    stressSumMap("level_200") = level_200.toString
    stressSumMap("level_50_%") = level_50_%.toString
    stressSumMap("level_100_%") = level_100_%.toString
    stressSumMap("level_200_%") = level_200_%.toString

    stressSumMap
  }


  def printDetailMap(stressDetailMap: Seq[(String, String)]): Unit = {
    println(stressDetailMap)
  }

  def printSumMap(groupCycleMap: mutable.HashMap[String, String], stressSumMap: mutable.HashMap[String, String]): Unit = {

    println("--------------------------------------")
    println("groupNum:        " + groupCycleMap("groupNum"))
    println("cycleNum:        " + groupCycleMap("cycleNum"))
    println("threadNum:       " + groupCycleMap("threadNum"))
    println()
    println("totalNum:        " + stressSumMap("totalNum"))
    println("successNum:      " + stressSumMap("successNum"))
    println("errorNum:        " + stressSumMap("errorNum"))
    println()
    println("success_%:       " + stressSumMap("success_%"))
    println("qps:             " + stressSumMap("qps"))
    println()
    println("firstStartTime:  " + stressSumMap("firstStartTime"))
    println("lastStartTime:   " + stressSumMap("lastStartTime"))
    println("firstEndTime:    " + stressSumMap("firstEndTime"))
    println("lastEndTime:     " + stressSumMap("lastEndTime"))
    println()
    println("trust_%:         " + stressSumMap("trust_%"))
    println("maxDiffTime:     " + stressSumMap("maxDiffTime"))
    println("crossTime:       " + stressSumMap("crossTime"))
    println("noCrossTime:     " + stressSumMap("noCrossTime"))
    println()
    println("level_50:        " + stressSumMap("level_50"))
    println("level_100:       " + stressSumMap("level_100"))
    println("level_200:       " + stressSumMap("level_200"))
    println()
    println("level_50_%:      " + stressSumMap("level_50_%"))
    println("level_100_%:     " + stressSumMap("level_100_%"))
    println("level_200_%:     " + stressSumMap("level_200_%"))
    println("--------------------------------------")

  }


}

case class GroupCycleBean(
                           var groupNum: Int = 0,
                           var groupId: Int = 0,
                           var cycleNum: Int = 0,
                           var cycleId: Int = 0,
                           var threadNum: Int = 0,
                           var threadId: Int = 0
                         ) {
  def toMap() = {

    var groupCycleMap = new mutable.HashMap[String, String]()
    groupCycleMap("groupNum") = groupNum.toString
    groupCycleMap("groupId") = groupId.toString
    groupCycleMap("cycleNum") = cycleNum.toString
    groupCycleMap("cycleId") = cycleId.toString.toString
    groupCycleMap("threadNum") = threadNum.toString
    groupCycleMap("threadId") = threadId.toString

    groupCycleMap
  }
}
