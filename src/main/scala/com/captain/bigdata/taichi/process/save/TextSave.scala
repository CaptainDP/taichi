package com.captain.bigdata.taichi.process.save

import com.captain.bigdata.taichi.constant.Constants
import com.captain.bigdata.taichi.process.BaseProcess
import com.captain.bigdata.taichi.util.HdfsUtil
import org.apache.commons.lang3.StringUtils

import scala.collection.mutable.ArrayBuffer

/**
  * TextSave
  *
  * @author <a href=mailto:captain_cc_2008@163.com>CaptainDP</a>
  * @date 2017/1/31 10:42
  * @func
  */
class TextSave extends BaseProcess {

  override def process: Unit = {

    logger.info("TextSave process...")
    val inputMap = context.inputMap

    implicit val hadoopConfig = context.session.sparkContext.hadoopConfiguration
    val filePath = inputMap.getOrElse(Constants.FILE_PATH, null)
    val fileName = inputMap.getOrElse(Constants.FILE_NAME, null)
    val sql = inputMap.getOrElse(Constants.SQL, null)
    val split = inputMap.getOrElse(Constants.SPLIT, null)
    val splitChar = getWriteSplit(split)

    val path = filePath + fileName
    logger.info("file_save_path_name=" + path)
    logger.info("output path=" + filePath + ",filename=" + fileName)
    HdfsUtil.remove(filePath)

    val df = context.session.sql(sql)

    logger.info("output split char=" + splitChar + "]")
    df.rdd.map(
      _.toSeq
    ).map(
      _.map(f => {
        if (f.isInstanceOf[BigDecimal] || f.isInstanceOf[java.math.BigDecimal])
          f.formatted("%.2f")
        else
          f
      })
    ).map(s => (fileName,
      s.mkString(splitChar))
    ).partitionBy(
      new KeyPartitioner(1)
    ).saveAsHadoopFile(filePath, classOf[String],
      classOf[String],
      classOf[FilterMultipleTextOutputFormat]
    )

    context.df = df
  }

  def getWriteSplit(input: String): String = {

    if (StringUtils.isBlank(input)) {
      return new String(Constants.DEFAULT_SPLIT_CHAR)
    }

    var parsed = input
    val upper = input.toUpperCase()
    if (upper.startsWith("0X")) {
      val hex = upper.substring(2)
      var array: ArrayBuffer[Byte] = ArrayBuffer[Byte]()
      for (i <- 0 to hex.length / 2 by 2) {
        val h = hex.substring(i, i + 2)
        val b = java.lang.Byte.parseByte(h, 16)
        array += b
      }
      parsed = new String(array.toArray)
    }
    parsed
  }
}
