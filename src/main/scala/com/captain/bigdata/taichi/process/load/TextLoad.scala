package com.captain.bigdata.taichi.process.load

import java.util.regex.Pattern

import com.captain.bigdata.taichi.bean.TextBean
import com.captain.bigdata.taichi.constant.Constants
import com.captain.bigdata.taichi.exception.AppException
import com.captain.bigdata.taichi.process.BaseProcess
import com.captain.bigdata.taichi.util.JsonUtil
import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import scala.collection.JavaConversions._
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
  * TextLoad
  *
  * @author <a href=mailto:captain_cc_2008@163.com>CaptainDP</a>
  * @date 2017/1/31 10:40
  * @func
  */
class TextLoad extends BaseProcess {

  val COLUMNS = "columns"
  val NAME = "name"
  val TYPE = "type"
  val DESC = "desc"

  override def process: Unit = {

    logger.info("TextLoad process...")
    val inputMap = context.inputMap

    val filePath = inputMap.getOrElse(Constants.FILE_PATH, null)
    val fileName = inputMap.getOrElse(Constants.FILE_NAME, null)
    val columns = inputMap.getOrElse(COLUMNS, null)
    val split = inputMap.getOrElse(Constants.SPLIT, null)
    val splitChar = getReadSplit(split)

    if (null == filePath) {
      throw new AppException("filePath is null")
    }

    if (null == columns) {
      throw new AppException("columns is null")
    }

    logger.info("columns=" + columns)
    val text = getTextConfig("{" + COLUMNS + ":" + columns + "}", classOf[TextBean])

    logger.info("text columns:" + text.columns)
    val schema = getSchema(text)

    val path = filePath + fileName
    logger.info("file_load_path_name=" + path)
    logger.info("split=[" + split + "] to [" + splitChar + "]")

    val rddArray = getRddArray(path, splitChar)

    val tableName = inputMap.getOrElse(Constants.TABLE_NAME, null)
    if (null != tableName) {
      context.transferMap += (Constants.RDD + tableName -> rddArray)
    }

    val rddRow = getRddRowFromArray(rddArray, schema)

    val df = context.session.createDataFrame(rddRow, schema)

    context.df = df

  }

  def getReadSplit(input: String): String = {

    if (StringUtils.isBlank(input)) {
      return Pattern.quote(new String(Constants.DEFAULT_SPLIT_CHAR))
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

      parsed = Pattern.quote(new String(array.toArray))
    }
    parsed
  }

  def getSchema(text: TextBean): StructType = {

    val array = ListBuffer[StructField]()

    for (map <- text.columns) {
      val name = map.getOrDefault(NAME, null)
      val tp = map.getOrDefault(TYPE, null)
      val desc = map.getOrDefault(DESC, null)

      if (StringUtils.isBlank(name)) {
        throw new AppException(map.toString + " name is null")
      }

      array += StructField(name, getFormatType(tp))
    }

    StructType(array)
  }

  def getFormatType(replace: String): DataType = {

    if (StringUtils.isBlank(replace)) {
      return StringType
    }

    var replaceTmp = ""
    if (replace.indexOf("(") == -1) {
      replaceTmp = replace
    } else {
      replaceTmp = replace.substring(0, replace.indexOf("("))
    }

    val tp = replaceTmp.toUpperCase() match {
      case "STRING" => StringType
      case "BINARY" => BinaryType
      case "BOOLEAN" => BooleanType
      case "DATE" => DateType
      case "TIMESTAMP" => TimestampType
      case "DECIMAL" =>
        if (replace.indexOf("(") == -1) {
          DataTypes.createDecimalType()
        } else {
          DataTypes.createDecimalType(replace.substring(replace.indexOf("(") + 1, replace.indexOf(",")).toInt, replace.substring(replace.indexOf(",") + 1, replace.indexOf(")")).toInt)
        }
      case "BIGDECIMAL" =>
        if (replace.indexOf("(") == -1) {
          new DecimalType()
        } else {
          new DecimalType(replace.substring(replace.indexOf("(") + 1, replace.indexOf(",")).toInt, replace.substring(replace.indexOf(",") + 1, replace.indexOf(")")).toInt)
        }
      case "CALENDARINTERVAL" => DataTypes.CalendarIntervalType
      case "DOUBLE" => DataTypes.DoubleType
      case "FLOAT" => DataTypes.FloatType
      case "BYTE" => DataTypes.ByteType
      case "INTEGER" => DataTypes.IntegerType
      case "INT" => DataTypes.IntegerType
      case "BIGINT" => DataTypes.LongType
      case "LONG" => DataTypes.LongType
      case "SHORT" => DataTypes.ShortType
      case "NULL" => DataTypes.NullType
      case _ => throw new AppException("no match colType exception")
    }

    tp
  }

  def getRddArray(path: String, splitChar: String): RDD[Array[String]] = {
    context.session.sparkContext.textFile(path).map(line => line.split(splitChar, -1))
  }

  def getRddRowFromArray(rddArray: RDD[Array[String]], schema: StructType): RDD[Row] = {

    val rddRow = rddArray.map(f = attributes => {
      var arr = ArrayBuffer[Any]()
      var index = 0

      if (attributes.length < schema.length) {
        throw new AppException("schema_length=[" + schema.length + "] < attributes.length=[" + attributes.length + "], data error=[" + attributes.toBuffer + "]")
      }

      for (i <- schema) {
        val value = attributes(index)
        var newValue = value
        i.dataType match {
          case StringType => arr += newValue.trim
          case IntegerType =>
            if (StringUtils.isBlank(value)) {
              newValue = "0"
            }
            arr += newValue.toInt
          case LongType =>
            if (StringUtils.isBlank(value)) {
              newValue = "0"
            }
            arr += newValue.toLong
          case DateType => arr += newValue.trim
          case _ =>
            if (i.dataType.isInstanceOf[DecimalType]) {
              if (StringUtils.isBlank(value)) newValue = "0"
              try {
                val precision = i.dataType.asInstanceOf[DecimalType].precision
                val scale = i.dataType.asInstanceOf[DecimalType].scale
                val result = Decimal(BigDecimal(newValue.trim), precision, scale)
                arr += result
              } catch {
                case e: NumberFormatException =>
                  throw new AppException(i.name + " value=" + value, e)
              }
            } else {
              throw new AppException("no match type exception " + i.dataType)
            }
        }
        index += 1
      }
      Row.fromSeq(arr)
    }
    )
    rddRow
  }

  def formatJson(json: String): String = {
    var jsonStr = json
    jsonStr = jsonStr.replace(", ", ",")
    jsonStr = jsonStr.replace("{", "{\"")
    jsonStr = jsonStr.replace(":", "\":\"")
    jsonStr = jsonStr.replace("=", "\":\"")
    jsonStr = jsonStr.replace("\"[", "[")
    jsonStr = jsonStr.replace(",", "\",\"")
    jsonStr = jsonStr.replace("}", "\"}")
    jsonStr = jsonStr.replace("}\",\"{", "},{")
    jsonStr = jsonStr.replace("]\"}", "]}")
    jsonStr
  }

  def getTextConfig[T](json: String, clazz: Class[T]) = {
    val jsonStr = formatJson(json)
    JsonUtil.getBeanFromJson(jsonStr, clazz)
  }

}
