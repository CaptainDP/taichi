package com.captain.bigdata.taichi.process.udf

import java.util

import org.apache.hadoop.hive.ql.exec.UDFArgumentException
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, ObjectInspectorFactory, PrimitiveObjectInspector, StructObjectInspector}

import scala.collection.mutable.ArrayBuffer

/**
  * UDTFSample
  *
  * @author <a href=mailto:captain_cc_2008@163.com>CaptainDP</a>
  * @date 2017/12/4 10:44
  * @func
  */
class UDTFSample extends GenericUDTF {

  val COLUMN1 = "column1"
  val COLUMN2 = "column2"
  val COLUMN3 = "column3"

  var stringOI: PrimitiveObjectInspector = null
  var stringOI2: PrimitiveObjectInspector = null

  override def initialize(args: Array[ObjectInspector]): StructObjectInspector = {
    if (args.length != 2)
      throw new UDFArgumentException("NameParserGenericUDTF() takes exactly tow argument")
    if ((args(0).getCategory ne ObjectInspector.Category.PRIMITIVE) && (args(0).asInstanceOf[PrimitiveObjectInspector].getPrimitiveCategory ne PrimitiveObjectInspector.PrimitiveCategory.STRING))
      throw new UDFArgumentException("NameParserGenericUDTF() takes a string as a first parameter")
    if ((args(1).getCategory ne ObjectInspector.Category.PRIMITIVE) && (args(1).asInstanceOf[PrimitiveObjectInspector].getPrimitiveCategory ne PrimitiveObjectInspector.PrimitiveCategory.STRING))
      throw new UDFArgumentException("NameParserGenericUDTF() takes a string as a second parameter")

    // input inspectors an object with 2 fields!
    stringOI = args(0).asInstanceOf[PrimitiveObjectInspector]
    stringOI2 = args(1).asInstanceOf[PrimitiveObjectInspector]

    // output inspectors -- an object with 3 fields!
    val fieldNames = new util.ArrayList[String]()
    fieldNames.add(COLUMN1)
    fieldNames.add(COLUMN2)
    fieldNames.add(COLUMN3)

    val fieldOIs = new util.ArrayList[ObjectInspector]()
    fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector)
    fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector)
    fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector)

    ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs)

  }


  override def process(record: Array[AnyRef]): Unit = {

    val input1 = stringOI.getPrimitiveJavaObject(record(0)).toString
    var input2 = stringOI2.getPrimitiveJavaObject(record(1))

    val results = processInputRecord(input1, input2.toString)
    val it = results.iterator
    while ( {
      it.hasNext
    }) {
      val r = it.next
      forward(r)
    }
  }

  override def close(): Unit = {
  }

  def processInputRecord(input1: String, input2: String): util.ArrayList[Array[AnyRef]] = {

    val result = new util.ArrayList[Array[AnyRef]]

    if (input1 == null || input1.isEmpty) return result
    if (input2 == null || input2.isEmpty) return result

    var list = ArrayBuffer[String]()
    list += "column1_" + input1
    list += "column2_" + input2
    list += "column3"

    result.add(list.toArray)

    result
  }

}

