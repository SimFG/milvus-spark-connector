package com.zilliz.spark.connector

import com.google.protobuf.ByteString

import com.zilliz.spark.connector.{MilvusClient, MilvusConnectionParams}
import com.zilliz.spark.connector.FloatConverter

object MilvusClientExample extends App {
  val floatValue = 1.0f

  try {
    println("Testing float16 conversion:")
    val float16Bytes = FloatConverter.toFloat16Bytes(floatValue)
    println(
      s"float16 bytes: ${float16Bytes.map("%02X".format(_)).mkString(" ")}"
    )

    println("Testing bfloat16 conversion:")
    val bfloat16Bytes = FloatConverter.toBFloat16Bytes(floatValue)
    println(
      s"bfloat16 bytes: ${bfloat16Bytes.map("%02X".format(_)).mkString(" ")}"
    )
  } catch {
    case e: IllegalArgumentException => println(e.getMessage)
  }
}
