package com.zilliz.spark.connector.writer

import org.apache.spark.sql.connector.write.{
  BatchWrite,
  DataWriterFactory,
  PhysicalWriteInfo,
  WriterCommitMessage
}
import org.apache.spark.sql.types.StructType

import com.zilliz.spark.connector.MilvusOption

case class MilvusBatchWriter(milvusOptions: MilvusOption, schema: StructType)
    extends BatchWrite {
  override def createBatchWriterFactory(
      info: PhysicalWriteInfo
  ): DataWriterFactory = {
    MilvusDataWriterFactory(milvusOptions, schema)
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {}

  override def abort(messages: Array[WriterCommitMessage]): Unit = {}
}
