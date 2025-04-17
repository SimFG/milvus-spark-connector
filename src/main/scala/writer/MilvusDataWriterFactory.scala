package com.zilliz.spark.connector.writer

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, DataWriterFactory}
import org.apache.spark.sql.types.StructType

import com.zilliz.spark.connector.MilvusOption

case class MilvusDataWriterFactory(
    milvusOptions: MilvusOption,
    schema: StructType
) extends DataWriterFactory
    with Serializable {
  override def createWriter(
      partitionId: Int,
      taskId: Long
  ): DataWriter[InternalRow] = {
    MilvusDataWriter(partitionId, taskId, milvusOptions, schema)
  }
}
