package com.zilliz.spark.connector.sources

import java.{util => ju}
import scala.jdk.CollectionConverters._

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{
  SupportsWrite,
  Table,
  TableCapability,
  TableProvider
}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.write.{
  BatchWrite,
  DataWriterFactory,
  PhysicalWriteInfo,
  WriterCommitMessage
}
import org.apache.spark.sql.connector.write.{
  BatchWrite,
  LogicalWriteInfo,
  Write,
  WriteBuilder
}
import org.apache.spark.sql.connector.write.{DataWriter, DataWriterFactory}
import org.apache.spark.sql.connector.write.WriterCommitMessage
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import com.zilliz.spark.connector.{DataTypeUtil, MilvusClient, MilvusOption}

// 1. DataSourceRegister and TableProvider
case class MilvusDataSource() extends TableProvider with DataSourceRegister {
  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: ju.Map[String, String]
  ): Table = {
    val options = Map.newBuilder[String, String]
    properties.forEach((key, value) => options.addOne(key, value))
    MilvusTable(
      MilvusOption(options.result()),
      Some(schema)
    )
  }

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    val optionsMap = Map.newBuilder[String, String]
    options.forEach((key, value) => optionsMap.addOne(key, value))
    val milvusOption = MilvusOption(optionsMap.result())
    val client = MilvusClient(milvusOption)
    try {
      val result = client.getCollectionSchema(
        milvusOption.databaseName,
        milvusOption.collectionName
      )
      val schema = result.getOrElse(
        throw new Exception(
          s"Failed to get collection schema: ${result.failed.get.getMessage}"
        )
      )
      StructType(
        schema.fields.map(field =>
          StructField(
            field.name,
            DataTypeUtil.toDataType(field),
            field.nullable
          )
        )
      )
    } finally {
      client.close()
    }
  }
  override def supportsExternalMetadata = true

  override def shortName() = "milvus"
}

// 2. Table
case class MilvusTable(
    milvusOption: MilvusOption,
    sparkSchema: Option[StructType]
) extends SupportsWrite {
  lazy val milvusCollection = {
    val client = MilvusClient(milvusOption)
    try {
      client
        .getCollectionInfo(
          dbName = milvusOption.databaseName,
          collectionName = milvusOption.collectionName
        )
        .getOrElse(
          throw new Exception(
            s"Collection ${milvusOption.collectionName} not found"
          )
        )
    } finally {
      client.close()
    }
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder =
    MilvusWriteBuilder(milvusOption, info)

  override def name(): String = milvusOption.collectionName

  override def schema(): StructType = sparkSchema.getOrElse(
    // StructType(
    //   milvusCollection.schema.fields.map(field =>
    //     StructField(
    //       field.name,
    //       DataTypeUtil.toDataType(field),
    //       field.nullable
    //     )
    //   )
    // )
    StructType(
      Array(StructField("id", LongType, false))
    ) // TODO: it only be used for read data
  )

  override def capabilities(): ju.Set[TableCapability] = {
    Set[TableCapability](
      TableCapability.BATCH_WRITE
        // TableCapability.BATCH_READ
    ).asJava
  }
}

// 3. WriteBuilder
case class MilvusWriteBuilder(
    milvusOptions: MilvusOption,
    info: LogicalWriteInfo
) extends WriteBuilder
    with Serializable {
  override def build: Write = MilvusWrite(milvusOptions, info.schema())
}

// 4. Write
case class MilvusWrite(milvusOptions: MilvusOption, schema: StructType)
    extends Write
    with Serializable {
  override def toBatch: BatchWrite = MilvusBatchWriter(milvusOptions, schema)
}

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

case class MilvusCommitMessage(rowCount: Int) extends WriterCommitMessage
