package com.zilliz.spark.connector.sources

import java.{util => ju}
import scala.jdk.CollectionConverters._

import org.apache.spark.sql.connector.catalog.{SupportsWrite, TableCapability}
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.types.{StructField, StructType}

import com.zilliz.spark.connector.{DataTypeUtil, MilvusClient, MilvusOption}
import com.zilliz.spark.connector.writer.MilvusWriteBuilder

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
    StructType(
      milvusCollection.schema.fields.map(field =>
        StructField(
          field.name,
          DataTypeUtil.toDataType(field),
          field.nullable
        )
      )
    )
  )

  override def capabilities(): ju.Set[TableCapability] = {
    Set[TableCapability](
      TableCapability.BATCH_WRITE
        // TableCapability.BATCH_READ
    ).asJava
  }
}
