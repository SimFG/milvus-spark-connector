package com.zilliz.spark.connector.sources

import java.util

import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import com.zilliz.spark.connector.{DataTypeUtil, MilvusClient, MilvusOption}
import com.zilliz.spark.connector.sources.MilvusTable

case class Milvus() extends TableProvider with DataSourceRegister {
  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]
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
