package com.zilliz.spark.connector

import scala.collection.Map

import org.apache.spark.sql.util.CaseInsensitiveStringMap

import com.zilliz.spark.connector.MilvusConnectionException

case class MilvusOption(
    uri: String,
    token: String = "",
    databaseName: String = "",
    collectionName: String = "",
    partitionName: String = "",
    insertMaxBatchSize: Int = 0,
    retryCount: Int = 3,
    retryInterval: Int = 1000,
    collectionID: String = "",
    partitionID: String = "",
    segmentID: String = "",
    fieldID: String = ""
)

object MilvusOption {
  // Constants for map keys
  val URI_KEY = "milvus.uri"
  val TOKEN_KEY = "milvus.token"
  val MILVUS_DATABASE_NAME = "milvus.database.name"
  val MILVUS_COLLECTION_NAME = "milvus.collection.name"
  val MILVUS_PARTITION_NAME = "milvus.partition.name"
  val MILVUS_COLLECTION_ID = "milvus.collection.id"
  val MILVUS_PARTITION_ID = "milvus.partition.id"
  val MILVUS_SEGMENT_ID = "milvus.segment.id"
  val MILVUS_FIELD_ID = "milvus.field.id"
  val MILVUS_INSERT_MAX_BATCHSIZE = "milvus.insertMaxBatchSize"
  val MILVUS_RETRY_COUNT = "milvus.retry.count"
  val MILVUS_RETRY_INTERVAL = "milvus.retry.interval"

  // Create MilvusOption from a map
  def apply(options: CaseInsensitiveStringMap): MilvusOption = {
    val uri = options.getOrDefault(URI_KEY, "")
    val token = options.getOrDefault(TOKEN_KEY, "")
    val databaseName = options.getOrDefault(MILVUS_DATABASE_NAME, "")
    val collectionName = options.getOrDefault(MILVUS_COLLECTION_NAME, "")
    val partitionName = options.getOrDefault(MILVUS_PARTITION_NAME, "")
    val collectionID = options.getOrDefault(MILVUS_COLLECTION_ID, "")
    val partitionID = options.getOrDefault(MILVUS_PARTITION_ID, "")
    val segmentID = options.getOrDefault(MILVUS_SEGMENT_ID, "")
    val fieldID = options.getOrDefault(MILVUS_FIELD_ID, "")
    val insertMaxBatchSize =
      options.getOrDefault(MILVUS_INSERT_MAX_BATCHSIZE, "5000").toInt
    val retryCount = options.getOrDefault(MILVUS_RETRY_COUNT, "3").toInt
    val retryInterval =
      options.getOrDefault(MILVUS_RETRY_INTERVAL, "1000").toInt

    // Validate uri and databaseName
    if (uri.isEmpty) {
      throw new MilvusConnectionException("Milvus URI cannot be empty")
    }

    MilvusOption(
      uri,
      token,
      databaseName,
      collectionName,
      partitionName,
      insertMaxBatchSize,
      retryCount,
      retryInterval,
      collectionID,
      partitionID,
      segmentID,
      fieldID
    )
  }
}
