package com.zilliz.spark.connector

import scala.collection.Map

import com.zilliz.spark.connector.MilvusConnectionException

case class MilvusOption(
    uri: String,
    token: String = "",
    databaseName: String = "",
    collectionName: String = "",
    partitionName: String = "",
    insertMaxBatchSize: Int = 0,
    retryCount: Int = 3,
    retryInterval: Int = 1000
)

object MilvusOption {
  // Constants for map keys
  val URI_KEY = "milvus.uri"
  val TOKEN_KEY = "milvus.token"
  val MILVUS_DATABASE_NAME = "milvus.database.name"
  val MILVUS_COLLECTION_NAME = "milvus.collection.name"
  val MILVUS_PARTITION_NAME = "milvus.partition.name"
  val MILVUS_INSERT_MAX_BATCHSIZE = "milvus.insertMaxBatchSize"
  val MILVUS_RETRY_COUNT = "milvus.retry.count"
  val MILVUS_RETRY_INTERVAL = "milvus.retry.interval"

  // Create MilvusOption from a map
  def apply(options: Map[String, String]): MilvusOption = {
    val uri = options.getOrElse(URI_KEY, "")
    val token = options.getOrElse(TOKEN_KEY, "")
    val databaseName = options.getOrElse(MILVUS_DATABASE_NAME, "")
    val collectionName = options.getOrElse(MILVUS_COLLECTION_NAME, "")
    val partitionName = options.getOrElse(MILVUS_PARTITION_NAME, "")
    val insertMaxBatchSize = options
      .get(MILVUS_INSERT_MAX_BATCHSIZE)
      .map(_.toInt)
      .getOrElse(5000)
    val retryCount = options
      .get(MILVUS_RETRY_COUNT)
      .map(_.toInt)
      .getOrElse(3)
    val retryInterval = options
      .get(MILVUS_RETRY_INTERVAL)
      .map(_.toInt)
      .getOrElse(1000)

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
      insertMaxBatchSize
    )
  }
}
