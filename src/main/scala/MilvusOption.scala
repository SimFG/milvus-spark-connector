package com.zilliz.spark.connector

import scala.collection.Map

import org.apache.spark.sql.util.CaseInsensitiveStringMap

import com.zilliz.spark.connector.binlog.Constants
import com.zilliz.spark.connector.MilvusConnectionException

case class MilvusOption(
    uri: String,
    token: String = "",
    serverPemPath: String = "",
    clientKeyPath: String = "",
    clientPemPath: String = "",
    caPemPath: String = "",
    databaseName: String = "",
    collectionName: String = "",
    partitionName: String = "",
    insertMaxBatchSize: Int = 0,
    retryCount: Int = 3,
    retryInterval: Int = 1000,
    collectionID: String = "",
    partitionID: String = "",
    segmentID: String = "",
    fieldID: String = "",
    fieldIDs: String = ""
)

object MilvusOption {
  // Constants for map keys
  val MilvusUri = "milvus.uri"
  val MilvusToken = "milvus.token"
  val MilvusServerPemPath = "milvus.server.pem"
  val MilvusClientKeyPath = "milvus.client.key"
  val MilvusClientPemPath = "milvus.client.pem"
  val MilvusCaPemPath = "milvus.ca.pem"
  val MilvusDatabaseName = "milvus.database.name"
  val MilvusCollectionName = "milvus.collection.name"
  val MilvusPartitionName = "milvus.partition.name"
  val MilvusCollectionID = "milvus.collection.id"
  val MilvusPartitionID = "milvus.partition.id"
  val MilvusSegmentID = "milvus.segment.id"
  val MilvusFieldID = "milvus.field.id"
  val MilvusInsertMaxBatchSize = "milvus.insertMaxBatchSize"
  val MilvusRetryCount = "milvus.retry.count"
  val MilvusRetryInterval = "milvus.retry.interval"

  // reader config
  val ReaderPath = Constants.LogReaderPathParamName
  val ReaderType = Constants.LogReaderTypeParamName
  val ReaderBeginTimestamp = Constants.LogReaderBeginTimestamp
  val ReaderEndTimestamp = Constants.LogReaderEndTimestamp
  val ReaderFieldIDs = Constants.LogReaderFieldIDs

  // s3 config
  val S3FileSystemTypeName = Constants.S3FileSystemTypeName
  val S3Endpoint = Constants.S3Endpoint
  val S3BucketName = Constants.S3BucketName
  val S3RootPath = Constants.S3RootPath
  val S3AccessKey = Constants.S3AccessKey
  val S3SecretKey = Constants.S3SecretKey
  val S3UseSSL = Constants.S3UseSSL
  val S3PathStyleAccess = Constants.S3PathStyleAccess

  // Create MilvusOption from a map
  def apply(options: CaseInsensitiveStringMap): MilvusOption = {
    val uri = options.getOrDefault(MilvusUri, "")
    val token = options.getOrDefault(MilvusToken, "")
    val serverPemPath = options.getOrDefault(MilvusServerPemPath, "")
    val clientKeyPath = options.getOrDefault(MilvusClientKeyPath, "")
    val clientPemPath = options.getOrDefault(MilvusClientPemPath, "")
    val caPemPath = options.getOrDefault(MilvusCaPemPath, "")

    val databaseName = options.getOrDefault(MilvusDatabaseName, "")
    val collectionName = options.getOrDefault(MilvusCollectionName, "")
    val partitionName = options.getOrDefault(MilvusPartitionName, "")
    val collectionID = options.getOrDefault(MilvusCollectionID, "")
    val partitionID = options.getOrDefault(MilvusPartitionID, "")
    val segmentID = options.getOrDefault(MilvusSegmentID, "")
    val fieldID = options.getOrDefault(MilvusFieldID, "")
    val insertMaxBatchSize =
      options.getOrDefault(MilvusInsertMaxBatchSize, "5000").toInt
    val retryCount = options.getOrDefault(MilvusRetryCount, "3").toInt
    val retryInterval =
      options.getOrDefault(MilvusRetryInterval, "1000").toInt
    val fieldIDs = options.getOrDefault(ReaderFieldIDs, "")

    MilvusOption(
      uri,
      token,
      serverPemPath,
      clientKeyPath,
      clientPemPath,
      caPemPath,
      databaseName,
      collectionName,
      partitionName,
      insertMaxBatchSize,
      retryCount,
      retryInterval,
      collectionID,
      partitionID,
      segmentID,
      fieldID,
      fieldIDs
    )
  }
}
