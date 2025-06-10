package com.zilliz.spark.connector

import org.apache.spark.sql.{DataFrame, SparkSession}

object MilvusDataReader {
  def read(
      session: SparkSession,
      config: MilvusDataReaderConfig
  ): DataFrame = {
    // TODO: Implement this
    null
  }
}

case class MilvusDataReaderConfig(
    uri: String,
    token: String,
    collectionName: String,
    options: Map[String, String] = Map.empty
)
