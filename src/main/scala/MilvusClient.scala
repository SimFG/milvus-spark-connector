package com.zilliz.spark.connector

import java.net.URI
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

import com.google.protobuf.ByteString

import io.milvus.grpc.common.{
  ClientInfo,
  ConsistencyLevel,
  ErrorCode,
  KeyValuePair,
  Status
}
import io.milvus.grpc.common.{SegmentLevel, SegmentState}
import io.milvus.grpc.milvus.{
  ConnectRequest,
  CreateCollectionRequest,
  CreateDatabaseRequest,
  DeleteRequest,
  DescribeCollectionRequest,
  GetPersistentSegmentInfoRequest,
  InsertRequest,
  MilvusServiceGrpc,
  MutationResult
}
import io.milvus.grpc.schema.{
  CollectionSchema,
  DataType,
  FieldData,
  FieldSchema,
  FunctionSchema,
  ValueField
}

import io.grpc.{ManagedChannel, ManagedChannelBuilder}

/** A simplified client for interacting with Milvus
  */
class MilvusClient(params: MilvusConnectionParams) {
  private lazy val channel: ManagedChannel = {
    val uri = new URI(params.uri)
    val scheme = uri.getScheme
    val isHttps = scheme.equalsIgnoreCase("https")
    val host = uri.getHost
    var port = uri.getPort
    if (port == -1) {
      if (isHttps) {
        port = 443
      } else {
        port = 80
      }
    }
    // TODO fubang: add tls support
    var channelBuilder = ManagedChannelBuilder
      .forAddress(host, port)
      .usePlaintext()
    if (isHttps) {
      channelBuilder = channelBuilder.useTransportSecurity()
    }
    channelBuilder.build()
  }
  private lazy val stub: MilvusServiceGrpc.MilvusServiceBlockingStub = {
    val server = MilvusServiceGrpc.blockingStub(channel).withWaitForReady()
    server.connect(
      ConnectRequest(
        clientInfo = Some(
          ClientInfo(
            sdkType = "spark-connector",
            sdkVersion = "0.1.0",
            localTime = java.time.LocalDateTime.now().toString,
            host = java.net.InetAddress.getLocalHost.getHostName,
            user = "scala-sdk-user"
          )
        )
      )
    )
    server
  }

  def checkStatus(api: String, status: Status): Try[Status] = {
    if (status.code != 0 || status.errorCode != ErrorCode.Success) {
      Failure(new Exception(s"Failed to $api: ${status.reason}"))
    } else {
      Success(status)
    }
  }

  def createDatabase(
      dbName: String,
      properties: Map[String, String] = Map.empty
  ): Try[Status] = {
    try {
      val status = stub.createDatabase(
        CreateDatabaseRequest(
          dbName = dbName,
          properties = properties
            .map(kv => KeyValuePair(key = kv._1, value = kv._2))
            .toSeq
        )
      )
      checkStatus("createDatabase", status)
    } catch {
      case e: Exception =>
        Failure(
          new Exception(s"Failed to create database: ${e.getMessage}")
        )
    }

  }

  def createCollectionField(
      name: String,
      isPrimary: Boolean = false,
      description: String = "",
      dataType: DataType,
      typeParams: Map[String, String] = Map.empty,
      autoID: Boolean = false,
      elementType: DataType = DataType.None,
      defaultValue: Option[ValueField] = None,
      isDynamic: Boolean = false,
      isPartitionKey: Boolean = false,
      isClusteringKey: Boolean = false,
      nullable: Boolean = false,
      isFunctionOutput: Boolean = false
  ): FieldSchema = {
    FieldSchema(
      name = name,
      isPrimaryKey = isPrimary,
      description = description,
      dataType = dataType,
      typeParams =
        typeParams.map(kv => KeyValuePair(key = kv._1, value = kv._2)).toSeq,
      autoID = autoID,
      elementType = elementType,
      defaultValue = defaultValue,
      isDynamic = isDynamic,
      isPartitionKey = isPartitionKey,
      isClusteringKey = isClusteringKey,
      nullable = nullable,
      isFunctionOutput = isFunctionOutput
    )
  }

  def createCollectionSchema(
      dbName: String = "",
      name: String,
      description: String = "",
      fields: Seq[FieldSchema],
      enableDynamicSchema: Boolean = false,
      enableAutoID: Boolean = false,
      properties: Map[String, String] = Map.empty,
      functions: Seq[FunctionSchema] = Seq.empty
  ): CollectionSchema = {
    CollectionSchema(
      name = name,
      description = description,
      fields = fields,
      autoID = enableAutoID,
      enableDynamicField = enableDynamicSchema,
      properties = properties
        .map(kv => KeyValuePair(key = kv._1, value = kv._2))
        .toSeq,
      functions = functions,
      dbName = dbName
    )
  }

  def createCollection(
      dbName: String = "",
      collectionName: String,
      schema: CollectionSchema,
      shardsNum: Int = 1,
      consistencyLevel: ConsistencyLevel = ConsistencyLevel.Strong,
      numPartitions: Long = 0L,
      properties: Map[String, String] = Map.empty
  ): Try[Status] = {
    try {
      val status = stub.createCollection(
        CreateCollectionRequest(
          dbName = dbName,
          collectionName = collectionName,
          schema = schema.toByteString,
          shardsNum = shardsNum,
          consistencyLevel = consistencyLevel,
          numPartitions = numPartitions,
          properties = properties
            .map(kv => KeyValuePair(key = kv._1, value = kv._2))
            .toSeq
        )
      )
      checkStatus("createCollection", status)
    } catch {
      case e: Exception =>
        Failure(
          new Exception(s"Failed to create collection: ${e.getMessage}")
        )
    }
  }

  def packFieldData(): FieldData = {
    FieldData(
      `type` = DataType.Int64,
      fieldName = "pk",
      isDynamic = false,
      validData = Seq.empty
    )
  }

  def insert(
      dbName: String = "",
      collectionName: String,
      partitionName: Option[String] = None,
      fieldsData: Seq[FieldData] = Seq.empty,
      numRows: Int = 0,
      schemaTimestamp: Long = 0L
  ): Try[Status] = {
    try {
      val insertResult = stub.insert(
        InsertRequest(
          dbName = dbName,
          collectionName = collectionName,
          partitionName = partitionName.getOrElse(""),
          fieldsData = fieldsData,
          numRows = numRows,
          schemaTimestamp = schemaTimestamp
        )
      )
      checkStatus(
        "insert",
        insertResult.status.getOrElse(
          Status(
            errorCode = ErrorCode.UnexpectedError,
            reason = "Insert Status is empty"
          )
        )
      )
    } catch {
      case e: Exception =>
        Failure(
          new Exception(s"Failed to insert data: ${e.getMessage}")
        )
    }
  }

  def delete[T](
      dbName: String = "",
      collectionName: String,
      partitionName: Option[String] = None,
      pkName: Option[String] = None,
      pks: Seq[T] = Seq.empty
  )(implicit processor: PKProcessor[T]): Try[Status] = {
    try {
      val expr: String = pkName match {
        case Some(name) => {
          s"$name in [${processor.process(pks)}]"
        }
        case None => {
          val remotePKName = getPKName(dbName, collectionName)
          val name = remotePKName
            .getOrElse(
              throw new Exception(
                s"Failed to get PK name for collection $collectionName"
              )
            )
          s"$name in [${processor.process(pks)}]"
        }
      }
      val deleteResult = stub.delete(
        DeleteRequest(
          dbName = dbName,
          collectionName = collectionName,
          partitionName = partitionName.getOrElse(""),
          expr = expr
        )
      )
      Success(
        Status(
          errorCode = ErrorCode.Success,
          reason =
            s"Mock success for deleting from collection: $collectionName with expr: $expr${partitionName
                .map(p => s" partition: $p")
                .getOrElse("")}"
        )
      )
    } catch {
      case e: Exception =>
        Failure(
          new Exception(s"Failed to delete data: ${e.getMessage}")
        )
    }
  }

  def getPKName(dbName: String, collectionName: String): Try[String] = {
    try {
      val collectionInfo = stub.describeCollection(
        DescribeCollectionRequest(
          dbName = dbName,
          collectionName = collectionName
        )
      )
      Success(
        collectionInfo.schema
          .getOrElse(
            throw new Exception(
              s"Collection schema for $collectionName not found"
            )
          )
          .fields
          .find(_.isPrimaryKey)
          .map(_.name)
          .getOrElse(
            throw new Exception(
              s"Primary key not found for collection $collectionName"
            )
          )
      )
    } catch {
      case e: Exception =>
        Failure(new Exception(s"Failed to get PK name: ${e.getMessage}"))
    }
  }

  def getCollectionSchema(
      dbName: String,
      collectionName: String
  ): Try[CollectionSchema] = {
    try {
      val collectionInfo = stub.describeCollection(
        DescribeCollectionRequest(
          dbName = dbName,
          collectionName = collectionName
        )
      )
      Success(
        collectionInfo.schema.getOrElse(
          throw new Exception(
            s"Collection schema for $collectionName not found"
          )
        )
      )
    } catch {
      case e: Exception =>
        Failure(
          new Exception(s"Failed to get collection schema: ${e.getMessage}")
        )
    }
  }

  def getCollectionInfo(
      dbName: String,
      collectionName: String
  ): Try[MilvusCollectionInfo] = {
    try {
      val collectionInfo = stub.describeCollection(
        DescribeCollectionRequest(
          dbName = dbName,
          collectionName = collectionName
        )
      )
      Success(
        MilvusCollectionInfo(
          dbName = dbName,
          collectionName = collectionName,
          collectionID = collectionInfo.collectionID,
          schema = collectionInfo.schema.getOrElse(
            throw new Exception(
              s"Collection schema for $collectionName not found"
            )
          )
        )
      )
    } catch {
      case e: Exception =>
        Failure(
          new Exception(s"Failed to get collection info: ${e.getMessage}")
        )
    }
  }

  def getSegments(
      dbName: String,
      collectionName: String
  ): Try[Seq[MilvusSegmentInfo]] = {
    try {
      val segments = stub.getPersistentSegmentInfo(
        GetPersistentSegmentInfoRequest(
          dbName = dbName,
          collectionName = collectionName
        )
      )
      Success(
        segments.infos.map(info =>
          MilvusSegmentInfo(
            segmentID = info.segmentID,
            collectionID = info.collectionID,
            partitionID = info.partitionID,
            numRows = info.numRows,
            state = info.state,
            level = info.level
          )
        )
      )
    } catch {
      case e: Exception =>
        Failure(
          new Exception(s"Failed to get segments: ${e.getMessage}")
        )
    }
  }

  def close(): Unit = {
    channel.shutdownNow()
  }
}

object MilvusClient {
  def apply(params: MilvusConnectionParams): MilvusClient = {
    new MilvusClient(params)
  }
  def apply(options: MilvusOption): MilvusClient = {
    new MilvusClient(
      MilvusConnectionParams(options.uri, options.token, options.databaseName)
    )
  }
}

case class MilvusConnectionParams(
    uri: String,
    token: String = "",
    databaseName: String = ""
)

case class MilvusCollectionInfo(
    dbName: String,
    collectionName: String,
    collectionID: Long,
    schema: CollectionSchema
)

case class MilvusSegmentInfo(
    segmentID: Long,
    collectionID: Long,
    partitionID: Long,
    numRows: Long,
    state: SegmentState,
    level: SegmentLevel
)

trait PKProcessor[T] {
  def process(seq: Seq[T]): String
}

object PKProcessor {
  implicit object IntProcessor extends PKProcessor[Int] {
    def process(seq: Seq[Int]): String = seq.mkString(", ")
  }

  implicit object StringProcessor extends PKProcessor[String] {
    def process(seq: Seq[String]): String = seq.map(s => s"'$s'").mkString(", ")
  }
}
