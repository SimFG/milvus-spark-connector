package com.zilliz.spark.connector.binlog

import java.io.{BufferedReader, InputStreamReader}
import java.io.FileNotFoundException
import java.net.URI
import java.util.{Collections, HashMap, Map => JMap}
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.fs.s3a.S3AFileSystem
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{
  Table,
  TableCapability,
  TableProvider
}
import org.apache.spark.sql.connector.catalog.SupportsRead
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.{
  DataType => SparkDataType,
  IntegerType,
  LongType,
  StringType,
  StructType
}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.SparkSession
import org.apache.spark.unsafe.types.UTF8String

import com.zilliz.spark.connector.{
  MilvusClient,
  MilvusCollectionInfo,
  MilvusOption
}
import io.milvus.grpc.schema.DataType

// 1. DataSourceRegister and TableProvider
class MilvusBinlogDataSource
    extends DataSourceRegister
    with TableProvider
    with Logging {
  override def shortName(): String = "milvusbinlog"

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    // Schema is fixed: a single column "values" of Array[String]
    // logInfo(
    //   s"inferSchema options, keys: ${options.keySet()}, values: ${options.values()}"
    // )
    val readerType = options.get(Constants.LogReaderTypeParamName)
    if (readerType == null) {
      throw new IllegalArgumentException(
        s"Option '${Constants.LogReaderTypeParamName}' is required for milvusbinlog format."
      )
    }
    readerType match {
      case Constants.LogReaderTypeInsert | Constants.LogReaderTypeDelete => {
        StructType(
          Seq(
            org.apache.spark.sql.types
              .StructField("data", StringType, true),
            org.apache.spark.sql.types
              .StructField("timestamp", LongType, true),
            org.apache.spark.sql.types
              .StructField("data_type", IntegerType, true)
          )
        )
      }
      case _ => {
        throw new IllegalArgumentException(
          s"Unsupported reader type: $readerType"
        )
      }
    }
  }

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: java.util.Map[String, String]
  ): Table = {
    // logInfo(s"getTable schema, properties: $properties")
    val path = properties.get(MilvusOption.ReaderPath)
    val collectionID = properties.get(MilvusOption.MilvusCollectionID)
    val collectionName = properties.get(MilvusOption.MilvusCollectionName)
    if (path == null && collectionID == null && collectionName == null) {
      throw new IllegalArgumentException(
        "Option 'path' or 'collection' is required for milvusbinlog format."
      )
    }
    // Pass all options to the table
    new MilvusBinlogTable(schema, properties)
  }

  // For Spark 3.0+ TableProvider also defines supportsExternalMetadata
  override def supportsExternalMetadata(): Boolean = true
}

// 2. Table
class MilvusBinlogTable(
    customSchema: StructType,
    properties: java.util.Map[String, String]
) extends Table
    with SupportsRead {
  val milvusOption = MilvusOption(new CaseInsensitiveStringMap(properties))
  var milvusCollection: MilvusCollectionInfo = _
  initInfo()
  var milvusPKType: String = ""

  def initInfo(): Unit = {
    if (milvusOption.uri.isEmpty || milvusOption.collectionName.isEmpty) {
      return
    }
    val client = MilvusClient(milvusOption)
    try {
      milvusCollection = client
        .getCollectionInfo(
          milvusOption.databaseName,
          milvusOption.collectionName
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

  override def name(): String = s"MilvusBinlogTable"

  override def schema(): StructType = {
    var sparkPKType: SparkDataType = StringType
    var optionPKType: String = milvusOption.collectionPKType.toLowerCase()
    if (optionPKType.isEmpty && milvusCollection != null) {
      val pkField = milvusCollection.schema.fields.find(_.isPrimaryKey)
      if (pkField.isDefined && pkField.get.dataType == DataType.Int64) {
        optionPKType = "int64"
      }
    }
    if (MilvusOption.isInt64PK(optionPKType)) {
      sparkPKType = LongType
    }
    milvusPKType = optionPKType

    StructType(
      Seq(
        org.apache.spark.sql.types
          .StructField(
            "data",
            sparkPKType,
            true
          ),
        org.apache.spark.sql.types
          .StructField("timestamp", LongType, true),
        org.apache.spark.sql.types
          .StructField("data_type", IntegerType, true)
      )
    )
  }

  override def capabilities(): java.util.Set[TableCapability] = {
    Collections.singleton(TableCapability.BATCH_READ)
  }

  override def newScanBuilder(
      options: CaseInsensitiveStringMap
  ): ScanBuilder = {
    // Merge table properties with scan options. Scan options take precedence.
    val mergedOptions: JMap[String, String] = new HashMap[String, String]()
    mergedOptions.putAll(properties)
    mergedOptions.putAll(options)

    if (
      milvusCollection != null && mergedOptions.get(
        MilvusOption.MilvusCollectionID
      ) == null
    ) {
      mergedOptions.put(
        MilvusOption.MilvusCollectionID,
        milvusCollection.collectionID.toString
      )
    }
    mergedOptions.put(
      MilvusOption.MilvusCollectionPKType,
      milvusPKType
    )

    val allOptions = new CaseInsensitiveStringMap(mergedOptions)
    new MilvusBinlogScanBuilder(schema(), allOptions)
  }
}

// 3. ScanBuilder
class MilvusBinlogScanBuilder(
    schema: StructType,
    options: CaseInsensitiveStringMap
) extends ScanBuilder {
  override def build(): Scan = new MilvusBinlogScan(schema, options)
}

// 4. Scan (Batch Scan)
class MilvusBinlogScan(schema: StructType, options: CaseInsensitiveStringMap)
    extends Scan
    with Batch
    with Logging {
  private val milvusOption = MilvusOption(options)
  private val readerOptions = MilvusBinlogReaderOption(options)
  private val pathOption: String = getPathOption()
  if (pathOption == null) {
    throw new IllegalArgumentException(
      "Option 'path' is required for mybinlog files."
    )
  }

  def getPathOption(): String = {
    if (!readerOptions.notEmpty(readerOptions.s3FileSystemType)) {
      return options.get(MilvusOption.ReaderPath)
    }
    val collection = milvusOption.collectionID
    val partition = milvusOption.partitionID
    val segment = milvusOption.segmentID
    val field = milvusOption.fieldID
    if (collection.isEmpty) {
      return options.get(MilvusOption.ReaderPath)
    }
    if (
      readerOptions.readerType == Constants.LogReaderTypeInsert && field.isEmpty
    ) {
      throw new IllegalArgumentException(
        "Option 'field' is required for insert log."
      )
    }
    val firstPath =
      if (readerOptions.readerType == Constants.LogReaderTypeInsert) {
        "insert_log"
      } else {
        "delta_log"
      }
    if (partition.isEmpty) {
      return s"${firstPath}/${collection}"
    }
    if (segment.isEmpty) {
      return s"${firstPath}/${collection}/${partition}"
    }
    if (readerOptions.readerType == Constants.LogReaderTypeInsert) {
      return s"${firstPath}/${collection}/${partition}/${segment}/${field}"
    }
    return s"${firstPath}/${collection}/${partition}/${segment}"
  }

  override def readSchema(): StructType = schema

  override def toBatch: Batch = this

  def getBinlogStatuses(
      fs: FileSystem,
      client: MilvusClient,
      segmentPath: Path
  ): Seq[FileStatus] = {
    val paths = segmentPath.toString().split("/")
    val segmentID = paths(paths.length - 1).toLong
    val collectionID = paths(paths.length - 3).toLong
    val result = client.getSegmentInfo(collectionID, segmentID)
    if (result.isFailure) {
      throw new IllegalArgumentException(
        s"Failed to get segment info: ${result.failed.get.getMessage}"
      )
    }
    val insertLogIDs = result.get.insertLogIDs
    val deleteLogIDs = result.get.deleteLogIDs
    try {

      val field = options.getOrDefault("field", "")
      if (readerOptions.readerType == Constants.LogReaderTypeInsert) {
        fs.listStatus(segmentPath)
          .filter(_.getPath.getName == field)
          .filter(_.isDirectory())
          .flatMap(status => {
            fs.listStatus(status.getPath())
          })
          .filter(status => {
            val paths = status.getPath().toString.split("/")
            val logID = paths(paths.length - 1)
            val fieldID = paths(paths.length - 2)
            insertLogIDs.contains(s"${fieldID}/${logID}")
          })
          .toSeq
      } else {
        fs.listStatus(segmentPath)
          .filter(status => {
            val logID = status.getPath().getName()
            deleteLogIDs.contains(logID)
          })
          .toSeq
      }
    } catch {
      case e: FileNotFoundException =>
        logWarning(s"Path $segmentPath not found")
        Seq[FileStatus]()
    }
  }

  def getPartitionOrSegmentStatuses(
      fs: FileSystem,
      dirPath: Path
  ): Seq[FileStatus] = {
    try {
      if (!fs.getFileStatus(dirPath).isDirectory) {
        throw new IllegalArgumentException(
          s"Path $dirPath is not a directory."
        )
      }
      fs.listStatus(dirPath)
        .filter(_.isDirectory())
        .filterNot(_.getPath.getName.startsWith("_"))
        .filterNot(_.getPath.getName.startsWith("."))
        .toSeq
    } catch {
      case e: FileNotFoundException =>
        logWarning(s"Path $dirPath not found")
        Seq[FileStatus]()
    }
  }

  def getValidSegments(client: MilvusClient): Seq[String] = {
    val result = client.getSegments(
      milvusOption.databaseName,
      milvusOption.collectionName
    )
    result
      .getOrElse(
        throw new Exception(
          s"Failed to get segment info: ${result.failed.get.getMessage}"
        )
      )
      .map(_.segmentID.toString)
  }

  override def planInputPartitions(): Array[InputPartition] = {
    var path = readerOptions.getFilePath(pathOption)
    var fileStatuses = Seq[FileStatus]()
    val fs = readerOptions.getFileSystem(path)

    val collection = milvusOption.collectionID
    val partition = milvusOption.partitionID
    val segment = milvusOption.segmentID
    val field = milvusOption.fieldID
    if (
      readerOptions.notEmpty(
        readerOptions.s3FileSystemType
      ) && !collection.isEmpty
    ) {
      val client = MilvusClient(milvusOption)

      var validSegments = Seq[String]()
      if (segment.isEmpty() && !milvusOption.collectionName.isEmpty()) {
        validSegments = getValidSegments(client)
      }

      if (!partition.isEmpty && !segment.isEmpty) { // full path
        fileStatuses = getBinlogStatuses(fs, client, path)
      } else if (!partition.isEmpty) { // leak segment path
        val segmentStatuses = getPartitionOrSegmentStatuses(fs, path)
        segmentStatuses
          .filter(status => validSegments.contains(status.getPath().getName))
          .foreach(status => {
            fileStatuses = fileStatuses ++ getBinlogStatuses(
              fs,
              client,
              status.getPath()
            )
          })
      } else { // leak partition path
        val partitionStatuses = getPartitionOrSegmentStatuses(fs, path)
        val segmentStatuses = partitionStatuses.flatMap(status => {
          getPartitionOrSegmentStatuses(fs, status.getPath())
        })
        segmentStatuses
          .filter(status => validSegments.contains(status.getPath().getName))
          .foreach(status => {
            fileStatuses = fileStatuses ++ getBinlogStatuses(
              fs,
              client,
              status.getPath()
            )
          })
      }

      client.close()
    } else {
      fileStatuses = if (fs.getFileStatus(path).isDirectory) {
        fs.listStatus(path)
          .filterNot(_.getPath.getName.startsWith("_"))
          .filterNot(_.getPath.getName.startsWith(".")) // Ignore hidden files
      } else {
        Array(fs.getFileStatus(path))
      }
    }
    logInfo(
      s"all file statuses: ${fileStatuses.map(_.getPath.toString).mkString(", ")}"
    )

    val result = fileStatuses
      .map(status =>
        MilvusBinlogInputPartition(status.getPath.toString): InputPartition
      )
      .toArray
    fs.close()
    result
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    new MilvusBinlogPartitionReaderFactory(options)
  }
}

case class MilvusBinlogInputPartition(filePath: String) extends InputPartition

case class MilvusBinlogReaderOption(
    readerType: String,
    s3FileSystemType: String,
    s3BucketName: String,
    s3RootPath: String,
    s3Endpoint: String,
    s3AccessKey: String,
    s3SecretKey: String,
    s3UseSSL: Boolean,
    s3PathStyleAccess: Boolean,
    beginTimestamp: Long, // inclusive // TODO delete it, use filter
    endTimestamp: Long, // exclusive
    milvusPKType: String
) extends Serializable
    with Logging {
  def notEmpty(str: String): Boolean = str != null && str.trim.nonEmpty

  def getConf(): Configuration = {
    val conf = new Configuration()
    if (notEmpty(s3FileSystemType)) {
      conf.set(
        "fs.s3a.endpoint",
        s3Endpoint
      )
      conf.set("fs.s3a.path.style.access", s3PathStyleAccess.toString)
      conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      conf.set(
        "fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider,com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
      )
      conf.set(
        "fs.s3a.access.key",
        s3AccessKey
      )
      conf.set(
        "fs.s3a.secret.key",
        s3SecretKey
      )
      conf.set(
        "fs.s3a.connection.ssl.enabled",
        s3UseSSL.toString
      )
    }
    conf
  }

  def getFileSystem(path: Path): FileSystem = {
    if (notEmpty(s3FileSystemType)) {
      val conf = getConf()
      val fileSystem = new S3AFileSystem()
      fileSystem.initialize(
        new URI(
          s"s3a://${s3BucketName}/"
        ),
        conf
      )
      fileSystem
    } else {
      val conf = getConf()
      path.getFileSystem(conf)
    }
  }

  def getFilePath(path: String): Path = {
    if (notEmpty(s3FileSystemType)) {
      if (path.startsWith("s3a://")) {
        new Path(path)
      } else {
        val finalPath = s"s3a://${s3BucketName}/${s3RootPath}/${path}"
        new Path(new URI(finalPath))
      }
    } else {
      new Path(path)
    }
  }
}

object MilvusBinlogReaderOption {
  def apply(options: CaseInsensitiveStringMap): MilvusBinlogReaderOption = {
    new MilvusBinlogReaderOption(
      options.get(Constants.LogReaderTypeParamName),
      options.get(Constants.S3FileSystemTypeName),
      options.getOrDefault(Constants.S3BucketName, "a-bucket"),
      options.getOrDefault(Constants.S3RootPath, "files"),
      options.getOrDefault(Constants.S3Endpoint, "localhost:9000"),
      options.getOrDefault(Constants.S3AccessKey, "minioadmin"),
      options.getOrDefault(Constants.S3SecretKey, "minioadmin"),
      options.getOrDefault(Constants.S3UseSSL, "false").toBoolean,
      options.getOrDefault(Constants.S3PathStyleAccess, "true").toBoolean,
      options.getOrDefault(Constants.LogReaderBeginTimestamp, "0").toLong,
      options.getOrDefault(Constants.LogReaderEndTimestamp, "0").toLong,
      options.getOrDefault(MilvusOption.MilvusCollectionPKType, "")
    )
  }
}

// 5. PartitionReaderFactory
class MilvusBinlogPartitionReaderFactory(options: CaseInsensitiveStringMap)
    extends PartitionReaderFactory {

  private val readerOptions = MilvusBinlogReaderOption(options)

  override def createReader(
      partition: InputPartition
  ): PartitionReader[InternalRow] = {
    val filePath = partition.asInstanceOf[MilvusBinlogInputPartition].filePath
    new MilvusBinlogPartitionReader(filePath, readerOptions)
  }
}

// 6. PartitionReader
class MilvusBinlogPartitionReader(
    filePath: String,
    options: MilvusBinlogReaderOption
) extends PartitionReader[InternalRow]
    with Logging {
  private val readerType: String = options.readerType

  private val path = options.getFilePath(filePath)
  private val fs: FileSystem = options.getFileSystem(path)
  private val inputStream = fs.open(path)

  private val objectMapper = LogReader.getObjectMapper()
  private val descriptorEvent = LogReader.readDescriptorEvent(inputStream)
  private val dataType = descriptorEvent.data.payloadDataType
  private var deleteEvent: DeleteEventData = null
  private var insertEvent: InsertEventData = null
  private var currentIndex: Int = 0

  override def next(): Boolean = {
    readerType match {
      case Constants.LogReaderTypeInsert => readInsertEvent()
      case Constants.LogReaderTypeDelete => readDeleteEvent()
      case _ =>
        throw new IllegalArgumentException(
          s"Unsupported reader type: $readerType"
        )
    }
  }

  private def readInsertEvent(): Boolean = {
    if (insertEvent != null && currentIndex == insertEvent.datas.length - 1) {
      insertEvent = null
      currentIndex = 0
    }
    if (insertEvent == null) {
      insertEvent =
        LogReader.readInsertEvent(inputStream, objectMapper, dataType)
    } else {
      currentIndex += 1
    }

    insertEvent != null
  }

  private def getInsertInternalRow(): InternalRow = {
    val data = insertEvent.datas(currentIndex)
    val timestamp = insertEvent.timestamp
    val dataType = insertEvent.dataType.value

    InternalRow(
      UTF8String.fromString(data),
      timestamp,
      dataType
    )
  }

  private def readDeleteEvent(): Boolean = {
    if (deleteEvent != null && currentIndex == deleteEvent.pks.length - 1) {
      deleteEvent = null
      currentIndex = 0
    }
    if (deleteEvent == null) {
      deleteEvent =
        LogReader.readDeleteEvent(inputStream, objectMapper, dataType)
    } else {
      currentIndex += 1
    }

    deleteEvent != null
  }

  private def getDeleteInternalRow(): InternalRow = {
    val pk = deleteEvent.pks(currentIndex)
    val timestamp = deleteEvent.timestamps(currentIndex)
    val pkType = deleteEvent.pkType.value

    InternalRow(
      if (MilvusOption.isInt64PK(options.milvusPKType)) {
        pk.toLong
      } else {
        UTF8String.fromString(pk)
      },
      timestamp,
      pkType
    )
  }

  override def get(): InternalRow = {
    try {
      readerType match {
        case Constants.LogReaderTypeInsert => getInsertInternalRow()
        case Constants.LogReaderTypeDelete => getDeleteInternalRow()
        case _ =>
          throw new IllegalArgumentException(
            s"Unsupported reader type: $readerType"
          )
      }
    } catch {
      case e: Exception =>
        logError(
          s"Error parsing line: $currentIndex in file $filePath. Error: ${e.getMessage}"
        )
        InternalRow.empty // Or re-throw exception based on desired error handling
    }
  }

  override def close(): Unit = {
    if (inputStream != null) {
      inputStream.close()
    }
    if (fs != null) {
      fs.close()
    }
  }
}
