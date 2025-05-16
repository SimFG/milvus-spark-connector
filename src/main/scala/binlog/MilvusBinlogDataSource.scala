package com.zilliz.spark.connector.binlog

import java.io.{BufferedReader, InputStreamReader}
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
  DataType,
  IntegerType,
  LongType,
  StringType,
  StructType
}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.SparkSession
import org.apache.spark.unsafe.types.UTF8String

// 1. DataSourceRegister and TableProvider
class MilvusBinlogDataSource
    extends DataSourceRegister
    with TableProvider
    with Logging {
  override def shortName(): String = "milvusbinlog"

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    // Schema is fixed: a single column "values" of Array[String]
    logInfo(
      s"inferSchema options, keys: ${options.keySet()}, values: ${options.values()}"
    )
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
    logInfo(s"getTable schema, properties: $properties")
    val path = properties.get("path")
    if (path == null) {
      throw new IllegalArgumentException(
        "Option 'path' is required for mybinlog format."
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

  // TODO: add a name for the table
  override def name(): String = s"MilvusBinlogTable"

  override def schema(): StructType = {
    Option(customSchema)
      .filter(_.fields.nonEmpty)
      .getOrElse(
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
    with Batch {
  private val pathOption: String = options.get("path")
  if (pathOption == null) {
    throw new IllegalArgumentException(
      "Option 'path' is required for mybinlog files."
    )
  }
  private val readerOptions = MilvusBinlogReaderOptions(options)

  override def readSchema(): StructType = schema

  override def toBatch: Batch = this

  override def planInputPartitions(): Array[InputPartition] = {
    val path = readerOptions.getFilePath(pathOption)
    val fs = readerOptions.getFileSystem(path)

    val fileStatuses = if (fs.getFileStatus(path).isDirectory) {
      fs.listStatus(path)
        .filterNot(_.getPath.getName.startsWith("_"))
        .filterNot(_.getPath.getName.startsWith(".")) // Ignore hidden files
    } else {
      Array(fs.getFileStatus(path))
    }

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

case class MilvusBinlogReaderOptions(
    readerType: String,
    s3FileSystemType: String,
    s3BucketName: String,
    s3RootPath: String,
    s3Endpoint: String,
    s3AccessKey: String,
    s3SecretKey: String,
    s3UseSSL: Boolean
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
      conf.set("fs.s3a.path.style.access", "true")
      conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
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

object MilvusBinlogReaderOptions {
  def apply(options: CaseInsensitiveStringMap): MilvusBinlogReaderOptions = {
    new MilvusBinlogReaderOptions(
      options.get(Constants.LogReaderTypeParamName),
      options.get(Constants.S3FileSystemTypeName),
      options.getOrDefault(Constants.S3BucketName, "a-bucket"),
      options.getOrDefault(Constants.S3RootPath, "files"),
      options.getOrDefault(Constants.S3Endpoint, "localhost:9000"),
      options.getOrDefault(Constants.S3AccessKey, "minioadmin"),
      options.getOrDefault(Constants.S3SecretKey, "minioadmin"),
      options.getOrDefault(Constants.S3UseSSL, "false").toBoolean
    )
  }
}

// 5. PartitionReaderFactory
class MilvusBinlogPartitionReaderFactory(options: CaseInsensitiveStringMap)
    extends PartitionReaderFactory {

  private val readerOptions = MilvusBinlogReaderOptions(options)

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
    options: MilvusBinlogReaderOptions
) extends PartitionReader[InternalRow]
    with Logging {
  private val readerType: String = options.readerType

  // private val conf =
  //   new Configuration() // Use Spark's Hadoop conf for HDFS etc.
  // private val conf = options.getConf()
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
      UTF8String.fromString(pk),
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
