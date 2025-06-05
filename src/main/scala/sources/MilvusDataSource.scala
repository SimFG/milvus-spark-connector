package com.zilliz.spark.connector.sources

import java.{util => ju}
import java.io.FileNotFoundException
import java.util.{Collections, HashMap, Map => JMap}
import scala.collection.mutable
import scala.jdk.CollectionConverters._

import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{
  SupportsWrite,
  Table,
  TableCapability,
  TableProvider
}
import org.apache.spark.sql.connector.catalog.SupportsRead
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.{
  Batch,
  InputPartition,
  PartitionReader,
  PartitionReaderFactory,
  Scan,
  ScanBuilder
}
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
import org.apache.spark.sql.connector.write.{
  DataWriter,
  DataWriterFactory,
  WriterCommitMessage
}
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.{
  LongType,
  StringType,
  StructField,
  StructType
}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import com.zilliz.spark.connector.{DataTypeUtil, MilvusClient, MilvusOption}
import com.zilliz.spark.connector.binlog.MilvusBinlogReaderOption
import com.zilliz.spark.connector.MilvusCollectionInfo
import com.zilliz.spark.connector.MilvusSegmentInfo

// 1. DataSourceRegister and TableProvider
case class MilvusDataSource() extends TableProvider with DataSourceRegister {
  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: ju.Map[String, String]
  ): Table = {
    val options = new CaseInsensitiveStringMap(properties)
    val milvusOption = MilvusOption(options)
    if (milvusOption.uri.isEmpty) {
      throw new IllegalArgumentException(
        s"Option '${MilvusOption.MilvusUri}' is required for reading milvus data."
      )
    }
    MilvusTable(
      milvusOption,
      Some(schema)
    )
  }

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    val milvusOption = MilvusOption(options)
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
) extends Table
    with SupportsWrite
    with SupportsRead
    with Logging {
  var milvusCollection: MilvusCollectionInfo = _
  initInfo()
  var fieldIDs =
    if (milvusOption.fieldIDs.nonEmpty) {
      milvusOption.fieldIDs.split(",").toSeq
    } else {
      Seq[String]()
    }

  def initInfo(): Unit = {
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

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder =
    MilvusWriteBuilder(milvusOption, info)

  override def newScanBuilder(
      options: CaseInsensitiveStringMap
  ): ScanBuilder = {
    // Merge table properties with scan options. Scan options take precedence.
    val mergedOptions: JMap[String, String] = new HashMap[String, String]()
    mergedOptions.putAll(properties)
    mergedOptions.putAll(options)
    if (mergedOptions.get(MilvusOption.MilvusCollectionID) == null) {
      mergedOptions.put(
        MilvusOption.MilvusCollectionID,
        milvusCollection.collectionID.toString
      )
    }

    val allOptions = new CaseInsensitiveStringMap(mergedOptions)
    new MilvusScanBuilder(schema(), allOptions)
  }

  override def name(): String = milvusOption.collectionName

  override def schema(): StructType = {
    var fields = Seq[StructField]()
    var fieldName2ID = mutable.Map[String, Long]()
    milvusCollection.schema.fields.zipWithIndex.foreach { case (field, index) =>
      fieldName2ID(field.name) = if (field.fieldID == 0) {
        index + 100
      } else {
        field.fieldID
      }
    }
    fields = fields :+ StructField("rowID", LongType, false)
    fields = fields :+ StructField("timestamp", LongType, false)
    fields = fields ++ milvusCollection.schema.fields
      .filter(field =>
        fieldIDs.isEmpty || fieldIDs.contains(fieldName2ID(field.name).toString)
      )
      .map(field =>
        StructField(
          field.name,
          DataTypeUtil.toDataType(field),
          field.nullable
        )
      )
    val maxFieldID = fieldName2ID.values.max
    if (
      milvusCollection.schema.enableDynamicField &&
      (fieldIDs.isEmpty || fieldIDs.contains((maxFieldID + 1).toString))
    ) {
      fields = fields :+ StructField("$meta", StringType, true)
    }
    StructType(fields)
  }

  override def capabilities(): ju.Set[TableCapability] = {
    Set[TableCapability](
      TableCapability.BATCH_WRITE,
      TableCapability.BATCH_READ
    ).asJava
  }
}

// 3. WriteBuilder and ScanBuilder
case class MilvusWriteBuilder(
    milvusOptions: MilvusOption,
    info: LogicalWriteInfo
) extends WriteBuilder
    with Serializable {
  override def build: Write = MilvusWrite(milvusOptions, info.schema())
}

class MilvusScanBuilder(
    schema: StructType,
    options: CaseInsensitiveStringMap
) extends ScanBuilder {
  override def build(): Scan = new MilvusScan(schema, options)
}

class MilvusScan(schema: StructType, options: CaseInsensitiveStringMap)
    extends Scan
    with Batch
    with Logging {
  private val milvusOption = MilvusOption(options)
  private val readerOption = MilvusBinlogReaderOption(options)
  private val pathOption: String = getPathOption()
  if (pathOption == null) {
    throw new IllegalArgumentException(
      "Option 'path' is required for mybinlog files."
    )
  }
  private val fieldIDs =
    if (options.get(MilvusOption.ReaderFieldIDs) != null) {
      val optionIDs = options.get(MilvusOption.ReaderFieldIDs).split(",").toSeq
      optionIDs ++ Seq[String]("0", "1")
    } else {
      Seq[String]()
    }

  def getPathOption(): String = {
    if (options.get(MilvusOption.ReaderPath) != null) {
      return options.get(MilvusOption.ReaderPath)
    }
    val collection = milvusOption.collectionID
    val partition = milvusOption.partitionID
    val segment = milvusOption.segmentID
    val firstPath = "insert_log"
    if (collection.isEmpty) {
      throw new IllegalArgumentException(
        "Option 'collection' is required for reading milvus data."
      )
    }
    if (partition.isEmpty) {
      return s"${firstPath}/${collection}"
    }
    if (segment.isEmpty) {
      return s"${firstPath}/${collection}/${partition}"
    }
    return s"${firstPath}/${collection}/${partition}/${segment}"
  }

  override def readSchema(): StructType = {
    schema
  }

  def getCollectionOrPartitionStatuses(
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

  def getSegmentFieldMap(
      fs: FileSystem,
      rootPath: Path
  ): Seq[Map[String, String]] = {
    val fileStatuses = if (fs.getFileStatus(rootPath).isDirectory) {
      val fieldDirStatuses = fs
        .listStatus(rootPath)
        .filterNot(_.getPath.getName.startsWith("_"))
        .filterNot(_.getPath.getName.startsWith(".")) // Ignore hidden files
      fieldDirStatuses
        .flatMap(fieldDirStatus => {
          val fieldPath = fieldDirStatus.getPath()
          if (fs.getFileStatus(fieldPath).isDirectory) {
            val deepFileStatuses = fs
              .listStatus(fieldPath)
              .filterNot(_.getPath.getName.startsWith("_"))
              .filterNot(
                _.getPath.getName.startsWith(".")
              ) // Ignore hidden files
            deepFileStatuses
          } else {
            throw new IllegalArgumentException(
              s"fieldPath is not a directory: $fieldPath"
            )
          }
        })
    } else {
      // Array(fs.getFileStatus(rootPath))
      throw new IllegalArgumentException(
        s"rootPath is not a directory: $rootPath"
      )
    }

    var filePathMap = mutable.Map[String, Seq[String]]()
    fileStatuses.foreach(status => {
      val filePath = status.getPath.toString
      val paths = filePath.split("/")
      val fileName = paths(paths.length - 1)
      val filedID = paths(paths.length - 2)
      if (filePathMap.contains(filedID)) {
        filePathMap(filedID) = filePathMap(filedID) :+ fileName
      } else {
        filePathMap(filedID) = Seq(fileName)
      }
    })

    if (fieldIDs.nonEmpty) {
      filePathMap = filePathMap.filter(entry => fieldIDs.contains(entry._1))
    }

    // Sort the file names in ascending order for each field ID
    filePathMap.foreach { case (fieldId, fileNames) =>
      filePathMap(fieldId) = fileNames.sorted
    }

    val fieldMaps = filePathMap.head._2.indices.map { i =>
      filePathMap.map { case (fieldId, fileNames) =>
        val fullPath = s"${rootPath.toString()}/${fieldId}/${fileNames(i)}"
        // logInfo(s"field file fullPath: $fullPath")
        fieldId -> fullPath
      }.toMap
    }.toList
    return fieldMaps
  }

  def getValidSegments(): Seq[String] = {
    val client = MilvusClient(milvusOption)
    try {
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
    } finally {
      client.close()
    }
  }

  override def toBatch: Batch = this

  override def planInputPartitions(): Array[InputPartition] = {
    val rootPath = readerOption.getFilePath(pathOption)
    val fs = readerOption.getFileSystem(rootPath)

    // segment path
    val rawPath = options.getOrDefault(MilvusOption.ReaderPath, "")
    val collection = milvusOption.collectionID
    val partition = milvusOption.partitionID
    val segment = milvusOption.segmentID

    var validSegments = Seq[String]()
    if (segment.isEmpty()) {
      validSegments = getValidSegments()
    }

    var fieldMaps = Seq[Map[String, String]]()
    if (rawPath.isEmpty) {
      if (!partition.isEmpty() && !segment.isEmpty()) {
        fieldMaps ++= getSegmentFieldMap(fs, rootPath)
      } else if (!partition.isEmpty()) {
        var segmentStatuses = getCollectionOrPartitionStatuses(fs, rootPath)
        segmentStatuses
          .filter(status => validSegments.contains(status.getPath().getName))
          .foreach(status => {
            fieldMaps ++= getSegmentFieldMap(fs, status.getPath())
          })
      } else {
        var partitionStatuses = getCollectionOrPartitionStatuses(fs, rootPath)
        partitionStatuses.foreach(status => {
          val segmentStatuses =
            getCollectionOrPartitionStatuses(fs, status.getPath())
          segmentStatuses
            .filter(status => validSegments.contains(status.getPath().getName))
            .foreach(status => {
              fieldMaps ++= getSegmentFieldMap(fs, status.getPath())
            })
        })
      }
    } else {
      fieldMaps ++= getSegmentFieldMap(fs, rootPath)
    }

    val result = fieldMaps
      .map(fieldMap => MilvusInputPartition(fieldMap): InputPartition)
      .toArray
    fs.close()
    result
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    new MilvusReaderFactory(schema, options)
  }
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

case class MilvusInputPartition(fieldFiles: Map[String, String])
    extends InputPartition

class MilvusReaderFactory(
    schema: StructType,
    options: CaseInsensitiveStringMap
) extends PartitionReaderFactory {

  private val readerOptions = MilvusBinlogReaderOption(options)

  override def createReader(
      partition: InputPartition
  ): PartitionReader[InternalRow] = {
    val milvusPartition = partition.asInstanceOf[MilvusInputPartition]
    // Create the data reader with the file map, schema, and options
    new MilvusPartitionReader(schema, milvusPartition.fieldFiles, readerOptions)
  }
}
