package com.zilliz.spark.connector.sources

import java.io.InputStream
import java.util.concurrent.{CompletableFuture, Executors, TimeUnit}
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}
import scala.util.control.Breaks._

import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.fs.s3a.S3AFileSystem
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{
  ArrayType,
  DataTypes => SparkDataTypes,
  StringType,
  StructType
}
import org.apache.spark.unsafe.types.UTF8String

import com.zilliz.spark.connector.binlog.{
  Constants,
  DeleteEventData,
  DescriptorEvent,
  DescriptorEventData,
  LogReader
}
import com.zilliz.spark.connector.binlog.InsertEventData
import com.zilliz.spark.connector.MilvusS3Option
import io.milvus.grpc.schema.{DataType => MilvusDataType}

class MilvusPartitionReader(
    schema: StructType,
    fieldFilesSeq: Seq[Map[String, String]],
    options: MilvusS3Option,
    pushedFilters: Array[Filter] = Array.empty[Filter]
) extends PartitionReader[InternalRow]
    with Logging {

  // Shared FileSystem instance for all field file readers
  private var sharedFileSystem: FileSystem = _
  private var fieldFileReaders: Map[String, FieldFileReader] = Map.empty
  private var currentFieldFilesIndex: Int = 0 // Index for current reading

  // Enhanced preloading mechanism with 16 concurrent tasks
  private var preloadedBatches: Map[Int, Map[String, FieldFileReader]] =
    Map.empty
  private var preloadFutures: Map[Int, Future[Map[String, FieldFileReader]]] =
    Map.empty
  private var nextPreloadIndex: Int = 0 // Index for next preload task
  private val preloadExecutor =
    Executors.newFixedThreadPool(
      options.s3PreloadPoolSize
    ) // Increased to 16 threads
  private implicit val executionContext: ExecutionContext =
    ExecutionContext.fromExecutor(preloadExecutor)

  // Background preloading task
  private var backgroundPreloadTask: Option[Future[Unit]] = None
  private var stopBackgroundPreload: Boolean = false

  open()

  // Helper trait/class to abstract reading a single field file
  // This encapsulates the logic from MilvusBinlogPartitionReader
  private trait FieldFileReader {
    def open(): Unit // Open the file and prepare for reading
    def hasNext(): Boolean // Check if there are more records in this file
    def readNextRecord()
        : Any // Read and parse the next record, return the single field value
    def getDataType(): MilvusDataType // Get the data type of the field
    def moveToNextRecord(): Unit // Move to the next record
    def close(): Unit // Close the file stream
  }

  private class MilvusBinlogFieldFileReader(
      filePath: String,
      sharedFs: FileSystem, // Accept shared FileSystem instead of creating own
      options: MilvusS3Option
  ) extends FieldFileReader
      with Logging {

    private val path = options.getFilePath(filePath)
    private var inputStream: InputStream = _

    // Binlog specific state (adapt from MilvusBinlogPartitionReader)
    private val objectMapper =
      LogReader.getObjectMapper() // Assuming object mapper is reusable
    private var descriptorEvent: DescriptorEvent = _
    private var dataType: MilvusDataType = _ // Data type from descriptor event
    private var deleteEvent: DeleteEventData = null
    private var insertEvent: InsertEventData = null
    private var currentIndex: Int =
      0 // Index within the current event's data array
    private var currentReaderType: String =
      _ // Need to know if this file is insert or delete type

    // You might need to pass the expected reader type (insert/delete) for this field file
    // Or infer it from the file content/name if possible
    // For simplicity, assuming insert type for now, you need to handle delete as well
    // This could be passed via options or determined during scan planning
    currentReaderType =
      Constants.LogReaderTypeInsert // Placeholder - determine actual type

    override def open(): Unit = {
      // logInfo(s"Opening field file: $filePath")
      inputStream = sharedFs.open(path) // Use shared FileSystem
      // Read descriptor event to get data type
      descriptorEvent = LogReader.readDescriptorEvent(inputStream)
      dataType = descriptorEvent.data.payloadDataType
      hasNext()
      // logInfo(s"Opened file $filePath. Data type: $dataType")
    }

    override def getDataType(): MilvusDataType = dataType

    override def hasNext(): Boolean = {
      currentReaderType match {
        case Constants.LogReaderTypeInsert => hasNextInsertEvent()
        case Constants.LogReaderTypeDelete => hasNextDeleteEvent()
        case _ =>
          throw new IllegalStateException(
            s"Unknown reader type for file $filePath: $currentReaderType"
          )
      }
    }

    private def hasNextInsertEvent(): Boolean = {
      // Check if we need to read the next event
      if (insertEvent != null && currentIndex >= insertEvent.getDataSize()) {
        insertEvent = null
        currentIndex = 0
      }
      if (insertEvent == null) {
        insertEvent =
          LogReader.readInsertEvent(inputStream, objectMapper, dataType)
      }

      insertEvent != null
    }

    private def hasNextDeleteEvent(): Boolean = {
      if (deleteEvent != null && currentIndex == deleteEvent.pks.length - 1) {
        deleteEvent = null
        currentIndex = 0
      }
      if (deleteEvent == null) {
        deleteEvent =
          LogReader.readDeleteEvent(inputStream, objectMapper, dataType)
      }

      deleteEvent != null
    }

    override def readNextRecord(): Any = {
      currentReaderType match {
        case Constants.LogReaderTypeInsert => readNextInsertRecord()
        case Constants.LogReaderTypeDelete => readNextDeleteRecord()
        case _ =>
          throw new IllegalStateException(
            s"Unknown reader type for file $filePath: $currentReaderType"
          )
      }
    }

    override def moveToNextRecord(): Unit = {
      currentIndex += 1
    }

    private def readNextInsertRecord(): Any = {
      if (insertEvent == null) {
        // This should not happen if hasNext() was called correctly
        throw new IllegalStateException(
          s"Attempted to read from null insert event in file $filePath"
        )
      }
      val data = insertEvent.getData(currentIndex)
      data
    }

    private def readNextDeleteRecord(): String = {
      if (deleteEvent == null) {
        // This should not happen if hasNext() was called correctly
        throw new IllegalStateException(
          s"Attempted to read from null delete event in file $filePath"
        )
      }
      val pk = deleteEvent.pks(currentIndex)
      pk
    }

    override def close(): Unit = {
      // logInfo(s"Closing field file: $filePath")
      if (inputStream != null) {
        try {
          inputStream.close()
        } catch {
          case e: Exception =>
            logWarning(s"Error closing input stream for $filePath", e)
        }
      }
      // Note: Don't close shared FileSystem here
    }
  }

  // Initialize shared FileSystem
  private def initializeSharedFileSystem(): Unit = {
    if (fieldFilesSeq.nonEmpty && fieldFilesSeq.head.nonEmpty) {
      val samplePath = fieldFilesSeq.head.values.head
      val path = options.getFilePath(samplePath)
      sharedFileSystem = options.getFileSystem(path)
      logInfo(s"Initialized shared FileSystem for path: $samplePath")
    }
  }

  // Open all field files when the reader is initialized
  // Note: Spark calls open() when the reader is ready to start reading
  def open(): Unit = {
    if (fieldFilesSeq.isEmpty) {
      throw new IllegalStateException("No field files provided in the sequence")
    }

    initializeSharedFileSystem()

    // Start background preloading task for all files
    startBackgroundPreloading()

    // Log file statistics for debugging input size issues
    val totalFiles = fieldFilesSeq.map(_.size).sum
    val totalBatches = fieldFilesSeq.length
    logInfo(
      s"Opened MilvusPartitionReader with $totalBatches batches, $totalFiles total files"
    )
    logInfo("Background preloading started for all batches.")
  }

  // Background preloading task that continuously submits preload jobs
  private def startBackgroundPreloading(): Unit = {
    backgroundPreloadTask = Some(Future {
      while (
        !stopBackgroundPreload && nextPreloadIndex < fieldFilesSeq.length
      ) {
        // Check if we already have a future for this batch
        if (!preloadFutures.contains(nextPreloadIndex)) {
          val batchIndex = nextPreloadIndex
          preloadFutures += (batchIndex -> Future {
            try {
              val batchFieldFiles = fieldFilesSeq(batchIndex)
              val batchReaders =
                batchFieldFiles.map { case (fieldName, filePath) =>
                  val reader = new MilvusBinlogFieldFileReader(
                    filePath,
                    sharedFileSystem,
                    options
                  )
                  reader.open()
                  fieldName -> reader
                }
              logInfo(
                s"Background preloaded batch $batchIndex with ${batchReaders.size} field readers"
              )
              batchReaders
            } catch {
              case e: Exception =>
                logWarning(
                  s"Background preload failed for batch $batchIndex: ${e.getMessage}",
                  e
                )
                Map.empty[String, FieldFileReader]
            }
          })

          // Process completed futures and move them to preloadedBatches
          processCompletedFutures()
        }

        nextPreloadIndex += 1
      }

      logInfo("Background preloading task completed for all batches")
    })
  }

  // Process completed futures and move them to preloadedBatches
  private def processCompletedFutures(): Unit = {
    val completedFutures = preloadFutures.filter { case (_, future) =>
      future.isCompleted
    }

    completedFutures.foreach { case (batchIndex, future) =>
      future.value match {
        case Some(Success(readers)) if readers.nonEmpty =>
          preloadedBatches += (batchIndex -> readers)
          logInfo(
            s"Moved completed preload for batch $batchIndex to preloadedBatches"
          )
        case Some(Success(_)) =>
          logWarning(s"Preload for batch $batchIndex completed but was empty")
        case Some(Failure(e)) =>
          logWarning(s"Preload for batch $batchIndex failed: ${e.getMessage}")
        case None =>
          // Should not happen if isCompleted is true
          logWarning(
            s"Preload for batch $batchIndex has no value despite being completed"
          )
      }

      // Remove from futures map
      preloadFutures -= batchIndex
    }
  }

  private def closeCurrentReaders(): Unit = {
    fieldFileReaders.values.foreach { reader =>
      try {
        reader.close()
      } catch {
        case e: Exception =>
          logWarning(s"Error closing field reader", e)
      }
    }
    fieldFileReaders = Map.empty
  }

  // Check if there is a next row by checking if all field readers have a next record
  override def next(): Boolean = {
    // Check if we've reached the end of all batches
    if (currentFieldFilesIndex >= fieldFilesSeq.length) {
      return false
    }

    // Process any completed futures first
    processCompletedFutures()

    // Check if we need to load the current batch
    if (fieldFileReaders.isEmpty) {
      // Wait for the current batch to be preloaded
      waitForBatchToBePreloaded(currentFieldFilesIndex)
    }

    // Check if any reader is not initialized (shouldn't happen after open)
    if (fieldFileReaders.isEmpty) {
      logWarning(
        "MilvusDataReader.next() called before open() or with empty readers."
      )
      return false
    }

    var hasNext = false
    do {
      // Check if ALL field readers have a next record
      val allHaveNext = fieldFileReaders.values.forall(_.hasNext())

      // Consistency check: If some have next and some don't, it's an alignment error
      if (!allHaveNext && fieldFileReaders.values.exists(_.hasNext())) {
        val status = fieldFileReaders
          .map { case (name, reader) => s"$name: ${reader.hasNext()}" }
          .mkString(", ")
        throw new IllegalStateException(
          s"Record count mismatch between field files in partition. Status: $status"
        )
      }

      hasNext = allHaveNext

      // If we have a next record, check if it passes the filters
      if (hasNext) {
        val row = buildCurrentRow()
        if (applyFilters(row)) {
          return true // Found a row that passes the filters
        }
        // If the row doesn't pass the filters, move to next record and continue
        fieldFileReaders.values.foreach { reader =>
          reader.moveToNextRecord()
        }
      } else {
        // If no more records in current field files, try next set if available
        if (currentFieldFilesIndex < fieldFilesSeq.length - 1) {
          currentFieldFilesIndex += 1
          switchToNextBatch()
          return next() // Recursively try with next set of field files
        }
      }
    } while (hasNext)

    false // No more rows or no rows that pass the filters
  }

  private def switchToNextBatch(): Unit = {
    // Check if we've reached the end of all batches
    if (currentFieldFilesIndex >= fieldFilesSeq.length) {
      return
    }

    // Close current readers
    closeCurrentReaders()

    // Wait for the next batch to be preloaded
    waitForBatchToBePreloaded(currentFieldFilesIndex)
  }

  // Wait for a specific batch to be preloaded
  private def waitForBatchToBePreloaded(batchIndex: Int): Unit = {
    // Check if batchIndex is valid
    if (batchIndex >= fieldFilesSeq.length) {
      throw new IllegalStateException(
        s"Batch index $batchIndex is out of bounds (max: ${fieldFilesSeq.length - 1})"
      )
    }

    // First check if it's already in preloadedBatches
    preloadedBatches.get(batchIndex) match {
      case Some(preloadedReaders) if preloadedReaders.nonEmpty =>
        fieldFileReaders = preloadedReaders
        preloadedBatches -= batchIndex
        logInfo(s"Loaded batch $batchIndex from preloaded batches")

      case _ =>
        // Check if there's a future for this batch
        preloadFutures.get(batchIndex) match {
          case Some(future) =>
            // Wait for the future to complete with infinite timeout and periodic warnings
            logInfo(s"Waiting for batch $batchIndex to be preloaded...")
            try {
              val preloadedReaders =
                waitForFutureWithWarnings(future, batchIndex)
              preloadFutures -= batchIndex
              if (preloadedReaders.nonEmpty) {
                fieldFileReaders = preloadedReaders
                logInfo(s"Successfully loaded batch $batchIndex after waiting")
              } else {
                throw new IllegalStateException(
                  s"Batch $batchIndex preload returned empty readers"
                )
              }
            } catch {
              case e: Exception =>
                throw new IllegalStateException(
                  s"Batch $batchIndex preload failed: ${e.getMessage}"
                )
            }
          case None =>
            throw new IllegalStateException(
              s"No preload task found for batch $batchIndex"
            )
        }
    }
  }

  // Wait for future with infinite timeout and periodic warnings
  private def waitForFutureWithWarnings[T](
      future: Future[T],
      batchIndex: Int
  ): T = {
    val startTime = System.currentTimeMillis()
    val initialSleepTime = 500L // Start with 500ms
    val maxSleepTime = 5000L // Max sleep time of 5 seconds
    val backoffMultiplier = 2 // Multiply sleep time by this factor
    var currentSleepTime = initialSleepTime
    var lastWarningTime = 0L
    val warningInterval = 30000L // 30 seconds warning interval

    while (!future.isCompleted) {
      val elapsedTime = System.currentTimeMillis() - startTime

      // Output warning every 30 seconds
      if (elapsedTime - lastWarningTime >= warningInterval) {
        logWarning(
          s"Still waiting for batch $batchIndex to be preloaded after ${elapsedTime / 1000} seconds..."
        )
        lastWarningTime = elapsedTime
      }

      // Sleep with backoff strategy
      Thread.sleep(currentSleepTime)

      // Increase sleep time with backoff, but cap at maxSleepTime
      currentSleepTime =
        Math.min((currentSleepTime * backoffMultiplier).toLong, maxSleepTime)
    }

    // Now that future is completed, get the result
    future.value match {
      case Some(Success(result))    => result.asInstanceOf[T]
      case Some(Failure(exception)) => throw exception
      case None =>
        throw new IllegalStateException(
          s"Future for batch $batchIndex completed but has no value"
        )
    }
  }

  // Get the current row by reading one record from each field reader
  override def get(): InternalRow = {
    if (fieldFileReaders.isEmpty) {
      throw new IllegalStateException(
        "MilvusDataReader.get() called before open() or with empty readers."
      )
    }

    // Create a new InternalRow with the correct number of fields
    val row = InternalRow.fromSeq(Seq.fill(fieldFileReaders.size)(null))

    // Read one record from each field reader and set the corresponding field in the row
    // Ensure the order matches the schema's field order
    // Convert field names to integers and sort them
    val sortedFieldIndices = fieldFileReaders.keys
      .map(fieldName => fieldName.toInt)
      .toSeq
      .sorted
      .zipWithIndex
    sortedFieldIndices.foreach { case (fieldID, index) =>
      fieldFileReaders.get(fieldID.toString) match {
        case Some(reader) =>
          try {
            val fieldValue = reader.readNextRecord()
            reader.moveToNextRecord()
            setInternalRowValue(
              row,
              index,
              fieldValue,
              reader.getDataType()
            )
          } catch {
            case e: Exception =>
              logError(
                s"Error reading record for field '$fieldID' from file ${fieldFilesSeq(currentFieldFilesIndex)
                    .getOrElse(fieldID.toString, "N/A")}",
                e
              )
              // Handle error: set null, throw exception, or skip row
              row.setNullAt(index) // Example: set null on error
          }
        case None =>
          // This should not happen if planInputPartitions and open were correct
          logError(s"No reader found for schema field: $fieldID")
          row.setNullAt(index) // Set null if reader is missing
      }
    }

    row // Return the populated row
  }

  // Helper function to set value in InternalRow with type conversion
  // This needs to handle different Spark SQL data types
  private def setInternalRowValue(
      row: InternalRow,
      ordinal: Int,
      inputValue: Any,
      dataType: MilvusDataType
  ): Unit = {
    if (inputValue == null) {
      row.setNullAt(ordinal)
      return
    }
    val sparkFieldType = schema.fields(ordinal).dataType
    dataType match {
      case MilvusDataType.String | MilvusDataType.VarChar |
          MilvusDataType.JSON =>
        row.update(ordinal, UTF8String.fromString(inputValue.toString))
      case MilvusDataType.Int64 =>
        row.setLong(ordinal, inputValue.asInstanceOf[Long])
      case MilvusDataType.Int32 =>
        row.setInt(ordinal, inputValue.asInstanceOf[Int])
      case MilvusDataType.Int16 =>
        row.setShort(ordinal, inputValue.asInstanceOf[Short])
      case MilvusDataType.Int8 =>
        row.setByte(ordinal, inputValue.asInstanceOf[Byte])
      case MilvusDataType.Float =>
        row.setFloat(ordinal, inputValue.asInstanceOf[Float])
      case MilvusDataType.Double =>
        row.setDouble(ordinal, inputValue.asInstanceOf[Double])
      case MilvusDataType.Bool =>
        row.setBoolean(ordinal, inputValue.asInstanceOf[Boolean])
      case MilvusDataType.FloatVector =>
        row.update(
          ordinal,
          ArrayData.toArrayData(inputValue.asInstanceOf[Array[Float]])
        )
      case MilvusDataType.Float16Vector =>
        row.update(
          ordinal,
          ArrayData.toArrayData(inputValue.asInstanceOf[Array[Float]])
        )
      case MilvusDataType.BFloat16Vector =>
        row.update(
          ordinal,
          ArrayData.toArrayData(inputValue.asInstanceOf[Array[Float]])
        )
      case MilvusDataType.BinaryVector =>
        row.update(
          ordinal,
          ArrayData.toArrayData(inputValue.asInstanceOf[Array[Byte]])
        )
      case MilvusDataType.Int8Vector =>
        row.update(
          ordinal,
          ArrayData.toArrayData(inputValue.asInstanceOf[Array[Byte]])
        )
      case MilvusDataType.SparseFloatVector =>
        val sparseMap = inputValue.asInstanceOf[Map[Long, Float]]
        row.update(
          ordinal,
          ArrayBasedMapData.apply(
            sparseMap.keys.toArray,
            sparseMap.values.toArray
          )
        )
      case MilvusDataType.Array =>
        if (sparkFieldType.isInstanceOf[ArrayType]) {
          val sparkArrayType = sparkFieldType.asInstanceOf[ArrayType]
          val arrayValue = inputValue.asInstanceOf[Array[String]]
          sparkArrayType.elementType match {
            case SparkDataTypes.BooleanType =>
              val array = arrayValue.map(_.toBoolean)
              row.update(ordinal, ArrayData.toArrayData(array))
            case SparkDataTypes.IntegerType =>
              val array = arrayValue.map(_.toInt)
              row.update(ordinal, ArrayData.toArrayData(array))
            case SparkDataTypes.LongType =>
              val array = arrayValue.map(_.toLong)
              row.update(ordinal, ArrayData.toArrayData(array))
            case SparkDataTypes.FloatType =>
              val array = arrayValue.map(_.toFloat)
              row.update(ordinal, ArrayData.toArrayData(array))
            case SparkDataTypes.DoubleType =>
              val array = arrayValue.map(_.toDouble)
              row.update(ordinal, ArrayData.toArrayData(array))
            case SparkDataTypes.StringType =>
              val array = arrayValue.map(UTF8String.fromString(_))
              row.update(ordinal, ArrayData.toArrayData(array))
            case _ =>
              throw new UnsupportedOperationException(
                s"Unsupported data type for setting value at ordinal $ordinal: $dataType"
              )
          }
        } else {
          throw new UnsupportedOperationException(
            s"Unsupported data type for setting value at ordinal $ordinal: $dataType"
          )
        }
      case _ =>
        logWarning(
          s"Unsupported data type for setting value at ordinal $ordinal: $dataType"
        )
        row.setNullAt(ordinal)
    }
  }

  private def buildCurrentRow(): InternalRow = {
    if (fieldFileReaders.isEmpty) {
      throw new IllegalStateException(
        "MilvusDataReader.buildCurrentRow() called before open() or with empty readers."
      )
    }

    // Create a new InternalRow with the correct number of fields
    val row = InternalRow.fromSeq(Seq.fill(fieldFileReaders.size)(null))

    // Read one record from each field reader and set the corresponding field in the row
    // Ensure the order matches the schema's field order
    // Convert field names to integers and sort them
    val sortedFieldIndices = fieldFileReaders.keys
      .map(fieldName => fieldName.toInt)
      .toSeq
      .sorted
      .zipWithIndex
    sortedFieldIndices.foreach { case (fieldID, index) =>
      fieldFileReaders.get(fieldID.toString) match {
        case Some(reader) =>
          try {
            val fieldValue = reader.readNextRecord()
            setInternalRowValue(
              row,
              index,
              fieldValue,
              reader.getDataType()
            )
          } catch {
            case e: Exception =>
              logError(
                s"Error reading record for field '$fieldID' from file ${fieldFilesSeq(currentFieldFilesIndex)
                    .getOrElse(fieldID.toString, "N/A")}",
                e
              )
              // Handle error: set null, throw exception, or skip row
              row.setNullAt(index) // Example: set null on error
          }
        case None =>
          // This should not happen if planInputPartitions and open were correct
          logError(s"No reader found for schema field: $fieldID")
          row.setNullAt(index) // Set null if reader is missing
      }
    }

    row // Return the populated row
  }

  private def applyFilters(row: InternalRow): Boolean = {
    import org.apache.spark.sql.sources._
    import org.apache.spark.unsafe.types.UTF8String

    if (pushedFilters.isEmpty) {
      return true
    }

    pushedFilters.forall(filter => evaluateFilter(filter, row))
  }

  private def evaluateFilter(filter: Filter, row: InternalRow): Boolean = {
    import org.apache.spark.sql.sources._
    import org.apache.spark.unsafe.types.UTF8String

    filter match {
      case EqualTo(attr, value) =>
        val columnIndex = getColumnIndex(attr)
        if (columnIndex == -1) return true
        val rowValue = getRowValue(row, columnIndex, attr)
        rowValue == value

      case GreaterThan(attr, value) =>
        val columnIndex = getColumnIndex(attr)
        if (columnIndex == -1) return true
        val rowValue = getRowValue(row, columnIndex, attr)
        compareValues(rowValue, value) > 0

      case GreaterThanOrEqual(attr, value) =>
        val columnIndex = getColumnIndex(attr)
        if (columnIndex == -1) return true
        val rowValue = getRowValue(row, columnIndex, attr)
        compareValues(rowValue, value) >= 0

      case LessThan(attr, value) =>
        val columnIndex = getColumnIndex(attr)
        if (columnIndex == -1) return true
        val rowValue = getRowValue(row, columnIndex, attr)
        compareValues(rowValue, value) < 0

      case LessThanOrEqual(attr, value) =>
        val columnIndex = getColumnIndex(attr)
        if (columnIndex == -1) return true
        val rowValue = getRowValue(row, columnIndex, attr)
        compareValues(rowValue, value) <= 0

      case In(attr, values) =>
        val columnIndex = getColumnIndex(attr)
        if (columnIndex == -1) return true
        val rowValue = getRowValue(row, columnIndex, attr)
        values.contains(rowValue)

      case IsNull(attr) =>
        val columnIndex = getColumnIndex(attr)
        if (columnIndex == -1) return true
        row.isNullAt(columnIndex)

      case IsNotNull(attr) =>
        val columnIndex = getColumnIndex(attr)
        if (columnIndex == -1) return true
        !row.isNullAt(columnIndex)

      case And(left, right) =>
        evaluateFilter(left, row) && evaluateFilter(right, row)

      case Or(left, right) =>
        evaluateFilter(left, row) || evaluateFilter(right, row)

      case _ => true // Unsupported filter, don't filter out
    }
  }

  private def getColumnIndex(columnName: String): Int = {
    try {
      schema.fieldIndex(columnName)
    } catch {
      case _: IllegalArgumentException => -1
    }
  }

  private def getRowValue(
      row: InternalRow,
      columnIndex: Int,
      columnName: String
  ): Any = {
    if (row.isNullAt(columnIndex)) {
      return null
    }

    schema.fields(columnIndex).dataType match {
      case SparkDataTypes.LongType    => row.getLong(columnIndex)
      case SparkDataTypes.IntegerType => row.getInt(columnIndex)
      case SparkDataTypes.FloatType   => row.getFloat(columnIndex)
      case SparkDataTypes.DoubleType  => row.getDouble(columnIndex)
      case SparkDataTypes.StringType =>
        row.getUTF8String(columnIndex).toString
      case SparkDataTypes.BooleanType => row.getBoolean(columnIndex)
      case _ => row.get(columnIndex, schema.fields(columnIndex).dataType)
    }
  }

  private def compareValues(rowValue: Any, filterValue: Any): Int = {
    (rowValue, filterValue) match {
      case (rv: Long, fv: Long)       => rv.compareTo(fv)
      case (rv: Long, fv: Int)        => rv.compareTo(fv.toLong)
      case (rv: Int, fv: Long)        => rv.toLong.compareTo(fv)
      case (rv: Int, fv: Int)         => rv.compareTo(fv)
      case (rv: Float, fv: Float)     => rv.compareTo(fv)
      case (rv: Double, fv: Double)   => rv.compareTo(fv)
      case (rv: String, fv: String)   => rv.compareTo(fv)
      case (rv: Boolean, fv: Boolean) => rv.compareTo(fv)
      case (rv: Long, fv: String)     => rv.toString.compareTo(fv)
      case (rv: String, fv: Long)     => rv.compareTo(fv.toString)
      case (rv: Boolean, fv: String)  => rv.toString.compareTo(fv)
      case (rv: String, fv: Boolean)  => rv.compareTo(fv.toString)
      case _ => 0 // Default to equal if types don't match
    }
  }

  // Close all field file readers and cleanup resources
  override def close(): Unit = {
    logInfo("MilvusDataReader closing all resources.")

    // Stop background preloading task
    stopBackgroundPreload = true
    backgroundPreloadTask.foreach { future =>
      if (!future.isCompleted) {
        logInfo("Background preload task is still running, will be stopped")
      }
    }
    backgroundPreloadTask = None

    // Cancel preload futures if still running
    preloadFutures.values.foreach { future =>
      if (!future.isCompleted) {
        // We can't actually cancel Scala Future, but we can ignore the result
        logInfo(
          "Preload future is still running, will be ignored on completion"
        )
      }
    }
    preloadFutures = Map.empty

    // Close current field file readers
    closeCurrentReaders()

    // Close all preloaded batch readers
    preloadedBatches.values.foreach { readers =>
      readers.values.foreach { reader =>
        try {
          reader.close()
        } catch {
          case e: Exception =>
            logWarning("Error closing preloaded batch field file reader", e)
        }
      }
    }
    preloadedBatches = Map.empty

    // Close shared FileSystem
    if (sharedFileSystem != null) {
      try {
        sharedFileSystem.close()
        logInfo("Shared FileSystem closed successfully")
      } catch {
        case e: Exception =>
          logWarning("Error closing shared FileSystem", e)
      }
      sharedFileSystem = null
    }

    // Shutdown executor for preloading
    try {
      preloadExecutor.shutdown()
      if (!preloadExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
        preloadExecutor.shutdownNow()
        logWarning(
          "Preload executor did not terminate gracefully, forced shutdown"
        )
      } else {
        logInfo("Preload executor shutdown successfully")
      }
    } catch {
      case e: Exception =>
        logWarning("Error shutting down preload executor", e)
        preloadExecutor.shutdownNow()
    }

    logInfo("All MilvusDataReader resources closed.")
  }
}
