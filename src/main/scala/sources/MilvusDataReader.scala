package com.zilliz.spark.connector.sources

import java.io.InputStream
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
  InsertEventData,
  LogReader
}
import com.zilliz.spark.connector.MilvusS3Option
import io.milvus.grpc.schema.{DataType => MilvusDataType}

class MilvusPartitionReader(
    schema: StructType,
    fieldFilesSeq: Seq[Map[String, String]],
    options: MilvusS3Option,
    pushedFilters: Array[Filter] = Array.empty[Filter]
) extends PartitionReader[InternalRow]
    with Logging {
  private val readerType: String = options.readerType
  private var fieldFileReaders: Map[String, FieldFileReader] = Map.empty
  private var currentFieldFilesIndex: Int = 0
  open()

  // Helper trait/class to abstract reading a single field file
  // This encapsulates the logic from MilvusBinlogPartitionReader
  private trait FieldFileReader {
    def open(): Unit // Open the file and prepare for reading
    def hasNext(): Boolean // Check if there are more records in this file
    def readNextRecord()
        : String // Read and parse the next record, return the single field value
    def getDataType(): MilvusDataType // Get the data type of the field
    def moveToNextRecord(): Unit // Move to the next record
    def close(): Unit // Close the file stream
  }

  private class MilvusBinlogFieldFileReader(
      filePath: String,
      options: MilvusS3Option
  ) extends FieldFileReader
      with Logging {
    private val path = options.getFilePath(filePath)
    private var fs: FileSystem = _
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
      fs = options.getFileSystem(path)
      inputStream = fs.open(path)
      // Read descriptor event to get data type
      descriptorEvent = LogReader.readDescriptorEvent(inputStream)
      dataType = descriptorEvent.data.payloadDataType
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
      if (insertEvent != null && currentIndex >= insertEvent.datas.length) {
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

    override def readNextRecord(): String = {
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

    private def readNextInsertRecord(): String = {
      if (insertEvent == null) {
        // This should not happen if hasNext() was called correctly
        throw new IllegalStateException(
          s"Attempted to read from null insert event in file $filePath"
        )
      }
      val data = insertEvent.datas(currentIndex)
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
      if (fs != null) {
        try {
          fs.close() // Revisit FS lifecycle management
        } catch {
          case e: Exception =>
            logWarning(s"Error closing file system for $filePath", e)
        }
      }
    }
  }

  // Open all field files when the reader is initialized
  // Note: Spark calls open() when the reader is ready to start reading
  def open(): Unit = {
    if (fieldFilesSeq.isEmpty) {
      throw new IllegalStateException("No field files provided in the sequence")
    }
    openCurrentFieldFiles()
    logInfo("All field file readers opened.")
  }

  private def openCurrentFieldFiles(): Unit = {
    // Close existing readers if any
    close()

    val currentFieldFiles = fieldFilesSeq(currentFieldFilesIndex)
    fieldFileReaders = currentFieldFiles.map { case (fieldName, filePath) =>
      // Create a FieldFileReader for each field's file
      val reader = new MilvusBinlogFieldFileReader(filePath, options)
      reader.open() // Open the individual file reader
      fieldName -> reader
    }
    if (
      fieldFileReaders.isEmpty && currentFieldFilesIndex < fieldFilesSeq.length - 1
    ) {
      currentFieldFilesIndex += 1
      openCurrentFieldFiles()
    }
  }

  // Check if there is a next row by checking if all field readers have a next record
  override def next(): Boolean = {
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
          openCurrentFieldFiles()
          return next() // Recursively try with next set of field files
        }
      }
    } while (hasNext)

    false // No more rows or no rows that pass the filters
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
      value: String,
      dataType: MilvusDataType
  ): Unit = {
    if (value == null) {
      row.setNullAt(ordinal)
      return
    }
    val sparkFieldType = schema.fields(ordinal).dataType
    dataType match {
      case MilvusDataType.String | MilvusDataType.VarChar |
          MilvusDataType.JSON =>
        row.update(ordinal, UTF8String.fromString(value.toString))
      case MilvusDataType.Int64 =>
        row.setLong(ordinal, value.toLong)
      case MilvusDataType.Int32 => row.setInt(ordinal, value.toInt)
      case MilvusDataType.Int16 => row.setShort(ordinal, value.toShort)
      case MilvusDataType.Int8  => row.setByte(ordinal, value.toByte)
      case MilvusDataType.Float =>
        row.setFloat(ordinal, value.toFloat)
      case MilvusDataType.Double =>
        row.setDouble(ordinal, value.toDouble)
      case MilvusDataType.Bool =>
        row.setBoolean(ordinal, value.toBoolean)
      case MilvusDataType.FloatVector =>
        val floatArray = value.split(",").map(_.toFloat)
        row.update(ordinal, ArrayData.toArrayData(floatArray))
      case MilvusDataType.Float16Vector =>
        val floatArray = value.split(",").map(_.toFloat)
        row.update(ordinal, ArrayData.toArrayData(floatArray))
      case MilvusDataType.BFloat16Vector =>
        val floatArray = value.split(",").map(_.toFloat)
        row.update(ordinal, ArrayData.toArrayData(floatArray))
      case MilvusDataType.BinaryVector =>
        val binaryArray = value.split(",").map(_.toByte)
        row.update(ordinal, ArrayData.toArrayData(binaryArray))
      case MilvusDataType.Int8Vector =>
        val int8Array = value.split(",").map(_.toByte)
        row.update(ordinal, ArrayData.toArrayData(int8Array))
      case MilvusDataType.SparseFloatVector =>
        val sparseMap = LogReader.stringToLongFloatMap(value)
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
          sparkArrayType.elementType match {
            case SparkDataTypes.BooleanType =>
              val array = value.split(",").map(_.toBoolean)
              row.update(ordinal, ArrayData.toArrayData(array))
            case SparkDataTypes.IntegerType =>
              val array = value.split(",").map(_.toInt)
              row.update(ordinal, ArrayData.toArrayData(array))
            case SparkDataTypes.LongType =>
              val array = value.split(",").map(_.toLong)
              row.update(ordinal, ArrayData.toArrayData(array))
            case SparkDataTypes.FloatType =>
              val array = value.split(",").map(_.toFloat)
              row.update(ordinal, ArrayData.toArrayData(array))
            case SparkDataTypes.DoubleType =>
              val array = value.split(",").map(_.toDouble)
              row.update(ordinal, ArrayData.toArrayData(array))
            case SparkDataTypes.StringType =>
              val array = value.split(",").map(UTF8String.fromString)
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

  // Close all field file readers
  override def close(): Unit = {
    logInfo("MilvusDataReader closing all field file readers.")
    fieldFileReaders.values.foreach { reader =>
      try {
        reader.close()
      } catch {
        case e: Exception => logWarning("Error closing field file reader", e)
      }
    }
    fieldFileReaders = Map.empty // Clear the map
    logInfo("All field file readers closed.")
  }
}
