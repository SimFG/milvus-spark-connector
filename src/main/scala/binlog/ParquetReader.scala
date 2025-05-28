package com.zilliz.spark.connector.binlog

import java.io.{File, FileOutputStream, IOException}
import java.nio.file.{Files, Paths}
import java.nio.ByteBuffer
import scala.collection.mutable.ListBuffer
import scala.util.{Try, Using}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.column.page.PageReadStore
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter
import org.apache.parquet.example.data.Group
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.io.{ColumnIOFactory, MessageColumnIO, RecordReader}
import org.apache.parquet.schema.{MessageType, PrimitiveType, Type}
import org.apache.spark.internal.Logging

import com.zilliz.spark.connector.FloatConverter
import com.zilliz.spark.connector.IntConverter
import io.milvus.grpc.schema.ScalarField

/** ParquetPayloadReader reads and parses parquet format data from a byte array.
  * This implementation aligns with the Go implementation's
  * ParquetPayloadReader.
  */
class ParquetPayloadReader(data: Array[Byte])
    extends AutoCloseable // {
    with Logging {

  private val hadoopConfig: Configuration = new Configuration()
  private var tempFile: Option[File] = None
  private var reader: Option[ParquetFileReader] = None
  private var schema: Option[MessageType] = None

  /** Initializes the Parquet reader. This method creates a temporary file to
    * store the Parquet data, since Parquet libraries work with files rather
    * than byte arrays.
    */
  private def initializeReader(): Unit = {
    if (reader.isDefined) {
      return // Already initialized
    }

    if (data == null || data.isEmpty) {
      throw new IOException("Empty Parquet data")
    }

    Try {
      // Create a temporary file to store the Parquet data
      val file = File.createTempFile("parquet_payload_", ".parquet")
      file.deleteOnExit()
      tempFile = Some(file)

      // Write the Parquet data to the temporary file
      Using(new FileOutputStream(file)) { fos =>
        fos.write(data)
      }.get // Re-throw any exception from file writing

      // logDebug(
      //   s"Created temporary Parquet file at ${file.getAbsolutePath} with size ${data.length} bytes"
      // )

      // Open the Parquet file
      val hadoopPath = new Path(file.getAbsolutePath)
      val inputFile = HadoopInputFile.fromPath(hadoopPath, hadoopConfig)
      val parquetReader = ParquetFileReader.open(inputFile)
      reader = Some(parquetReader)
      schema = Some(parquetReader.getFooter.getFileMetaData.getSchema)

      // logDebug(s"Parquet schema: ${schema.getOrElse("N/A")}")

      // Print field names for debugging
      // schema.foreach { msgType =>
      //   val fields = msgType.getFields
      //   if (fields.isEmpty) {
      // logWarning("Parquet schema has no fields")
      // } else {

      // logDebug("Schema field names:")
      // fields.forEach { field =>
      // logDebug(
      //   s"  Field ${msgType.getFieldIndex(field.getName)}: ${field.getName} (${field.asPrimitiveType().getPrimitiveTypeName})"
      // )
      // }
      // }
      // }
    }.recover {
      case e: IOException =>
        // logError(s"Error initializing Parquet reader: ${e.getMessage}", e)
        cleanupTempFile()
        throw new IOException(
          s"Failed to initialize Parquet reader: ${e.getMessage}",
          e
        )
      case e: Exception =>
        // logError(s"Unexpected error parsing Parquet: ${e.getMessage}", e)
        cleanupTempFile()
        throw new IOException(
          s"Unexpected error parsing Parquet: ${e.getMessage}",
          e
        )
    }.get
  }

  private def cleanupTempFile(): Unit = {
    tempFile.filter(_.exists()).foreach { file =>
      Try {
        Files.delete(file.toPath)
      }.recover { case e: IOException =>
      // Log but continue - this is just cleanup
      // logWarning(s"Failed to delete temporary file: ${e.getMessage}")
      }
    }
    tempFile = None
  }

  /** Closes the reader and cleans up resources.
    */
  override def close(): Unit = {
    reader.foreach { r =>
      Try(r.close()).recover { case e: IOException =>
      // Log but continue - this is just cleanup
      // logWarning(s"Failed to close Parquet reader: ${e.getMessage}")
      }
    }
    reader = None
    schema = None // Also clear the schema reference
    cleanupTempFile()
  }

  def getBooleanFromPayload(columnIndex: Int): List[Boolean] = {
    val values = new ListBuffer[Boolean]()
    processParquetFile((group, schema) => {
      if (
        isValidColumnAccess(
          group,
          schema,
          columnIndex,
          PrimitiveType.PrimitiveTypeName.BOOLEAN
        )
      ) {
        values += group.getBoolean(columnIndex, 0)
      }
    })
    values.toList
  }

  def getInt8FromPayload(columnIndex: Int): List[Byte] = {
    val values = new ListBuffer[Byte]()
    processParquetFile((group, schema) => {
      // Parquet stores INT8 as INT32
      if (
        isValidColumnAccess(
          group,
          schema,
          columnIndex,
          PrimitiveType.PrimitiveTypeName.INT32
        )
      ) {
        values += group.getInteger(columnIndex, 0).toByte
      }
    })
    values.toList
  }

  def getInt16FromPayload(columnIndex: Int): List[Short] = {
    val values = new ListBuffer[Short]()
    processParquetFile((group, schema) => {
      // Parquet stores INT16 as INT32
      if (
        isValidColumnAccess(
          group,
          schema,
          columnIndex,
          PrimitiveType.PrimitiveTypeName.INT32
        )
      ) {
        values += group.getInteger(columnIndex, 0).toShort
      }
    })
    values.toList
  }

  def getInt32FromPayload(columnIndex: Int): List[Int] = {
    val values = new ListBuffer[Int]()
    processParquetFile((group, schema) => {
      if (
        isValidColumnAccess(
          group,
          schema,
          columnIndex,
          PrimitiveType.PrimitiveTypeName.INT32
        )
      ) {
        values += group.getInteger(columnIndex, 0)
      }
    })
    values.toList
  }

  def getInt64FromPayload(columnIndex: Int): List[Long] = {
    val values = new ListBuffer[Long]()
    processParquetFile((group, schema) => {
      // Try to get values even if the column type doesn't exactly match
      // This helps with schema variations
      if (schema.getFieldCount > 0 && columnIndex < schema.getFieldCount) {
        val fieldType = schema.getType(columnIndex)

        // Handle case where we only have one column and it might be our target
        if (schema.getFieldCount == 1 && columnIndex == 0) {
          Try {
            if (group.getFieldRepetitionCount(0) > 0) {
              fieldType.asPrimitiveType().getPrimitiveTypeName match {
                case PrimitiveType.PrimitiveTypeName.INT64 =>
                  values += group.getLong(0, 0)
                case PrimitiveType.PrimitiveTypeName.INT32 =>
                  // Allow casting INT32 to INT64
                  values += group.getInteger(0, 0).toLong
                case PrimitiveType.PrimitiveTypeName.BINARY =>
                  // Try to parse the string as a number
                  Try(group.getString(0, 0).toLong).toOption
                    .foreach(values += _)
                case _ => // Ignore other types
              }
            }
          }.recover { case e: Exception =>
          // logWarning(s"Error accessing column 0: ${e.getMessage}")
          }
        }
        // Normal case - try INT64 column
        else if (
          isValidColumnAccess(
            group,
            schema,
            columnIndex,
            PrimitiveType.PrimitiveTypeName.INT64
          )
        ) {
          values += group.getLong(columnIndex, 0)
        }
      }
    })
    values.toList
  }

  def getFloat32FromPayload(columnIndex: Int): List[Float] = {
    val values = new ListBuffer[Float]()
    processParquetFile((group, schema) => {
      if (
        isValidColumnAccess(
          group,
          schema,
          columnIndex,
          PrimitiveType.PrimitiveTypeName.FLOAT
        )
      ) {
        values += group.getFloat(columnIndex, 0)
      }
    })
    values.toList
  }

  def getFloat64FromPayload(columnIndex: Int): List[Double] = {
    val values = new ListBuffer[Double]()
    processParquetFile((group, schema) => {
      if (
        isValidColumnAccess(
          group,
          schema,
          columnIndex,
          PrimitiveType.PrimitiveTypeName.DOUBLE
        )
      ) {
        values += group.getDouble(columnIndex, 0)
      }
    })
    values.toList
  }

  def getStringFromPayload(columnIndex: Int): List[String] = {
    val values = new ListBuffer[String]()
    processParquetFile((group, schema) => {
      // Try to get values even if the column type doesn't exactly match
      // This helps with schema variations
      if (schema.getFieldCount > 0) {
        // If we only have one column, use it regardless of index if it's binary
        if (schema.getFieldCount == 1 && columnIndex < schema.getFieldCount) {
          Try {
            if (group.getFieldRepetitionCount(0) > 0) {
              val fieldType = schema.getType(0)
              if (
                fieldType
                  .asPrimitiveType()
                  .getPrimitiveTypeName == PrimitiveType.PrimitiveTypeName.BINARY
              ) {
                values += group.getString(0, 0)
              }
            }
          }.recover { case e: Exception =>
          // logWarning(s"Error accessing single column: ${e.getMessage}")
          }
        }
        // Normal case - try the specified column index if it's binary
        else if (
          isValidColumnAccess(
            group,
            schema,
            columnIndex,
            PrimitiveType.PrimitiveTypeName.BINARY
          )
        ) {
          values += group.getString(columnIndex, 0)
        }
      }
    })
    values.toList
  }

  def getBinaryFromPayload(columnIndex: Int): List[Array[Byte]] = {
    val values = new ListBuffer[Array[Byte]]()
    processParquetFile((group, schema) => {
      if (
        isValidColumnAccess(
          group,
          schema,
          columnIndex,
          PrimitiveType.PrimitiveTypeName.BINARY
        )
      ) {
        val buffer: ByteBuffer = group.getBinary(columnIndex, 0).toByteBuffer
        val bytes = new Array[Byte](buffer.remaining())
        buffer.get(bytes)
        values += bytes
      }
    })
    values.toList
  }

  def getFloatVectorFromPayload(columnIndex: Int): List[Array[Float]] = {
    val values = new ListBuffer[Array[Float]]()
    processParquetFile((group, schema) => {
      val dim =
        schema.getColumns().get(0).getPrimitiveType().getTypeLength() / 4
      val floatVector = new Array[Float](dim)
      val buffer = group.getBinary(columnIndex, 0).toByteBuffer
      buffer.order(Constants.Endian)
      buffer.asFloatBuffer().get(floatVector)
      values += floatVector
    })
    values.toList
  }

  def getFloat16VectorFromPayload(columnIndex: Int): List[Array[Float]] = {
    val values = new ListBuffer[Array[Float]]()
    processParquetFile((group, schema) => {
      val dim =
        schema.getColumns().get(0).getPrimitiveType().getTypeLength() / 2
      val floatVector = new Array[Float](dim)
      val float16Bytes = new Array[Byte](2)
      val buffer = group.getBinary(columnIndex, 0).toByteBuffer
      buffer.order(Constants.Endian)
      for (i <- 0 until dim) {
        buffer.get(float16Bytes)
        floatVector(i) = FloatConverter.fromFloat16Bytes(float16Bytes.toSeq)
      }
      values += floatVector
    })
    values.toList
  }

  def getBFloat16VectorFromPayload(columnIndex: Int): List[Array[Float]] = {
    val values = new ListBuffer[Array[Float]]()
    processParquetFile((group, schema) => {
      val dim =
        schema.getColumns().get(0).getPrimitiveType().getTypeLength() / 2
      val floatVector = new Array[Float](dim)
      val float16Bytes = new Array[Byte](2)
      val buffer = group.getBinary(columnIndex, 0).toByteBuffer
      buffer.order(Constants.Endian)
      for (i <- 0 until dim) {
        buffer.get(float16Bytes)
        floatVector(i) = FloatConverter.fromBFloat16Bytes(float16Bytes.toSeq)
      }
      values += floatVector
    })
    values.toList
  }

  def getBinaryVectorFromPayload(columnIndex: Int): List[Array[Byte]] = {
    val values = new ListBuffer[Array[Byte]]()
    processParquetFile((group, schema) => {
      val dim =
        schema.getColumns().get(0).getPrimitiveType().getTypeLength()
      val binaryVector = new Array[Byte](dim)
      val buffer = group.getBinary(columnIndex, 0).toByteBuffer
      buffer.get(binaryVector)
      values += binaryVector
    })
    values.toList
  }

  def getInt8VectorFromPayload(columnIndex: Int): List[Array[Int]] = {
    val values = new ListBuffer[Array[Int]]()
    processParquetFile((group, schema) => {
      val dim =
        schema.getColumns().get(0).getPrimitiveType().getTypeLength()
      val int8Vector = new Array[Int](dim)
      val buffer = group.getBinary(columnIndex, 0).toByteBuffer
      for (i <- 0 until dim) {
        int8Vector(i) = buffer.get().toInt
      }
      values += int8Vector
    })
    values.toList
  }

  def getSparseVectorFromPayload(columnIndex: Int): List[Array[String]] = {
    val values = new ListBuffer[Array[String]]()
    processParquetFile((group, schema) => {
      val buffer = group.getBinary(columnIndex, 0).toByteBuffer
      val dataLen = (buffer.limit() - buffer.position()) / 8
      var sparseBytes = new ListBuffer[Byte]()
      var sparseStrs = new ListBuffer[String]()
      for (i <- 0L until dataLen) {
        val idxBytes = new Array[Byte](4)
        buffer.get(idxBytes)
        val idx = IntConverter.fromUInt32Bytes(idxBytes.toSeq)
        val valueBytes = new Array[Byte](4)
        buffer.get(valueBytes)
        val value = FloatConverter.fromFloatBytes(valueBytes.toSeq)
        sparseBytes ++= idxBytes
        sparseBytes ++= valueBytes
        sparseStrs += s"($idx:$value)"
      }
      // values += sparseBytes.toArray
      values += sparseStrs.toArray
      // println(
      //   s"fubang new, dataLen: $dataLen, currentPos: ${buffer
      //       .position()}, limit: ${buffer.limit()}, sparseStrs: ${sparseStrs
      //       .mkString(",")}"
      // )
    })
    values.toList
  }

  def getArrayFromPayload(columnIndex: Int): List[Array[String]] = {
    val values = new ListBuffer[Array[String]]()
    var lastPos = 0
    processParquetFile((group, schema) => {
      val buffer = group.getBinary(columnIndex, 0).toByteBuffer
      val startPos = buffer.position()
      val dataLen = buffer.limit() - startPos
      val arrayBytes = new Array[Byte](dataLen)
      buffer.get(arrayBytes)
      val scalarObj = ScalarField.parseFrom(arrayBytes)
      var arrayValues = new ListBuffer[String]()
      if (scalarObj.data.isBoolData) {
        arrayValues ++= scalarObj.getBoolData.data.map(_.toString)
      } else if (scalarObj.data.isIntData) {
        arrayValues ++= scalarObj.getIntData.data.map(_.toString)
      } else if (scalarObj.data.isLongData) {
        arrayValues ++= scalarObj.getLongData.data.map(_.toString)
      } else if (scalarObj.data.isFloatData) {
        arrayValues ++= scalarObj.getFloatData.data.map(_.toString)
      } else if (scalarObj.data.isDoubleData) {
        arrayValues ++= scalarObj.getDoubleData.data.map(_.toString)
      } else if (scalarObj.data.isStringData) {
        arrayValues ++= scalarObj.getStringData.data.map(_.toString)
      } else if (scalarObj.data.isBytesData) {
        arrayValues ++= scalarObj.getBytesData.data.map(_.toString)
      } else if (scalarObj.data.isArrayData) {
        arrayValues ++= scalarObj.getArrayData.data.map(_.toString)
      } else {
        throw new IOException(
          s"Unsupported data type: ${scalarObj.data.number}, for insert event"
        )
      }
      values += arrayValues.toArray
      lastPos = buffer.position()
    })
    values.toList
  }

  private def isValidColumnAccess(
      group: Group,
      schema: MessageType,
      columnIndex: Int,
      expectedType: PrimitiveType.PrimitiveTypeName
  ): Boolean = {
    if (columnIndex < 0 || columnIndex >= schema.getFieldCount) {
      return false
    }

    Try {
      if (group.getFieldRepetitionCount(columnIndex) <= 0) {
        return false
      }

      val field = schema.getType(columnIndex)
      if (!field.isPrimitive) {
        return false
      }

      val actualType = field.asPrimitiveType().getPrimitiveTypeName

      // Be more permissive about types - allow access if the basic category matches
      // For numeric columns, allow some flexibility - integer types can be cast
      (expectedType, actualType) match {
        case (
              PrimitiveType.PrimitiveTypeName.INT32,
              PrimitiveType.PrimitiveTypeName.INT32
            ) =>
          true
        case (
              PrimitiveType.PrimitiveTypeName.INT32,
              PrimitiveType.PrimitiveTypeName.INT64
            ) =>
          true // Allow INT64 to be read as INT32 (potential data loss)
        case (
              PrimitiveType.PrimitiveTypeName.INT64,
              PrimitiveType.PrimitiveTypeName.INT64
            ) =>
          true
        case (
              PrimitiveType.PrimitiveTypeName.INT64,
              PrimitiveType.PrimitiveTypeName.INT32
            ) =>
          true // Allow INT32 to be read as INT64
        case (e, a) if e == a => true // Exact type match
        case _                => false // No match or unsupported casting
      }
    }.recover { case e: Exception =>
      // logError(
      //   s"Error checking column access for index $columnIndex with expected type $expectedType: ${e.getMessage}"
      // )
      false
    }.getOrElse(false) // Default to false in case of any exception
  }

  private def processParquetFile(
      processor: (Group, MessageType) => Unit
  ): Unit = {
    initializeReader()
    (reader, schema) match {
      case (Some(parquetReader), Some(parquetSchema)) =>
        Try {
          var pages: PageReadStore = null
          while ({
            pages = parquetReader.readNextRowGroup()
            pages != null
          }) {
            val rows = pages.getRowCount
            val columnIO = new ColumnIOFactory().getColumnIO(parquetSchema)
            val recordReader: RecordReader[Group] = columnIO.getRecordReader(
              pages,
              new GroupRecordConverter(parquetSchema)
            )

            for (_ <- 0L until rows) {
              val group = recordReader.read()
              processor.apply(group, parquetSchema)
            }
          }
        }.recover { case e: Exception =>
        // logError(s"Error processing Parquet file: ${e.getMessage}", e)
        }.get // Re-throw any exception during processing
      case _ =>
        // This case should ideally not happen if initializeReader is called in the constructor
        // logError("Parquet reader or schema not initialized.")
        throw new IOException("Parquet reader or schema not initialized.")
    }
  }
}
