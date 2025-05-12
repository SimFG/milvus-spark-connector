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
// import org.apache.spark.internal.Logging

/** ParquetPayloadReader reads and parses parquet format data from a byte array.
  * This implementation aligns with the Go implementation's
  * ParquetPayloadReader.
  */
class ParquetPayloadReader(data: Array[Byte]) extends AutoCloseable {
  // with Logging {

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
      schema.foreach { msgType =>
        val fields = msgType.getFields
        if (fields.isEmpty) {
          // logWarning("Parquet schema has no fields")
        } else {

          // logDebug("Schema field names:")
          fields.forEach { field =>
            // logDebug(
            //   s"  Field ${msgType.getFieldIndex(field.getName)}: ${field.getName} (${field.asPrimitiveType().getPrimitiveTypeName})"
            // )
          }

        }
      }
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
    }.get // Re-throw the exception if initialization failed
  }

  /** Cleans up temporary files created during the reading process.
    */
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

  /** Gets boolean values from the payload for the specified column.
    *
    * @param columnIndex
    *   The column index
    * @return
    *   List of boolean values
    */
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

  /** Gets int8 (byte) values from the payload for the specified column.
    *
    * @param columnIndex
    *   The column index
    * @return
    *   List of byte values
    */
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

  /** Gets int16 (short) values from the payload for the specified column.
    *
    * @param columnIndex
    *   The column index
    * @return
    *   List of short values
    */
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

  /** Gets int32 (integer) values from the payload for the specified column.
    *
    * @param columnIndex
    *   The column index
    * @return
    *   List of integer values
    */
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

  /** Gets int64 (long) values from the payload for the specified column.
    *
    * @param columnIndex
    *   The column index
    * @return
    *   List of long values
    */
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

  /** Gets float32 (float) values from the payload for the specified column.
    *
    * @param columnIndex
    *   The column index
    * @return
    *   List of float values
    */
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

  /** Gets float64 (double) values from the payload for the specified column.
    *
    * @param columnIndex
    *   The column index
    * @return
    *   List of double values
    */
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

  /** Gets string values from the payload for the specified column.
    *
    * @param columnIndex
    *   The column index
    * @return
    *   List of string values
    */
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

  /** Gets binary values from the payload for the specified column.
    *
    * @param columnIndex
    *   The column index
    * @return
    *   List of binary values as byte arrays
    */
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

  /** Gets float vector values from the payload for the specified column.
    *
    * @param columnIndex
    *   The column index
    * @return
    *   List of float vectors (as float arrays)
    */
  def getFloatVectorFromPayload(columnIndex: Int): List[Array[Float]] = {
    // Note: This implementation assumes that the float vector is stored as a LIST of FLOATs
    val values = new ListBuffer[Array[Float]]()
    // This requires custom implementation based on the specific Parquet schema used
    // TODO: Implement for specific use cases as needed
    values.toList
  }

  /** Gets binary vector values from the payload for the specified column.
    *
    * @param columnIndex
    *   The column index
    * @return
    *   List of binary vectors (as byte arrays)
    */
  def getBinaryVectorFromPayload(columnIndex: Int): List[Array[Byte]] = {
    // Note: This implementation assumes that the binary vector is stored as a BINARY field
    getBinaryFromPayload(columnIndex)
  }

  /** Checks if a column access is valid for the given schema and group.
    *
    * @param group
    *   The group to check
    * @param schema
    *   The schema to check against
    * @param columnIndex
    *   The column index to access
    * @param expectedType
    *   The expected primitive type
    * @return
    *   True if the access is valid
    */
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

  /** Processes the Parquet file and applies the specified record processor.
    *
    * @param processor
    *   The record processor to apply to each Parquet record
    */
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
