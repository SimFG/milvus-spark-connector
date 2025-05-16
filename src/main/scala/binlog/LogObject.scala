package com.zilliz.spark.connector.binlog

import java.io.{BufferedReader, IOException, InputStream, InputStreamReader}
import java.nio.{ByteBuffer, ByteOrder}
import scala.collection.mutable.ArrayBuffer

import io.milvus.grpc.schema.DataType

object EventTypeCode extends Enumeration {
  type EventTypeCode = Value

  val DescriptorEventType: EventTypeCode = Value(0)
  val InsertEventType: EventTypeCode = Value(1)
  val DeleteEventType: EventTypeCode = Value(2)
  val CreateCollectionEventType: EventTypeCode = Value(3)
  val DropCollectionEventType: EventTypeCode = Value(4)
  val CreatePartitionEventType: EventTypeCode = Value(5)
  val DropPartitionEventType: EventTypeCode = Value(6)
  val IndexFileEventType: EventTypeCode = Value(7)
  val EventTypeEnd: EventTypeCode = Value(8)
}

object Constants {
  // MagicNumber used in binlog
  val MagicNumber: Int = 0xfffabc
  val Endian: ByteOrder = ByteOrder.LITTLE_ENDIAN

  // reader
  val EmptyByteBuffer = ByteBuffer.allocate(0)

  // delete log reader
  val DeleteTimestampColumnName = "ts"
  val DeletePkTypeColumnName = "pkType"
  val DeletePkColumnName = "pk"

  // reader param constant
  val LogPathParamName = "path"
  val LogReaderTypeParamName = "readerType"
  val LogReaderTypeInsert = "insert"
  val LogReaderTypeDelete = "delete"

  // s3 config
  val S3FileSystemTypeName = "s3.fs" // default: s3a://
  val S3Endpoint = "s3.endpoint"
  val S3BucketName = "s3.bucket"
  val S3RootPath = "s3.rootPath"
  val S3AccessKey = "s3.user"
  val S3SecretKey = "s3.password"
  val S3UseSSL = "s3.useSSL"

  def readMagicNumber(buffer: ByteBuffer) = {
    val num = buffer.getInt()
    if (num != MagicNumber) {
      throw new IOException(s"Invalid magic number: $num")
    }
  }
}

object EventHeader {
  // ByteBuffer bufferLittle = ByteBuffer.wrap(dataLittleEndian);
  // bufferLittle.order(ByteOrder.LITTLE_ENDIAN);
  def read(buffer: ByteBuffer): EventHeader = {
    val timestamp = buffer.getLong() // TODO check the timestamp
    val eventType = EventTypeCode.apply(
      buffer.get()
    ) // TODO headerBuffer.get(8) & 0xFF; // Read as unsigned byte
    val eventLength = buffer.getInt()
    val nextPosition = buffer.getInt()
    EventHeader(timestamp, eventType, eventLength, nextPosition)
  }

  def getSize(): Int = {
    java.lang.Long.BYTES + java.lang.Byte.BYTES + java.lang.Integer.BYTES * 2
  }
}

case class EventHeader(
    timestamp: Long,
    eventType: EventTypeCode.EventTypeCode, // int8
    eventLength: Int,
    nextPosition: Int
) {
  override def toString: String = {
    s"EventHeader(timestamp: $timestamp, eventType: $eventType, eventLength: $eventLength, nextPosition: $nextPosition)"
  }
}

object DescriptorEventData {
  def read(
      buffer: ByteBuffer,
      bufferLength: Int
  ): DescriptorEventData = {
    val collectionID = buffer.getLong()
    val partitionID = buffer.getLong()
    val segmentID = buffer.getLong()
    val fieldID = buffer.getLong()
    val startTimestamp = buffer.getLong()
    val endTimestamp = buffer.getLong()
    val payloadDataType = DataType.fromValue(buffer.get())
    val postHeaderLengths = (0 until EventTypeCode.EventTypeEnd.id)
      .map(_ => buffer.get().toShort)
      .toArray
    val extraLength = buffer.getInt()
    val extraBytes =
      new Array[Byte](bufferLength - DescriptorEventData.getSize())
    buffer.get(extraBytes)
    DescriptorEventData(
      collectionID,
      partitionID,
      segmentID,
      fieldID,
      startTimestamp,
      endTimestamp,
      payloadDataType,
      postHeaderLengths,
      extraLength,
      extraBytes
    )
  }

  def getSize(): Int = {
    java.lang.Long.BYTES * 6 + java.lang.Integer.BYTES * 2 + java.lang.Short.BYTES * EventTypeCode.EventTypeEnd.id
  }
}

case class DescriptorEventData(
    collectionID: Long,
    partitionID: Long,
    segmentID: Long,
    fieldID: Long,
    startTimestamp: Long,
    endTimestamp: Long,
    payloadDataType: DataType,
    postHeaderLengths: Array[Short], // int8
    extraLength: Int, // int32
    extraData: Array[Byte]
) {
  override def toString: String = {
    s"DescriptorEventData(collectionID: $collectionID, partitionID: $partitionID, segmentID: $segmentID, fieldID: $fieldID, startTimestamp: $startTimestamp, endTimestamp: $endTimestamp, payloadDataType: $payloadDataType, postHeaderLengths: $postHeaderLengths, extraLength: $extraLength, extraData: $extraData)"
  }
}

case class DescriptorEvent(
    header: EventHeader,
    data: DescriptorEventData
) {
  override def toString: String = {
    s"DescriptorEvent(header: $header\n data: $data)"
  }
}

object BaseEventData {
  def read(buffer: ByteBuffer): BaseEventData = {
    val startTimestamp = buffer.getLong()
    val endTimestamp = buffer.getLong()
    BaseEventData(startTimestamp, endTimestamp)
  }

  def getSize(): Int = {
    java.lang.Long.BYTES * 2
  }
}

case class BaseEventData(
    startTimestamp: Long,
    endTimestamp: Long
)

class DeleteEventData(
    val baseEventData: BaseEventData,
    var pks: ArrayBuffer[String],
    var timestamps: ArrayBuffer[Long],
    var pkType: DataType
) {

  override def toString: String = {
    s"DeleteEventData(baseEventData: $baseEventData, pks: $pks, timestamps: $timestamps, pkType: $pkType)"
  }
}

class InsertEventData(
    val baseEventData: BaseEventData,
    var datas: ArrayBuffer[String],
    var timestamp: Long,
    var dataType: DataType
) {
  override def toString: String = {
    s"InsertEventData(baseEventData: $baseEventData, datas: $datas, timestamp: $timestamp, dataType: $dataType)"
  }
}
