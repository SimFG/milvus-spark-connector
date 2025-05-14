package com.zilliz.spark.connector.binlog

import java.io.{BufferedReader, IOException, InputStream, InputStreamReader}
import java.nio.{ByteBuffer, ByteOrder}
import scala.collection.mutable.ArrayBuffer
import scala.util.Using

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import io.milvus.grpc.schema.DataType
import io.milvus.grpc.schema.DataType.FloatVector

object LogReader {

  def getByteBuffer(is: InputStream, size: Int): ByteBuffer = {
    val buffer = ByteBuffer.allocate(size)
    buffer.order(ByteOrder.LITTLE_ENDIAN)
    val len = is.read(buffer.array())
    if (len <= 0) {
      return Constants.EmptyByteBuffer
    }
    buffer
  }

  def getObjectMapper(): ObjectMapper = {
    val objectMapper = new ObjectMapper()
    objectMapper.registerModule(DefaultScalaModule)
    objectMapper
  }

  def readDescriptorEvent(input: InputStream): DescriptorEvent = {
    val buffer = getByteBuffer(input, 4)
    Constants.readMagicNumber(buffer)

    val eventHeaderBuffer = getByteBuffer(input, EventHeader.getSize())
    val eventHeader = EventHeader.read(eventHeaderBuffer)

    val descriptorEventDataBuffer =
      getByteBuffer(input, eventHeader.eventLength - EventHeader.getSize())
    val descriptorEventData = DescriptorEventData.read(
      descriptorEventDataBuffer,
      eventHeader.eventLength - EventHeader.getSize()
    )

    val descriptorEvent = DescriptorEvent(eventHeader, descriptorEventData)
    return descriptorEvent
  }

  def readDeleteEvent(
      input: InputStream,
      objectMapper: ObjectMapper,
      dataType: DataType
  ): DeleteEventData = {
    val eventHeaderBuffer = getByteBuffer(input, EventHeader.getSize())
    if (eventHeaderBuffer == Constants.EmptyByteBuffer) {
      return null
    }
    val eventHeader = EventHeader.read(eventHeaderBuffer)
    if (eventHeader.eventType != EventTypeCode.DeleteEventType) {
      throw new IOException(
        s"Expected delete event, but got ${eventHeader.eventType}"
      )
    }
    val baseEventDataBuffer =
      getByteBuffer(input, BaseEventData.getSize())
    val baseEventData = BaseEventData.read(baseEventDataBuffer)

    val deleteData = new DeleteEventData(
      baseEventData,
      ArrayBuffer.empty[String],
      ArrayBuffer.empty[Long],
      DataType.None
    )
    val eventDataSize =
      eventHeader.eventLength - EventHeader.getSize() - BaseEventData
        .getSize()
    val eventDataBuffer = getByteBuffer(input, eventDataSize)
    val parquetPayloadReader =
      new ParquetPayloadReader(eventDataBuffer.array())
    dataType match {
      case DataType.String => {
        val deleteDataStrings = parquetPayloadReader
          .getStringFromPayload(0)
          .map(_.toString)
        deleteDataStrings.foreach(deleteDataString => {
          val root = objectMapper.readValue(
            deleteDataString,
            classOf[Map[String, Any]]
          )
          val pkTypeV = root.get(Constants.DeletePkTypeColumnName)
          val pkV = root.get(Constants.DeletePkColumnName)
          val timestampsV =
            root.get(Constants.DeleteTimestampColumnName)
          pkTypeV match {
            case Some(v: java.lang.Integer) => {
              deleteData.pkType = DataType.fromValue(v)
            }
            case _ => {
              deleteData.pkType = DataType.None
            }
          }
          pkV match {
            case Some(v: java.lang.Long) => {
              deleteData.pks += v.toString
            }
            case Some(v: java.lang.String) => {
              deleteData.pks += v
            }
            case _ => {
              deleteData.pks += "0"
            }
          }
          timestampsV match {
            case Some(v: java.lang.Long) => {
              deleteData.timestamps += v
            }
            case _ => {
              deleteData.timestamps += 0L
            }
          }
        })
      }
      case _ => {
        throw new IOException(
          s"Unsupported data type: ${dataType}, expected String for delete event"
        )
      }
    }
    deleteData
  }

  def readInsertEvent(
      input: InputStream,
      objectMapper: ObjectMapper,
      dataType: DataType
  ): InsertEventData = {
    val eventHeaderBuffer = getByteBuffer(input, EventHeader.getSize())
    if (eventHeaderBuffer == Constants.EmptyByteBuffer) {
      return null
    }
    val eventHeader = EventHeader.read(eventHeaderBuffer)
    if (eventHeader.eventType != EventTypeCode.InsertEventType) {
      throw new IOException(
        s"Expected insert event, but got ${eventHeader.eventType}"
      )
    }
    val baseEventDataBuffer =
      getByteBuffer(input, BaseEventData.getSize())
    val baseEventData = BaseEventData.read(baseEventDataBuffer)

    val insertData = new InsertEventData(
      baseEventData,
      ArrayBuffer.empty[String],
      eventHeader.timestamp,
      dataType
    )
    val eventDataSize =
      eventHeader.eventLength - EventHeader.getSize() - BaseEventData
        .getSize()
    val eventDataBuffer = getByteBuffer(input, eventDataSize)
    val parquetPayloadReader =
      new ParquetPayloadReader(eventDataBuffer.array())
    dataType match {
      case DataType.String | DataType.VarChar => {
        val dataStrings = parquetPayloadReader
          .getStringFromPayload(0)
          .map(_.toString)
        insertData.datas ++= dataStrings
      }
      case DataType.Bool => {
        val dataBooleans = parquetPayloadReader
          .getBooleanFromPayload(0)
          .map(_.toString)
        insertData.datas ++= dataBooleans
      }
      case DataType.Int8 => {
        val dataInt8s = parquetPayloadReader
          .getInt8FromPayload(0)
          .map(_.toString)
        insertData.datas ++= dataInt8s
      }
      case DataType.Int16 => {
        val dataInt16s = parquetPayloadReader
          .getInt16FromPayload(0)
          .map(_.toString)
        insertData.datas ++= dataInt16s
      }
      case DataType.Int32 => {
        val dataInt32s = parquetPayloadReader
          .getInt32FromPayload(0)
          .map(_.toString)
        insertData.datas ++= dataInt32s
      }
      case DataType.Int64 => {
        val dataInt64s = parquetPayloadReader
          .getInt64FromPayload(0)
          .map(_.toString)
        insertData.datas ++= dataInt64s
      }
      case DataType.Float => {
        val dataFloats = parquetPayloadReader
          .getFloat32FromPayload(0)
          .map(_.toString)
        insertData.datas ++= dataFloats
      }
      case DataType.Double => {
        val dataDoubles = parquetPayloadReader
          .getFloat64FromPayload(0)
          .map(_.toString)
        insertData.datas ++= dataDoubles
      }
      case FloatVector => {
        val dataFloatVectors = parquetPayloadReader
          .getFloatVectorFromPayload(0)
          .map(vector => {
            vector.map(_.toString).mkString(",")
          })
        insertData.datas ++= dataFloatVectors
      }
      // TODO: vector type
      case _ => {
        throw new IOException(
          s"Unsupported data type: ${dataType}, for insert event"
        )
      }
    }
    insertData
  }

  def read(is: InputStream) = {
    Using(is) { input =>
      val objectMapper = new ObjectMapper()
      objectMapper.registerModule(DefaultScalaModule)

      val buffer = getByteBuffer(input, 4)
      Constants.readMagicNumber(buffer)

      val eventHeaderBuffer = getByteBuffer(input, EventHeader.getSize())
      val eventHeader = EventHeader.read(eventHeaderBuffer)

      val descriptorEventDataBuffer =
        getByteBuffer(input, eventHeader.eventLength - EventHeader.getSize())
      val descriptorEventData = DescriptorEventData.read(
        descriptorEventDataBuffer,
        eventHeader.eventLength - EventHeader.getSize()
      )

      val descriptorEvent = DescriptorEvent(eventHeader, descriptorEventData)
      println(descriptorEvent) // TODO: return the event
      val dataType = descriptorEvent.data.payloadDataType

      var isEOF = false
      while (!isEOF) {
        val eventHeaderBuffer = getByteBuffer(input, EventHeader.getSize())
        if (eventHeaderBuffer == Constants.EmptyByteBuffer) {
          isEOF = true
        } else {
          val eventHeader = EventHeader.read(eventHeaderBuffer)
          println(s"eventHeader: $eventHeader")
          eventHeader.eventType match {
            case EventTypeCode.DeleteEventType => {
              val baseEventDataBuffer =
                getByteBuffer(input, BaseEventData.getSize())
              val baseEventData = BaseEventData.read(baseEventDataBuffer)
              println(baseEventData) // TODO: return the event

              val deleteData = new DeleteEventData(
                baseEventData,
                ArrayBuffer.empty[String],
                ArrayBuffer.empty[Long],
                DataType.None
              )
              val eventDataSize =
                eventHeader.eventLength - EventHeader.getSize() - BaseEventData
                  .getSize()
              val eventDataBuffer = getByteBuffer(input, eventDataSize)
              val parquetPayloadReader =
                new ParquetPayloadReader(eventDataBuffer.array())
              dataType match {
                case DataType.String => {
                  val deleteDataStrings = parquetPayloadReader
                    .getStringFromPayload(0)
                    .map(_.toString)
                  println(s"deleteDataStrings: $deleteDataStrings")
                  deleteDataStrings.foreach(deleteDataString => {
                    // println(s"deleteDataString: $deleteDataString")
                    val root = objectMapper.readValue(
                      deleteDataString,
                      classOf[Map[String, Any]]
                    )
                    val pkTypeV = root.get(Constants.DeletePkTypeColumnName)
                    val pkV = root.get(Constants.DeletePkColumnName)
                    val timestampsV =
                      root.get(Constants.DeleteTimestampColumnName)
                    pkTypeV match {
                      case Some(v: java.lang.Integer) => {
                        deleteData.pkType = DataType.fromValue(v)
                      }
                      case _ => {
                        deleteData.pkType = DataType.None
                      }
                    }
                    pkV match {
                      case Some(v: java.lang.Long) => {
                        deleteData.pks += v.toString
                      }
                      case Some(v: java.lang.String) => {
                        deleteData.pks += v
                      }
                      case _ => {
                        deleteData.pks += "0"
                      }
                    }
                    timestampsV match {
                      case Some(v: java.lang.Long) => {
                        deleteData.timestamps += v
                      }
                      case _ => {
                        deleteData.timestamps += 0L
                      }
                    }
                  })
                }
                case _ => {
                  throw new IOException(
                    s"Unsupported data type: ${dataType}, for delete event"
                  )
                }
              }
              println(s"deleteData: $deleteData") // TODO: return the event
            }
            case _ => {
              throw new IOException(
                s"Unknown event type: ${eventHeader.eventType}"
              )
            }
          }
        }
      }
    }
  }
}
