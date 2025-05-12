package com.zilliz.spark.connector.binlog

import java.io.{BufferedReader, IOException, InputStream, InputStreamReader}
import java.nio.{ByteBuffer, ByteOrder}
import scala.collection.mutable.ArrayBuffer
import scala.util.Using

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import io.milvus.grpc.schema.DataType

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
