package com.zilliz.spark.connector

import java.nio.{ByteBuffer, ByteOrder}

import com.google.protobuf.ByteString

import com.zilliz.spark.connector.DataParseException
import io.milvus.grpc.schema.{
  ArrayArray,
  BoolArray,
  BytesArray,
  DataType,
  DoubleArray,
  FieldData,
  FieldSchema,
  FloatArray,
  GeometryArray,
  IntArray,
  JSONArray,
  LongArray,
  ScalarField,
  SparseFloatArray,
  StringArray,
  VectorField
}

object MilvusFieldData {
  // TODO: fubang null value support
  // TODO: fubang support dynamic field
  def packBoolFieldData(
      fieldName: String,
      fieldValues: Seq[Boolean]
  ): FieldData = {
    FieldData(
      `type` = DataType.Bool,
      fieldName = fieldName,
      field = FieldData.Field.Scalars(
        ScalarField(data =
          ScalarField.Data.BoolData(BoolArray(data = fieldValues))
        )
      )
    )
  }

  def packInt8FieldData(
      fieldName: String,
      fieldValues: Seq[Short]
  ): FieldData = {
    FieldData(
      `type` = DataType.Int8,
      fieldName = fieldName,
      field = FieldData.Field.Scalars(
        ScalarField(data =
          ScalarField.Data.IntData(IntArray(data = fieldValues.map(_.toInt)))
        )
      )
    )
  }

  def packInt16FieldData(
      fieldName: String,
      fieldValues: Seq[Short]
  ): FieldData = {
    FieldData(
      `type` = DataType.Int16,
      fieldName = fieldName,
      field = FieldData.Field.Scalars(
        ScalarField(data =
          ScalarField.Data.IntData(IntArray(data = fieldValues.map(_.toInt)))
        )
      )
    )
  }

  def packInt32FieldData(
      fieldName: String,
      fieldValues: Seq[Int]
  ): FieldData = {
    FieldData(
      `type` = DataType.Int32,
      fieldName = fieldName,
      field = FieldData.Field.Scalars(
        ScalarField(
          data = ScalarField.Data.IntData(
            IntArray(data = fieldValues)
          )
        )
      )
    )
  }

  def packInt64FieldData(
      fieldName: String,
      fieldValues: Seq[Long]
  ): FieldData = {
    FieldData(
      `type` = DataType.Int64,
      fieldName = fieldName,
      field = FieldData.Field.Scalars(
        ScalarField(data =
          ScalarField.Data.LongData(LongArray(data = fieldValues))
        )
      )
    )
  }

  def packFloatFieldData(
      fieldName: String,
      fieldValues: Seq[Float]
  ): FieldData = {
    FieldData(
      `type` = DataType.Float,
      fieldName = fieldName,
      field = FieldData.Field.Scalars(
        ScalarField(data =
          ScalarField.Data.FloatData(FloatArray(data = fieldValues))
        )
      )
    )
  }

  def packDoubleFieldData(
      fieldName: String,
      fieldValues: Seq[Double]
  ): FieldData = {
    FieldData(
      `type` = DataType.Double,
      fieldName = fieldName,
      field = FieldData.Field.Scalars(
        ScalarField(data =
          ScalarField.Data.DoubleData(DoubleArray(data = fieldValues))
        )
      )
    )
  }

  def packStringFieldData(
      fieldName: String,
      fieldValues: Seq[String]
  ): FieldData = {
    FieldData(
      `type` = DataType.VarChar,
      fieldName = fieldName,
      field = FieldData.Field.Scalars(
        ScalarField(data =
          ScalarField.Data.StringData(StringArray(data = fieldValues))
        )
      )
    )
  }

  def packArrayFieldData(
      fieldName: String,
      fieldValues: Seq[ScalarField],
      elementType: DataType
  ): FieldData = {
    FieldData(
      `type` = DataType.Array,
      fieldName = fieldName,
      field = FieldData.Field.Scalars(
        ScalarField(data =
          ScalarField.Data.ArrayData(
            ArrayArray(
              data = fieldValues,
              elementType = elementType
            )
          )
        )
      )
    )
  }

  def packJsonFieldData(
      fieldName: String,
      fieldValues: Seq[String]
  ): FieldData = {
    FieldData(
      `type` = DataType.JSON,
      fieldName = fieldName,
      field = FieldData.Field.Scalars(
        ScalarField(data =
          ScalarField.Data.JsonData(
            JSONArray(data = fieldValues.map(ByteString.copyFromUtf8))
          )
        )
      )
    )
  }

  def packGeometryFieldData(
      fieldName: String,
      fieldValues: Seq[Seq[Byte]]
  ): FieldData = {
    FieldData(
      `type` = DataType.Geometry,
      fieldName = fieldName,
      field = FieldData.Field.Scalars(
        ScalarField(data =
          ScalarField.Data.GeometryData(
            GeometryArray(
              data =
                fieldValues.map(bytes => ByteString.copyFrom(bytes.toArray))
            )
          )
        )
      )
    )
  }

  def packFloatVectorFieldData(
      fieldName: String,
      fieldValues: Seq[Seq[Float]],
      dim: Int
  ): FieldData = {
    val allValues = fieldValues.flatten.toArray
    FieldData(
      `type` = DataType.FloatVector,
      fieldName = fieldName,
      field = FieldData.Field.Vectors(
        VectorField(
          dim = dim,
          data = VectorField.Data.FloatVector(
            value = FloatArray(data = allValues)
          )
        )
      )
    )
  }

  def packBinaryVectorFieldData(
      fieldName: String,
      fieldValues: Seq[Seq[Byte]],
      dim: Int
  ): FieldData = {
    val allValues = ByteString.copyFrom(fieldValues.flatten.toArray)
    FieldData(
      `type` = DataType.BinaryVector,
      fieldName = fieldName,
      field = FieldData.Field.Vectors(
        VectorField(
          dim = dim,
          data = VectorField.Data.BinaryVector(allValues)
        )
      )
    )
  }

  def packInt8VectorFieldData(
      fieldName: String,
      fieldValues: Seq[Seq[Short]],
      dim: Int
  ): FieldData = {
    val allValues = fieldValues.flatten.toArray.map(_.toByte)
    FieldData(
      `type` = DataType.Int8Vector,
      fieldName = fieldName,
      field = FieldData.Field.Vectors(
        VectorField(
          dim = dim,
          data = VectorField.Data.Int8Vector(
            value = ByteString.copyFrom(allValues)
          )
        )
      )
    )
  }

  def packFloat16VectorFieldData(
      fieldName: String,
      fieldValues: Seq[Seq[Float]],
      dim: Int
  ): FieldData = {
    val allValues =
      fieldValues.flatten.map(FloatConverter.toFloat16Bytes).flatten
    FieldData(
      `type` = DataType.Float16Vector,
      fieldName = fieldName,
      field = FieldData.Field.Vectors(
        VectorField(
          dim = dim,
          data = VectorField.Data.Float16Vector(
            value = ByteString.copyFrom(allValues.toArray)
          )
        )
      )
    )
  }

  def packBFloat16VectorFieldData(
      fieldName: String,
      fieldValues: Seq[Seq[Float]],
      dim: Int
  ): FieldData = {
    val allValues =
      fieldValues.flatten.map(FloatConverter.toBFloat16Bytes).flatten
    FieldData(
      `type` = DataType.BFloat16Vector,
      fieldName = fieldName,
      field = FieldData.Field.Vectors(
        VectorField(
          dim = dim,
          data = VectorField.Data.Bfloat16Vector(
            value = ByteString.copyFrom(allValues.toArray)
          )
        )
      )
    )
  }

  def packSparseFloatVectorFieldData(
      fieldName: String,
      fieldValues: Seq[Map[Long, Float]],
      dim: Int
  ): FieldData = {
    val sparseDim = fieldValues.map(_.size).max
    val allValues = fieldValues
      .map(SparseFloatVectorConverter.encodeSparseFloatVector)
      .toArray
    FieldData(
      `type` = DataType.SparseFloatVector,
      fieldName = fieldName,
      field = FieldData.Field.Vectors(
        VectorField(
          dim = dim,
          data = VectorField.Data.SparseFloatVector(
            SparseFloatArray(
              contents = allValues.map(ByteString.copyFrom),
              dim = sparseDim
            )
          )
        )
      )
    )
  }
}

object FloatConverter {
  private def isWithinFloat16Range(value: Float): Boolean = {
    val absValue = Math.abs(value)
    // float16 range: ±65504
    absValue <= 65504f && !value.isNaN && !value.isInfinite
  }

  private def isWithinBFloat16Range(value: Float): Boolean = {
    // bfloat16 has 8 exponent bits, same as float32
    // so it can represent the same range as float32
    // but with less precision
    !value.isNaN && !value.isInfinite
  }

  def toFloat16Bytes(value: Float): Seq[Byte] = {
    if (!isWithinFloat16Range(value)) {
      throw new DataParseException(
        s"Value $value is out of float16 range"
      )
    }

    // IEEE 754 float32 format: 1 sign bit, 8 exponent bits, 23 fraction bits
    val bits = java.lang.Float.floatToRawIntBits(value)
    val sign = (bits >>> 31) & 0x1
    val exp = (bits >>> 23) & 0xff
    val frac = bits & 0x7fffff

    // IEEE 754 float16 format: 1 sign bit, 5 exponent bits, 10 fraction bits
    val f16Sign = sign
    val f16Exp = if (exp == 0) {
      0 // Zero/Subnormal
    } else if (exp == 0xff) {
      0x1f // Infinity/NaN
    } else {
      val newExp = exp - 127 + 15
      if (newExp < 0) 0
      else if (newExp > 0x1f) 0x1f
      else newExp
    }
    val f16Frac = frac >>> 13

    // Combine components into a half-precision (float16) value
    val f16Bits = (f16Sign << 15) | (f16Exp << 10) | f16Frac

    // Create byte array in big-endian format (high byte first)
    val bytes = new Array[Byte](2)
    bytes(0) = ((f16Bits >>> 8) & 0xff).toByte
    bytes(1) = (f16Bits & 0xff).toByte
    bytes.toSeq
  }

  def toBFloat16Bytes(value: Float): Seq[Byte] = {
    if (!isWithinBFloat16Range(value)) {
      throw new DataParseException(
        s"Value $value is out of bfloat16 range"
      )
    }

    // Get raw 32-bit representation
    val intValue = java.lang.Float.floatToRawIntBits(value)

    // bfloat16 preserves the sign bit, all 8 exponent bits, and the top 7 bits of the fraction
    // Create byte array in big-endian format (high byte first)
    val bytes = new Array[Byte](2)
    bytes(0) = ((intValue >>> 24) & 0xff).toByte
    bytes(1) = ((intValue >>> 16) & 0xff).toByte
    bytes.toSeq
  }
}

object SparseFloatVectorConverter {
  def encodeSparseFloatVector(sparse: Map[Long, Float]): Array[Byte] = {
    // 对 Map 的键进行排序（按升序排列）
    val sortedEntries = sparse.toSeq.sortBy(_._1)

    // 分配 ByteBuffer，大小为 (Int + Float) * 元素数量
    val buf = ByteBuffer.allocate((4 + 4) * sortedEntries.size)
    buf.order(ByteOrder.LITTLE_ENDIAN) // 设置为小端模式

    // 遍历排序后的键值对
    for ((k, v) <- sortedEntries) {
      if (k < 0 || k >= Math.pow(2.0, 32) - 1) {
        throw new DataParseException(
          s"Sparse vector index ($k) must be positive and less than 2^32-1"
        )
      }

      val lBuf = ByteBuffer.allocate(8) // Long 占 8 字节
      lBuf.order(ByteOrder.LITTLE_ENDIAN)
      lBuf.putLong(k)
      buf.put(lBuf.array(), 0, 4) // 只取前 4 字节

      if (v.isNaN || v.isInfinite) {
        throw new DataParseException(
          s"Sparse vector value ($v) cannot be NaN or Infinite"
        )
      }

      buf.putFloat(v)
    }

    buf.array()
  }
}

object MilvusSchemaUtil {
  def getDim(fieldSchema: FieldSchema): Int = {
    for (param <- fieldSchema.typeParams) {
      if (param.key == "dim") {
        return param.value.toInt
      }
    }
    throw new DataParseException(
      s"Field ${fieldSchema.name} has no dim parameter"
    )
  }
}
