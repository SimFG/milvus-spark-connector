package com.zilliz.spark.connector.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * Base trait for binary vector expressions
 */
abstract class BinaryVectorExpression(left: Expression, right: Expression)
  extends BinaryExpression with CodegenFallback {
  
  override def dataType: DataType = DoubleType
  override def nullable: Boolean = true
  
  protected def validateVectorTypes(leftType: DataType, rightType: DataType): Boolean = {
    (leftType, rightType) match {
      case (ArrayType(FloatType, _), ArrayType(FloatType, _)) => true
      case (ArrayType(DoubleType, _), ArrayType(DoubleType, _)) => true
      case (ArrayType(IntegerType, _), ArrayType(IntegerType, _)) => true
      case (ArrayType(LongType, _), ArrayType(LongType, _)) => true
      case (ArrayType(DecimalType(), _), ArrayType(DecimalType(), _)) => true
      case (ArrayType(FloatType, _), ArrayType(DoubleType, _)) => true
      case (ArrayType(DoubleType, _), ArrayType(FloatType, _)) => true
      case (ArrayType(_, _), ArrayType(_, _)) => true // Allow mixed numeric types
      case _ => false
    }
  }
  
  protected def extractFloatArray(value: Any): Array[Float] = {
    value match {
      case arrayData: ArrayData =>
        val size = arrayData.numElements()
        val result = new Array[Float](size)
        var i = 0
        while (i < size) {
          // Use null as elementType to let Spark handle the type inference
          val element = if (i < size) arrayData.get(i, null) else null
          result(i) = convertToFloat(element)
          i += 1
        }
        result
      case _ => null
    }
  }
  
  protected def extractDoubleArray(value: Any): Array[Double] = {
    value match {
      case arrayData: ArrayData =>
        val size = arrayData.numElements()
        val result = new Array[Double](size)
        var i = 0
        while (i < size) {
          // Use null as elementType to let Spark handle the type inference
          val element = if (i < size) arrayData.get(i, null) else null
          result(i) = convertToDouble(element)
          i += 1
        }
        result
      case _ => null
    }
  }
  
  protected def convertToFloat(value: Any): Float = {
    value match {
      case f: Float => f
      case d: Double => d.toFloat
      case i: Int => i.toFloat
      case l: Long => l.toFloat
      case decimal: org.apache.spark.sql.types.Decimal => decimal.toDouble.toFloat
      case null => 0.0f
      case _ => throw new IllegalArgumentException(s"Cannot convert ${value.getClass} to Float: $value")
    }
  }
  
  protected def convertToDouble(value: Any): Double = {
    value match {
      case f: Float => f.toDouble
      case d: Double => d
      case i: Int => i.toDouble
      case l: Long => l.toDouble
      case decimal: org.apache.spark.sql.types.Decimal => decimal.toDouble
      case null => 0.0
      case _ => throw new IllegalArgumentException(s"Cannot convert ${value.getClass} to Double: $value")
    }
  }
}

/**
 * Cosine Similarity Expression
 */
case class CosineSimilarityExpression(left: Expression, right: Expression)
  extends BinaryVectorExpression(left, right) {
  
  override protected def withNewChildrenInternal(
    newLeft: Expression, 
    newRight: Expression
  ): Expression = copy(left = newLeft, right = newRight)
  
  override def nullSafeEval(leftValue: Any, rightValue: Any): Any = {
    val leftArray = extractFloatArray(leftValue)
    val rightArray = extractFloatArray(rightValue)
    
    if (leftArray == null || rightArray == null || leftArray.length != rightArray.length) {
      null
    } else {
      computeCosineSimilarity(leftArray, rightArray)
    }
  }
  
  private def computeCosineSimilarity(v1: Array[Float], v2: Array[Float]): Double = {
    if (v1.length != v2.length) return 0.0
    
    var dotProduct = 0.0
    var normV1 = 0.0
    var normV2 = 0.0
    
    var i = 0
    while (i < v1.length) {
      dotProduct += v1(i) * v2(i)
      normV1 += v1(i) * v1(i)
      normV2 += v2(i) * v2(i)
      i += 1
    }
    
    val normProduct = math.sqrt(normV1) * math.sqrt(normV2)
    if (normProduct == 0.0) 0.0 else dotProduct / normProduct
  }
  
  override def prettyName: String = "cosine_similarity"
}

/**
 * L2 Distance Expression
 */
case class L2DistanceExpression(left: Expression, right: Expression)
  extends BinaryVectorExpression(left, right) {
  
  override protected def withNewChildrenInternal(
    newLeft: Expression, 
    newRight: Expression
  ): Expression = copy(left = newLeft, right = newRight)
  
  override def nullSafeEval(leftValue: Any, rightValue: Any): Any = {
    val leftArray = extractFloatArray(leftValue)
    val rightArray = extractFloatArray(rightValue)
    
    if (leftArray == null || rightArray == null || leftArray.length != rightArray.length) {
      null
    } else {
      computeL2Distance(leftArray, rightArray)
    }
  }
  
  private def computeL2Distance(v1: Array[Float], v2: Array[Float]): Double = {
    if (v1.length != v2.length) return Double.MaxValue
    
    var sum = 0.0
    var i = 0
    while (i < v1.length) {
      val diff = v1(i) - v2(i)
      sum += diff * diff
      i += 1
    }
    
    math.sqrt(sum)
  }
  
  override def prettyName: String = "l2_distance"
}

/**
 * Inner Product Expression
 */
case class InnerProductExpression(left: Expression, right: Expression)
  extends BinaryVectorExpression(left, right) {
  
  override protected def withNewChildrenInternal(
    newLeft: Expression, 
    newRight: Expression
  ): Expression = copy(left = newLeft, right = newRight)
  
  override def nullSafeEval(leftValue: Any, rightValue: Any): Any = {
    val leftArray = extractFloatArray(leftValue)
    val rightArray = extractFloatArray(rightValue)
    
    if (leftArray == null || rightArray == null || leftArray.length != rightArray.length) {
      null
    } else {
      computeInnerProduct(leftArray, rightArray)
    }
  }
  
  private def computeInnerProduct(v1: Array[Float], v2: Array[Float]): Double = {
    if (v1.length != v2.length) return 0.0
    
    var sum = 0.0
    var i = 0
    while (i < v1.length) {
      sum += v1(i) * v2(i)
      i += 1
    }
    
    sum
  }
  
  override def prettyName: String = "inner_product"
}

/**
 * Hamming Distance Expression for binary vectors
 */
case class HammingDistanceExpression(left: Expression, right: Expression)
  extends BinaryVectorExpression(left, right) {
  
  override protected def withNewChildrenInternal(
    newLeft: Expression, 
    newRight: Expression
  ): Expression = copy(left = newLeft, right = newRight)
  
  override def nullSafeEval(leftValue: Any, rightValue: Any): Any = {
    val leftArray = extractByteArray(leftValue)
    val rightArray = extractByteArray(rightValue)
    
    if (leftArray == null || rightArray == null || leftArray.length != rightArray.length) {
      null
    } else {
      computeHammingDistance(leftArray, rightArray)
    }
  }
  
  private def extractByteArray(value: Any): Array[Byte] = {
    value match {
      case arrayData: ArrayData =>
        val size = arrayData.numElements()
        val result = new Array[Byte](size)
        var i = 0
        while (i < size) {
          val element = arrayData.get(i, null)
          result(i) = convertToByte(element)
          i += 1
        }
        result
      case _ => null
    }
  }
  
  private def convertToByte(value: Any): Byte = {
    value match {
      case b: Byte => b
      case i: Int => i.toByte
      case l: Long => l.toByte
      case f: Float => f.toByte
      case d: Double => d.toByte
      case decimal: org.apache.spark.sql.types.Decimal => decimal.toInt.toByte
      case null => 0.toByte
      case _ => throw new IllegalArgumentException(s"Cannot convert ${value.getClass} to Byte: $value")
    }
  }
  
  private def computeHammingDistance(v1: Array[Byte], v2: Array[Byte]): Double = {
    if (v1.length != v2.length) return Double.MaxValue
    
    var distance = 0
    var i = 0
    while (i < v1.length) {
      val xor = v1(i) ^ v2(i)
      distance += java.lang.Integer.bitCount(xor & 0xff)
      i += 1
    }
    
    distance.toDouble
  }
  
  override def prettyName: String = "hamming_distance"
}

/**
 * Jaccard Distance Expression for binary vectors
 */
case class JaccardDistanceExpression(left: Expression, right: Expression)
  extends BinaryVectorExpression(left, right) {
  
  override protected def withNewChildrenInternal(
    newLeft: Expression, 
    newRight: Expression
  ): Expression = copy(left = newLeft, right = newRight)
  
  override def nullSafeEval(leftValue: Any, rightValue: Any): Any = {
    val leftArray = extractByteArray(leftValue)
    val rightArray = extractByteArray(rightValue)
    
    if (leftArray == null || rightArray == null || leftArray.length != rightArray.length) {
      null
    } else {
      computeJaccardDistance(leftArray, rightArray)
    }
  }
  
  private def extractByteArray(value: Any): Array[Byte] = {
    value match {
      case arrayData: ArrayData =>
        val size = arrayData.numElements()
        val result = new Array[Byte](size)
        var i = 0
        while (i < size) {
          val element = arrayData.get(i, null)
          result(i) = convertToByte(element)
          i += 1
        }
        result
      case _ => null
    }
  }
  
  private def convertToByte(value: Any): Byte = {
    value match {
      case b: Byte => b
      case i: Int => i.toByte
      case l: Long => l.toByte
      case f: Float => f.toByte
      case d: Double => d.toByte
      case decimal: org.apache.spark.sql.types.Decimal => decimal.toInt.toByte
      case null => 0.toByte
      case _ => throw new IllegalArgumentException(s"Cannot convert ${value.getClass} to Byte: $value")
    }
  }
  
  private def computeJaccardDistance(v1: Array[Byte], v2: Array[Byte]): Double = {
    if (v1.length != v2.length) return 1.0 // Maximum distance
    
    var intersection = 0
    var union = 0
    
    var i = 0
    while (i < v1.length) {
      val and = v1(i) & v2(i)
      val or = v1(i) | v2(i)
      intersection += java.lang.Integer.bitCount(and & 0xff)
      union += java.lang.Integer.bitCount(or & 0xff)
      i += 1
    }
    
    if (union == 0) 0.0 else 1.0 - (intersection.toDouble / union.toDouble)
  }
  
  override def prettyName: String = "jaccard_distance"
}

/**
 * Vector KNN Expression - simplified version
 * This is a basic implementation for demonstration
 */
case class VectorKNNExpression(
  vectors: Expression,
  queryVector: Expression, 
  k: Expression,
  distanceType: Expression
) extends Expression with CodegenFallback {
  
  override protected def withNewChildrenInternal(
    newChildren: IndexedSeq[Expression]
  ): Expression = {
    require(newChildren.length == 4)
    copy(
      vectors = newChildren(0),
      queryVector = newChildren(1),
      k = newChildren(2),
      distanceType = newChildren(3)
    )
  }
  
  override def children: Seq[Expression] = Seq(vectors, queryVector, k, distanceType)
  override def dataType: DataType = ArrayType(StructType(Seq(
    StructField("index", IntegerType, false),
    StructField("score", DoubleType, false)
  )))
  override def nullable: Boolean = true
  
  override def eval(input: InternalRow): Any = {
    // This is a simplified implementation
    // In practice, this would need more sophisticated logic
    null
  }
  
  override def prettyName: String = "vector_knn"
}