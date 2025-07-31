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
      case _ => false
    }
  }
  
  protected def extractFloatArray(value: Any): Array[Float] = {
    value match {
      case arrayData: ArrayData =>
        arrayData.toFloatArray()
      case _ => null
    }
  }
  
  protected def extractDoubleArray(value: Any): Array[Double] = {
    value match {
      case arrayData: ArrayData =>
        arrayData.toDoubleArray()
      case _ => null
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
        arrayData.toByteArray()
      case _ => null
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
        arrayData.toByteArray()
      case _ => null
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