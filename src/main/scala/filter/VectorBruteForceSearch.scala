package com.zilliz.spark.connector.filter

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.ml.linalg.{DenseVector, Vector, Vectors}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object VectorBruteForceSearch {
  private def cosineSimilarity(v1: Vector, v2: Vector): Double = {
    val dotProduct = v1.dot(v2)
    val normV1 = Vectors.norm(v1, 2.0)
    val normV2 = Vectors.norm(v2, 2.0)

    if (normV1 == 0.0 || normV2 == 0.0) {
      0.0
    } else {
      dotProduct / (normV1 * normV2)
    }
  }

  private val arrayFloatToDenseVectorUDF = udf((arr: Seq[Float]) => {
    if (arr == null) {
      null
    } else {
      Vectors.dense(arr.map(_.toDouble).toArray)
    }
  })

  def filterSimilarVectors(
      df: DataFrame,
      queryVector: Seq[Float],
      k: Int = 10,
      threshold: Double = 0.0,
      vectorCol: String = "vector",
      idCol: Option[String] = None
  ): DataFrame = {
    val spark = df.sparkSession
    import spark.implicits._

    val schema = df.schema
    if (!schema.fieldNames.contains(vectorCol)) {
      throw new IllegalArgumentException(
        s"DataFrame does not contain vector column: $vectorCol"
      )
    }

    val tempVectorColName = "_converted_dense_vector_"
    var processedDF = schema(vectorCol).dataType match {
      case ArrayType(FloatType, _) =>
        df.withColumn(
          tempVectorColName,
          arrayFloatToDenseVectorUDF(col(vectorCol))
        )
      case _ =>
        throw new IllegalArgumentException(
          s"Vector column '$vectorCol' must be of type Array[Float]. Found: ${schema(vectorCol).dataType}"
        )
    }

    val tempIdCol = "_brute_force_search_row_id_"
    val effectiveIdCol = idCol match {
      case Some(colName) =>
        if (!schema.fieldNames.contains(colName)) {
          throw new IllegalArgumentException(
            s"DataFrame does not contain ID column: $colName"
          )
        }
        colName
      case None =>
        println(
          "Warning: No ID column provided. Adding a default row index, which may cause a shuffle. " +
            "For production, consider ensuring your DataFrame has a unique identifier column."
        )
        processedDF =
          processedDF.withColumn(tempIdCol, monotonically_increasing_id())
        tempIdCol
    }

    val broadcastQueryVector =
      spark.sparkContext.broadcast(
        Vectors.dense(queryVector.map(_.toDouble).toArray)
      )
    val broadcastThreshold = spark.sparkContext.broadcast(threshold)
    val broadcastK = spark.sparkContext.broadcast(k)

    val searchResultsRDD = processedDF.rdd.mapPartitions { partitionIter =>
      val qv = broadcastQueryVector.value
      val currentK = broadcastK.value
      val partitionResults = ArrayBuffer[(Row, Double)]()

      partitionIter.foreach { row =>
        val id = row.getAs[Any](effectiveIdCol) match {
          case i: Int  => i.toLong
          case l: Long => l
          case _ =>
            throw new IllegalArgumentException(
              s"ID column '$effectiveIdCol' must be a numeric type (Int or Long). Found: ${row.getAs[Any](effectiveIdCol).getClass.getName}"
            )
        }
        val vector = row.getAs[Vector](tempVectorColName)
        val similarity = cosineSimilarity(qv, vector)
        if (similarity >= broadcastThreshold.value) {
          partitionResults += ((row, similarity))
        }
      }
      partitionResults.sortBy(-_._2).take(currentK).iterator
    }

    val originalSchema = processedDF.schema
    val resultSchema = originalSchema.add("similarity", DoubleType)

    val searchResultsWithSimilarity = searchResultsRDD.map {
      case (row, similarity) =>
        val values = row.toSeq :+ similarity
        Row.fromSeq(values)
    }

    var finalResultsDF = spark
      .createDataFrame(searchResultsWithSimilarity, resultSchema)
      .orderBy(desc("similarity"))
      .limit(k)
    finalResultsDF = finalResultsDF.drop(tempVectorColName)
    if (idCol.isEmpty) {
      finalResultsDF.drop(tempIdCol)
    } else {
      finalResultsDF
    }
  }
}
