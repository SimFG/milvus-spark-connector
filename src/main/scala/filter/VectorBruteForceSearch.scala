package com.zilliz.spark.connector.filter

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.ml.linalg.{DenseVector, Vector, Vectors}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object VectorBruteForceSearch {

  // Distance metric type enumeration
  object DistanceType extends Enumeration {
    type DistanceType = Value
    val COSINE, L2, IP = Value
  }

  // Search type enumeration
  object SearchType extends Enumeration {
    type SearchType = Value
    val KNN, RANGE = Value
  }

  import DistanceType._
  import SearchType._

  // Cosine similarity calculation
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

  // L2 distance calculation
  private def l2Distance(v1: Vector, v2: Vector): Double = {
    require(v1.size == v2.size, "Vectors must have the same size")
    var sum = 0.0
    for (i <- 0 until v1.size) {
      val diff = v1(i) - v2(i)
      sum += diff * diff
    }
    scala.math.sqrt(sum)
  }

  // Inner product calculation
  private def innerProduct(v1: Vector, v2: Vector): Double = {
    v1.dot(v2)
  }

  // Unified distance calculation function
  private def calculateDistance(
      v1: Vector,
      v2: Vector,
      distanceType: DistanceType
  ): Double = {
    distanceType match {
      case COSINE => cosineSimilarity(v1, v2)
      case L2     => l2Distance(v1, v2)
      case IP     => innerProduct(v1, v2)
    }
  }

  // Check if distance meets threshold condition
  private def meetsThreshold(
      distance: Double,
      threshold: Double,
      distanceType: DistanceType
  ): Boolean = {
    distanceType match {
      case COSINE =>
        distance >= threshold // Cosine similarity, higher values are more similar
      case L2 =>
        distance <= threshold // L2 distance, lower values are more similar
      case IP =>
        distance >= threshold // Inner product, higher values are more similar
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
      idCol: Option[String] = None,
      distanceType: DistanceType = COSINE,
      searchType: SearchType = KNN,
      radius: Option[Double] = None
  ): DataFrame = {
    val spark = df.sparkSession
    import spark.implicits._

    val schema = df.schema
    if (!schema.fieldNames.contains(vectorCol)) {
      throw new IllegalArgumentException(
        s"DataFrame does not contain vector column: $vectorCol"
      )
    }

    // Validate search type and parameters
    searchType match {
      case RANGE if radius.isEmpty =>
        throw new IllegalArgumentException(
          "Range search requires a radius parameter"
        )
      case _ => // All other cases are valid
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
    val broadcastDistanceType = spark.sparkContext.broadcast(distanceType)
    val broadcastSearchType = spark.sparkContext.broadcast(searchType)
    val broadcastRadius = spark.sparkContext.broadcast(radius)

    val searchResultsRDD = processedDF.rdd.mapPartitions { partitionIter =>
      val qv = broadcastQueryVector.value
      val currentK = broadcastK.value
      val currentDistanceType = broadcastDistanceType.value
      val currentSearchType = broadcastSearchType.value
      val currentRadius = broadcastRadius.value
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
        val distance = calculateDistance(qv, vector, currentDistanceType)

        // Determine whether to add result based on search type
        val shouldAdd = currentSearchType match {
          case KNN =>
            meetsThreshold(
              distance,
              broadcastThreshold.value,
              currentDistanceType
            )
          case RANGE =>
            currentRadius match {
              case Some(r) =>
                currentDistanceType match {
                  case COSINE =>
                    distance >= (1.0 - r) // Range search for cosine distance
                  case L2 => distance <= r // Range search for L2 distance
                  case IP => distance >= r // Range search for inner product
                }
              case None =>
                false // This case should not happen as it's validated above
            }
        }

        if (shouldAdd) {
          partitionResults += ((row, distance))
        }
      }

      // Sort results based on search type and distance type
      val sortedResults = currentDistanceType match {
        case COSINE | IP =>
          partitionResults.sortBy(-_._2) // Descending, higher values are better
        case L2 =>
          partitionResults.sortBy(_._2) // Ascending, lower values are better
      }

      // Determine number of results to return based on search type
      currentSearchType match {
        case KNN => sortedResults.take(currentK).iterator
        case RANGE =>
          sortedResults.iterator // Range search returns all matching results
      }
    }

    val originalSchema = processedDF.schema
    val distanceColName = distanceType match {
      case COSINE => "similarity"
      case L2     => "distance"
      case IP     => "inner_product"
    }
    val resultSchema = originalSchema.add(distanceColName, DoubleType)

    val searchResultsWithDistance = searchResultsRDD.map {
      case (row, distance) =>
        val values = row.toSeq :+ distance
        Row.fromSeq(values)
    }

    var finalResultsDF =
      spark.createDataFrame(searchResultsWithDistance, resultSchema)

    // Sort results based on distance type
    finalResultsDF = distanceType match {
      case COSINE | IP => finalResultsDF.orderBy(desc(distanceColName))
      case L2          => finalResultsDF.orderBy(asc(distanceColName))
    }

    // Limit result count based on search type
    if (searchType == KNN) {
      finalResultsDF = finalResultsDF.limit(k)
    }

    finalResultsDF = finalResultsDF.drop(tempVectorColName)
    if (idCol.isEmpty) {
      finalResultsDF.drop(tempIdCol)
    } else {
      finalResultsDF
    }
  }
}
