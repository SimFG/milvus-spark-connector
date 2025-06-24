package com.zilliz.spark.connector.jni

object NativeLib {
  // Declare native methods
  @native def getGreeting(): String
  @native def processArray(array: Array[Int]): Array[Int]
  @native def calculateSum(array: Array[Double]): Double
  @native def processString(input: String): String

  // Static initialization block, load native library
  try {
    // First try to load from java.library.path
    System.loadLibrary("mylibrary")
    println("Successfully loaded mylibrary from system library path")
  } catch {
    case _: UnsatisfiedLinkError =>
      try {
        // If failed, try to load from resources
        val resourcePath = "/native/libmylibrary.so"
        val inputStream = getClass.getResourceAsStream(resourcePath)
        if (inputStream != null) {
          val tempFile = java.io.File.createTempFile("libmylibrary", ".so")
          tempFile.deleteOnExit()

          val outputStream = new java.io.FileOutputStream(tempFile)
          val buffer = new Array[Byte](1024)
          var bytesRead = inputStream.read(buffer)
          while (bytesRead != -1) {
            outputStream.write(buffer, 0, bytesRead)
            bytesRead = inputStream.read(buffer)
          }
          inputStream.close()
          outputStream.close()

          System.load(tempFile.getAbsolutePath)
          println(
            s"Successfully loaded mylibrary from resources: ${tempFile.getAbsolutePath}"
          )
        } else {
          throw new RuntimeException("Native library not found in resources")
        }
      } catch {
        case e: Exception =>
          println(s"Failed to load native library: ${e.getMessage}")
          throw new RuntimeException("Could not load native library", e)
      }
  }

  // Test method to verify JNI is working properly
  def testNativeLibrary(): Unit = {
    try {
      println("Testing JNI library...")
      println(s"Greeting: ${getGreeting()}")

      val testArray = Array(1, 2, 3, 4, 5)
      val processedArray = processArray(testArray)
      println(s"Original array: ${testArray.toSeq.mkString(", ")}")
      println(s"Processed array: ${processedArray.toSeq.mkString(", ")}")

      val doubleArray = Array(1.1, 2.2, 3.3, 4.4, 5.5)
      val sum = calculateSum(doubleArray)
      println(s"Sum of ${doubleArray.toSeq.mkString(", ")} = $sum")

      val testString = "Hello JNI"
      val processedString = processString(testString)
      println(s"Original string: $testString")
      println(s"Processed string: $processedString")

      println("JNI library test completed successfully!")
    } catch {
      case e: Exception =>
        println(s"JNI library test failed: ${e.getMessage}")
        throw e
    }
  }

  // Main method for standalone JNI library testing
  def main(args: Array[String]): Unit = {
    println("=== Standalone JNI Library Test ===")
    testNativeLibrary()
    println("=== Test completed ===")
  }
}
