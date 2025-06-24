#include <jni.h>
#include <iostream>
#include <vector>
#include <string>
#include <algorithm>
#include <memory>
#include "com_zilliz_spark_connector_jni_NativeLib.h"

// Use C++ namespace
namespace milvus_jni
{

  // C++ helper functions
  std::string toUpperCase(const std::string &str)
  {
    std::string result = str;
    std::transform(result.begin(), result.end(), result.begin(), ::toupper);
    return result;
  }

  std::vector<int> processIntArray(const std::vector<int> &input)
  {
    std::vector<int> result;
    result.reserve(input.size());

    for (int value : input)
    {
      result.push_back(value * 2);
    }

    return result;
  }

  double calculateSum(const std::vector<double> &input)
  {
    double sum = 0.0;
    for (double value : input)
    {
      sum += value;
    }
    return sum;
  }

} // namespace milvus_jni

// JNI function implementations - must use extern "C" to avoid C++ name mangling
extern "C"
{

  // Implementation of getGreeting method
  JNIEXPORT jstring JNICALL Java_com_zilliz_spark_connector_jni_NativeLib_00024_getGreeting(JNIEnv *env, jobject /* obj */)
  {
    try
    {
      std::string greeting = "Hello from C++ JNI! This is a modern Spark-compatible native library.";
      return env->NewStringUTF(greeting.c_str());
    }
    catch (const std::exception &e)
    {
      // Throw Java exception
      jclass exceptionClass = env->FindClass("java/lang/RuntimeException");
      env->ThrowNew(exceptionClass, e.what());
      return nullptr;
    }
  }

  // Implementation of processArray method - use C++ vector to process arrays
  JNIEXPORT jintArray JNICALL Java_com_zilliz_spark_connector_jni_NativeLib_00024_processArray(JNIEnv *env, jobject /* obj */, jintArray array)
  {
    try
    {
      // Get array length
      jsize length = env->GetArrayLength(array);
      if (length == 0)
      {
        return env->NewIntArray(0);
      }

      // Get array elements
      jint *elements = env->GetIntArrayElements(array, nullptr);
      if (elements == nullptr)
      {
        jclass exceptionClass = env->FindClass("java/lang/OutOfMemoryError");
        env->ThrowNew(exceptionClass, "Failed to get array elements");
        return nullptr;
      }

      // Use C++ vector to process data
      std::vector<int> inputVector(elements, elements + length);
      std::vector<int> resultVector = milvus_jni::processIntArray(inputVector);

      // Create result array
      jintArray result = env->NewIntArray(length);
      if (result == nullptr)
      {
        env->ReleaseIntArrayElements(array, elements, 0);
        jclass exceptionClass = env->FindClass("java/lang/OutOfMemoryError");
        env->ThrowNew(exceptionClass, "Failed to create result array");
        return nullptr;
      }

      // Set result array
      env->SetIntArrayRegion(result, 0, length, resultVector.data());

      // Release memory
      env->ReleaseIntArrayElements(array, elements, 0);

      return result;
    }
    catch (const std::exception &e)
    {
      jclass exceptionClass = env->FindClass("java/lang/RuntimeException");
      env->ThrowNew(exceptionClass, e.what());
      return nullptr;
    }
  }

  // Implementation of calculateSum method - use C++ vector to calculate sum
  JNIEXPORT jdouble JNICALL Java_com_zilliz_spark_connector_jni_NativeLib_00024_calculateSum(JNIEnv *env, jobject /* obj */, jdoubleArray array)
  {
    try
    {
      jsize length = env->GetArrayLength(array);
      if (length == 0)
      {
        return 0.0;
      }

      jdouble *elements = env->GetDoubleArrayElements(array, nullptr);
      if (elements == nullptr)
      {
        jclass exceptionClass = env->FindClass("java/lang/OutOfMemoryError");
        env->ThrowNew(exceptionClass, "Failed to get array elements");
        return 0.0;
      }

      // Use C++ vector and algorithms
      std::vector<double> inputVector(elements, elements + length);
      double sum = milvus_jni::calculateSum(inputVector);

      env->ReleaseDoubleArrayElements(array, elements, 0);
      return sum;
    }
    catch (const std::exception &e)
    {
      jclass exceptionClass = env->FindClass("java/lang/RuntimeException");
      env->ThrowNew(exceptionClass, e.what());
      return 0.0;
    }
  }

  // Implementation of processString method - use C++ string processing
  JNIEXPORT jstring JNICALL Java_com_zilliz_spark_connector_jni_NativeLib_00024_processString(JNIEnv *env, jobject /* obj */, jstring input)
  {
    try
    {
      // Get input string
      const char *inputStr = env->GetStringUTFChars(input, nullptr);
      if (inputStr == nullptr)
      {
        jclass exceptionClass = env->FindClass("java/lang/OutOfMemoryError");
        env->ThrowNew(exceptionClass, "Failed to get input string");
        return nullptr;
      }

      // Use C++ string processing
      std::string inputString(inputStr);
      std::string processedString = "PROCESSED: " + milvus_jni::toUpperCase(inputString);

      // Create result string
      jstring result = env->NewStringUTF(processedString.c_str());

      // Release memory
      env->ReleaseStringUTFChars(input, inputStr);

      return result;
    }
    catch (const std::exception &e)
    {
      jclass exceptionClass = env->FindClass("java/lang/RuntimeException");
      env->ThrowNew(exceptionClass, e.what());
      return nullptr;
    }
  }

} // extern "C"