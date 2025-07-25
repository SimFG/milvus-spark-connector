#!/bin/bash

# Generate JNI Header Script
# This script generates JNI headers for the MilvusStorageJNI class

set -e

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
SCALA_SRC="$PROJECT_ROOT/src/main/scala"
C_SRC="$PROJECT_ROOT/src/main/c"
INCLUDE_DIR="$C_SRC/include"
TARGET_DIR="$PROJECT_ROOT/target"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}Generating JNI headers...${NC}"

# Create include directory if it doesn't exist
mkdir -p "$INCLUDE_DIR"

# Find Java/JDK
if [ -z "$JAVA_HOME" ]; then
    JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))
fi

echo -e "${YELLOW}Using JAVA_HOME: $JAVA_HOME${NC}"

# Create header manually
HEADER_FILE="$INCLUDE_DIR/com_zilliz_spark_connector_jni_MilvusStorageJNI.h"

echo -e "${YELLOW}Creating JNI header manually...${NC}"
cat > "$HEADER_FILE" << 'HEADER_EOF'
/* DO NOT EDIT THIS FILE - it is machine generated */
#include <jni.h>
/* Header for class com_zilliz_spark_connector_jni_MilvusStorageJNI */

#ifndef _Included_com_zilliz_spark_connector_jni_MilvusStorageJNI
#define _Included_com_zilliz_spark_connector_jni_MilvusStorageJNI
#ifdef __cplusplus
extern "C" {
#endif

/*
 * Class:     com_zilliz_spark_connector_jni_MilvusStorageJNI
 * Method:    createSpace
 * Signature: (Ljava/lang/String;Ljava/lang/String;)J
 */
JNIEXPORT jlong JNICALL Java_com_zilliz_spark_connector_jni_MilvusStorageJNI_00024_createSpace
  (JNIEnv *, jclass, jstring, jstring);

/*
 * Class:     com_zilliz_spark_connector_jni_MilvusStorageJNI
 * Method:    writeData
 * Signature: (JLjava/lang/String;)Z
 */
JNIEXPORT jboolean JNICALL Java_com_zilliz_spark_connector_jni_MilvusStorageJNI_00024_writeData
  (JNIEnv *, jclass, jlong, jstring);

/*
 * Class:     com_zilliz_spark_connector_jni_MilvusStorageJNI
 * Method:    readData
 * Signature: (J[Ljava/lang/String;)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_com_zilliz_spark_connector_jni_MilvusStorageJNI_00024_readData
  (JNIEnv *, jclass, jlong, jobjectArray);

/*
 * Class:     com_zilliz_spark_connector_jni_MilvusStorageJNI
 * Method:    readDataWithFilter
 * Signature: (J[Ljava/lang/String;Ljava/lang/String;)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_com_zilliz_spark_connector_jni_MilvusStorageJNI_00024_readDataWithFilter
  (JNIEnv *, jclass, jlong, jobjectArray, jstring);

/*
 * Class:     com_zilliz_spark_connector_jni_MilvusStorageJNI
 * Method:    getSpaceVersion
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_com_zilliz_spark_connector_jni_MilvusStorageJNI_00024_getSpaceVersion
  (JNIEnv *, jclass, jlong);

/*
 * Class:     com_zilliz_spark_connector_jni_MilvusStorageJNI
 * Method:    getStorageSize
 * Signature: (Ljava/lang/String;)J
 */
JNIEXPORT jlong JNICALL Java_com_zilliz_spark_connector_jni_MilvusStorageJNI_00024_getStorageSize
  (JNIEnv *, jclass, jstring);

/*
 * Class:     com_zilliz_spark_connector_jni_MilvusStorageJNI
 * Method:    closeSpace
 * Signature: (J)Z
 */
JNIEXPORT jboolean JNICALL Java_com_zilliz_spark_connector_jni_MilvusStorageJNI_00024_closeSpace
  (JNIEnv *, jclass, jlong);

/*
 * Class:     com_zilliz_spark_connector_jni_MilvusStorageJNI
 * Method:    deleteSpace
 * Signature: (Ljava/lang/String;)Z
 */
JNIEXPORT jboolean JNICALL Java_com_zilliz_spark_connector_jni_MilvusStorageJNI_00024_deleteSpace
  (JNIEnv *, jclass, jstring);

#ifdef __cplusplus
}
#endif
#endif
HEADER_EOF

# Verify header file exists
if [ -f "$HEADER_FILE" ]; then
    echo -e "${GREEN}JNI header generated successfully: $HEADER_FILE${NC}"
else
    echo -e "${RED}Failed to generate JNI header${NC}"
    exit 1
fi

echo -e "${GREEN}JNI header generation complete${NC}"
