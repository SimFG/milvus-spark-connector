package com.zilliz.spark.connector.writer

import org.apache.spark.sql.connector.write.WriterCommitMessage

case class MilvusCommitMessage(rowCount: Int) extends WriterCommitMessage
