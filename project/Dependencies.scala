import sbt._

object Dependencies {
  // Version constants
  lazy val scalapbVersion = "0.11.3"
  lazy val sparkVersion = "3.5.3"
  lazy val grpcJavaVersion = "1.37.0"
  // lazy val sparkVersion = "3.3.2"
  lazy val parquetVersion = "1.13.1"
  lazy val hadoopVersion =
    "3.3.4" // can't be changed https://github.com/apache/spark/blob/v3.5.3/pom.xml
  lazy val jacksonVersion = "2.17.3"

  lazy val munit = "org.scalameta" %% "munit" % "0.7.29"
  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.2.15"
  lazy val grpcNetty =
    "io.grpc" % "grpc-netty-shaded" % grpcJavaVersion excludeAll ExclusionRule(
      organization = "org.slf4j"
    )
  lazy val scalapbRuntime =
    "com.thesamet.scalapb" %% "scalapb-runtime" % scalapbVersion
  lazy val scalapbRuntimeGrpc =
    "com.thesamet.scalapb" %% "scalapb-runtime-grpc" % scalapbVersion
  lazy val scalapbCompilerPlugin =
    "com.thesamet.scalapb" %% "compilerplugin" % scalapbVersion
  lazy val sparkCore =
    "org.apache.spark" %% "spark-core" % sparkVersion % "provided,test"
  lazy val sparkSql =
    "org.apache.spark" %% "spark-sql" % sparkVersion % "provided,test"
  lazy val sparkCatalyst =
    "org.apache.spark" %% "spark-catalyst" % sparkVersion % "provided,test"
  lazy val sparkMLlib =
    "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided,test"
  lazy val parquetHadoop =
    "org.apache.parquet" % "parquet-hadoop" % parquetVersion
  lazy val hadoopCommon =
    "org.apache.hadoop" % "hadoop-common" % hadoopVersion % "provided,test" exclude ("javax.activation", "activation")
  lazy val hadoopAws =
    "org.apache.hadoop" % "hadoop-aws" % hadoopVersion
  lazy val awsSdkS3 =
    "software.amazon.awssdk" % "s3" % "2.30.38" // doc: https://javadoc.io/doc/software.amazon.awssdk/s3/2.30.38/index.html
  lazy val jacksonScala =
    "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion
  lazy val jacksonDatabind =
    "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion
}
