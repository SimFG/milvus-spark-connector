import sbt._

object Dependencies {
  // Version constants
  lazy val scalapbVersion = "0.11.3"
  lazy val sparkVersion = "3.5.3"
  // lazy val sparkVersion = "3.3.2"
  lazy val parquetVersion = "1.13.1"
  lazy val hadoopVersion = "3.3.6"
  lazy val jacksonVersion = "2.17.3"

  lazy val munit = "org.scalameta" %% "munit" % "0.7.29"
  lazy val grpcNetty =
    "io.grpc" % "grpc-netty-shaded" % scalapb.compiler.Version.grpcJavaVersion
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
  lazy val parquetCommon =
    "org.apache.parquet" % "parquet-common" % parquetVersion
  lazy val parquetColumn =
    "org.apache.parquet" % "parquet-column" % parquetVersion
  lazy val parquetHadoop =
    "org.apache.parquet" % "parquet-hadoop" % parquetVersion
  lazy val hadoopCommon =
    "org.apache.hadoop" % "hadoop-common" % hadoopVersion exclude ("javax.activation", "activation")
  lazy val jacksonScala =
    "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion
  lazy val jacksonDatabind =
    "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion
}
