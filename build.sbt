import Dependencies._

ThisBuild / scalaVersion := "2.13.16"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / name := "spark-connector"
ThisBuild / organization := "com.zilliz"
ThisBuild / organizationName := "zilliz"
ThisBuild / organizationHomepage := Some(url("https://zilliz.com/"))
ThisBuild / developers := List(
  Developer(
    id = "simfg",
    name = "SimFG",
    email = "bang.fu@zilliz.com",
    url = url("https://github.com/SimFG")
  )
)
ThisBuild / description := "Milvus Spark Connector to use in Spark ETLs to populate a Milvus vector database."
ThisBuild / licenses := List(
  "Apache 2.0 License" -> new URL(
    "https://github.com/zilliztech/milvus-spark-connector/blob/main/LICENSE"
  )
)
ThisBuild / homepage := Some(
  url("https://github.com/zilliztech/milvus-spark-connector")
)
ThisBuild / publishTo := Some(Resolver.mavenLocal)
ThisBuild / publishMavenStyle := true

lazy val root = (project in file("."))
  .settings(
    name := "spark-connector",
    libraryDependencies ++= Seq(
      munit % Test,
      grpcNetty,
      scalapbRuntime % "protobuf",
      scalapbRuntimeGrpc,
      scalapbCompilerPlugin,
      sparkCore,
      sparkSql,
      sparkCatalyst,
      sparkMLlib,
      parquetCommon,
      parquetColumn,
      parquetHadoop,
      hadoopCommon,
      hadoopAws,
      awsSdkS3,
      jacksonScala,
      jacksonDatabind
    ),
    Compile / PB.protoSources += baseDirectory.value / "milvus-proto/proto",
    Compile / PB.targets := Seq(
      scalapb.gen(grpc = true) -> (Compile / sourceManaged).value / "scalapb"
    ),
    Compile / unmanagedSourceDirectories += (
      Compile / PB.targets
    ).value.head.outputPath,
    Compile / packageBin / mappings ++= {
      val base = (Compile / PB.targets).value.head.outputPath
      (base ** "*.scala").get.map { file =>
        file -> s"generated_protobuf/${file.relativeTo(base).getOrElse(file)}"
      }
    }
  )

assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("com.google.protobuf.**" -> "shade_proto.@1").inAll,
  ShadeRule.rename("com.google.common.**" -> "shade_googlecommon.@1").inAll
)

assembly / assemblyMergeStrategy := {
  case PathList(
        "META-INF",
        "native-image",
        "io.netty",
        "transport",
        "reflection-config.json"
      ) =>
    MergeStrategy.discard
  case PathList(
        "META-INF",
        "native-image",
        "io.netty",
        "codec-http",
        "native-image.properties"
      ) =>
    MergeStrategy.discard
  case PathList(
        "META-INF",
        "native-image",
        "io.netty",
        "handler",
        "native-image.properties"
      ) =>
    MergeStrategy.discard
  case PathList(
        "META-INF",
        "native-image",
        "io.netty",
        "common",
        "native-image.properties"
      ) =>
    MergeStrategy.discard
  case PathList(
        "META-INF",
        "native-image",
        "io.netty",
        "buffer",
        "native-image.properties"
      ) =>
    MergeStrategy.discard
  case PathList(
        "META-INF",
        "native-image",
        "io.netty",
        "transport",
        "native-image.properties"
      ) =>
    MergeStrategy.discard
  case PathList(
        "META-INF",
        "native-image",
        "io.netty",
        "netty-codec",
        "generated",
        "handlers",
        "reflect-config.json"
      ) =>
    MergeStrategy.discard
  case PathList(
        "META-INF",
        "native-image",
        "io.netty",
        "netty-handler",
        "generated",
        "handlers",
        "reflect-config.json"
      ) =>
    MergeStrategy.discard
  case PathList("META-INF", "io.netty.versions.properties") =>
    MergeStrategy.discard // 丢弃 netty 的版本信息文件
  case PathList("mime.types") =>
    MergeStrategy.filterDistinctLines // 合并 mime.types，保留不重复的行
  case PathList(
        "META-INF",
        "native-image",
        "io.netty",
        "netty-common",
        "native-image.properties"
      ) =>
    MergeStrategy.discard // 丢弃 native-image 属性文件
  case PathList("META-INF", "FastDoubleParser-NOTICE") =>
    MergeStrategy.discard
  case PathList(
        "META-INF",
        "native-image",
        "io.netty",
        "netty-codec",
        "generated",
        "handlers",
        "reflect-config.json"
      ) =>
    MergeStrategy.discard
  case PathList(
        "META-INF",
        "native-image",
        "io.netty",
        "netty-handler",
        "generated",
        "handlers",
        "reflect-config.json"
      ) =>
    MergeStrategy.discard
  case PathList(
        "META-INF",
        "native-image",
        "io.netty",
        "netty-common",
        "native-image.properties"
      ) =>
    MergeStrategy.discard
  case PathList("module-info.class")         => MergeStrategy.discard
  case x if x.endsWith("/module-info.class") => MergeStrategy.discard
  case x =>
    val oldStrategy = (ThisBuild / assemblyMergeStrategy).value
    oldStrategy(x)
}

// See https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html for instructions on how to publish to Sonatype.
