val scala361 = "3.6.1"

ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := scala361

lazy val root = (project in file("."))
  .settings(
    name := "arcane-framework",
    idePackagePrefix := Some("com.sneaksanddata.arcane.framework"),

    // Compiler options
    Test / logBuffered := false,

    // Framework dependencies
    libraryDependencies += "dev.zio" %% "zio" % "2.1.6",
    libraryDependencies += "dev.zio" %% "zio-streams" % "2.1.6",
    libraryDependencies += "com.microsoft.sqlserver" % "mssql-jdbc" % "12.8.1.jre11",
    libraryDependencies += "software.amazon.awssdk" % "s3" % "2.25.27", 
    libraryDependencies += "com.lihaoyi" %% "upickle" % "4.0.2",

    // https://mvnrepository.com/artifact/org.apache.iceberg/iceberg-api
    libraryDependencies += "org.apache.iceberg" % "iceberg-api" % "1.7.0",
    // https://mvnrepository.com/artifact/org.apache.iceberg/iceberg-core
    libraryDependencies += "org.apache.iceberg" % "iceberg-core" % "1.7.0",
    // https://mvnrepository.com/artifact/org.apache.iceberg/iceberg-parquet
    libraryDependencies += "org.apache.iceberg" % "iceberg-parquet" % "1.7.0",
    // https://mvnrepository.com/artifact/org.apache.iceberg/iceberg-aws
    libraryDependencies += "org.apache.iceberg" % "iceberg-aws" % "1.7.0",
    // https://mvnrepository.com/artifact/software.amazon.awssdk/auth
    libraryDependencies += "software.amazon.awssdk" % "auth" % "2.25.27",
    // https://mvnrepository.com/artifact/software.amazon.awssdk/http-auth-aws
    libraryDependencies += "software.amazon.awssdk" % "http-auth-aws" % "2.29.9",
    // https://mvnrepository.com/artifact/software.amazon.awssdk/sts
    libraryDependencies += "software.amazon.awssdk" % "sts" % "2.29.9",
    // https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-common
    libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "3.4.1",
    // https://mvnrepository.com/artifact/io.trino/trino-jdbc
    libraryDependencies += "io.trino" % "trino-jdbc" % "465",

    // Azure dependencies
    // https://mvnrepository.com/artifact/com.azure/azure-storage-blob
    libraryDependencies += "com.azure" % "azure-storage-blob" % "12.29.0",
    // https://mvnrepository.com/artifact/com.azure/azure-identity
    libraryDependencies += "com.azure" % "azure-identity" % "1.14.2",


    // Test dependencies
    libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.19" % Test,
    libraryDependencies += "org.scalatest" %% "scalatest-flatspec" % "3.2.19" % Test,

    // Logging and metrics
    // For ZIO
    libraryDependencies += "dev.zio" %% "zio-logging" % "2.3.0",
    libraryDependencies += "dev.zio" %% "zio-logging-slf4j" % "2.3.0",

    // For DataDog
//    libraryDependencies += "org.slf4j" % "log4j-over-slf4j" % "2.0.13",
    libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.5.6",
    libraryDependencies +=  "net.logstash.logback" % "logstash-logback-encoder" % "8.0",
  )
