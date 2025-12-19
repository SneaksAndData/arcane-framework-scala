import sbtrelease.ReleaseStateTransformations.{checkSnapshotDependencies, inquireVersions, publishArtifacts, runClean, setReleaseVersion}

val scala361 = "3.6.1"

ThisBuild / organization := "com.sneaksanddata"
ThisBuild / scalaVersion := scala361

credentials += Credentials(
    "GitHub Package Registry",
    "maven.pkg.github.com",
    sys.env.getOrElse("GITHUB_ACTOR", ""),
    sys.env.getOrElse("GITHUB_TOKEN", "")
)

releaseVersionFile := file("version.sbt")
releaseVersionBump := sbtrelease.Version.Bump.Bugfix
releaseProcess := Seq[ReleaseStep](
    checkSnapshotDependencies,              // : ReleaseStep
    inquireVersions,                        // : ReleaseStep
    runClean,                               // : ReleaseStep
    setReleaseVersion,                      // : ReleaseStep
    publishArtifacts,                       // : ReleaseStep
)
releaseIgnoreUntrackedFiles := true
publishTo := {
    val ghRepo = "SneaksAndData/arcane-framework-scala"
    val ghUser = "_"
    val ghToken = sys.env.get("GITHUB_TOKEN")
    ghToken.map { token =>
        "GitHub Package Registry" at s"https://maven.pkg.github.com/$ghRepo"
    }
}
publishMavenStyle := true

lazy val root = (project in file("."))
  .settings(
    name := "arcane-framework",
    idePackagePrefix := Some("com.sneaksanddata.arcane.framework"),

    // Compiler options
    Test / logBuffered := false,

    // Framework dependencies
    libraryDependencies += "dev.zio" %% "zio" % "2.1.23",
    libraryDependencies += "dev.zio" %% "zio-streams" % "2.1.23",
    libraryDependencies += "com.microsoft.sqlserver" % "mssql-jdbc" % "12.8.2.jre11",
    libraryDependencies += "software.amazon.awssdk" % "s3" % "2.33.13",
    libraryDependencies += "com.lihaoyi" %% "upickle" % "4.4.1",
    libraryDependencies += "org.scala-lang.modules" %% "scala-parallel-collections" % "1.2.0",

    // Iceberg deps - read
    // https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-mapreduce-client-core
    libraryDependencies += "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "3.4.2",

    // Iceberg deps - core API, S3 and write
    // https://mvnrepository.com/artifact/org.apache.iceberg/iceberg-api
    libraryDependencies += "org.apache.iceberg" % "iceberg-api" % "1.10.0",
    // https://mvnrepository.com/artifact/org.apache.iceberg/iceberg-core
    libraryDependencies += "org.apache.iceberg" % "iceberg-core" % "1.10.0",
    // https://mvnrepository.com/artifact/org.apache.iceberg/iceberg-parquet
    libraryDependencies += "org.apache.iceberg" % "iceberg-parquet" % "1.10.0",
    // https://mvnrepository.com/artifact/org.apache.iceberg/iceberg-aws
    libraryDependencies += "org.apache.iceberg" % "iceberg-aws" % "1.10.0",
    // https://mvnrepository.com/artifact/software.amazon.awssdk/auth
    libraryDependencies += "software.amazon.awssdk" % "auth" % "2.33.13",
    // https://mvnrepository.com/artifact/software.amazon.awssdk/http-auth-aws
    libraryDependencies += "software.amazon.awssdk" % "http-auth-aws" % "2.33.13",
    // https://mvnrepository.com/artifact/software.amazon.awssdk/sts
    libraryDependencies += "software.amazon.awssdk" % "sts" % "2.33.13",
    // https://mvnrepository.com/artifact/software.amazon.awssdk/kms
    libraryDependencies += "software.amazon.awssdk" % "kms" % "2.33.13",
    // https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-common
    libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "3.4.2",
    // https://mvnrepository.com/artifact/io.trino/trino-jdbc
    libraryDependencies += "io.trino" % "trino-jdbc" % "478",

    // Azure dependencies
    // https://mvnrepository.com/artifact/com.azure/azure-storage-blob
    libraryDependencies += "com.azure" % "azure-storage-blob" % "12.32.0",
    // https://mvnrepository.com/artifact/com.azure/azure-identity
    libraryDependencies += "com.azure" % "azure-identity" % "1.18.1",
    // Jackson pin
    libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.18.1",
    libraryDependencies += "com.fasterxml.jackson.core" % "jackson-core" % "2.18.1",
    libraryDependencies += "com.fasterxml.jackson.core" % "jackson-annotations" % "2.18.1",
    libraryDependencies += "com.fasterxml.jackson.datatype" % "jackson-datatype-jsr310" % "2.18.1",


    // Test dependencies
    libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.19" % Test,
    libraryDependencies += "org.scalatest" %% "scalatest-flatspec" % "3.2.19" % Test,
    libraryDependencies += "org.scalatestplus" %% "easymock-5-3" % "3.2.19.0" % Test,
    libraryDependencies += "dev.zio" %% "zio-test"          % "2.1.23" % Test,
    libraryDependencies += "dev.zio" %% "zio-test-sbt"      % "2.1.23" % Test,

    // Logging and metrics
    // For ZIO
    libraryDependencies += "dev.zio" %% "zio-logging" % "2.5.2",
    libraryDependencies += "dev.zio" %% "zio-logging-slf4j2" % "2.5.2",
    
    // For DataDog
    libraryDependencies += "org.apache.logging.log4j" % "log4j-to-slf4j" % "2.25.3",
    libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.5.22",
    libraryDependencies += "net.logstash.logback" % "logstash-logback-encoder" % "9.0",

    // Metrics
    libraryDependencies += "dev.zio" %% "zio-metrics-connectors" % "2.5.4",
    libraryDependencies += "dev.zio" %% "zio-metrics-connectors-datadog" % "2.5.4",
  )
