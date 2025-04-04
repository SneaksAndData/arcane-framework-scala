package com.sneaksanddata.arcane.framework
package services.connectors.mssql

import models.ArcaneType.{ByteArrayType, IntType, StringType, TimestampType}
import models.*

import scala.jdk.CollectionConverters.*
import services.lakehouse.base.{CatalogWriter, IcebergCatalogSettings, S3CatalogFileIO}
import services.lakehouse.{IcebergCatalogCredential, IcebergS3CatalogWriter, given_Conversion_ArcaneSchema_Schema}

import com.sneaksanddata.arcane.framework.services.connectors.mssql.JdbcToIcebergTypeConverterTests.test
import com.sneaksanddata.arcane.framework.services.mssql.JdbcToIcebergTypeConverter
import org.apache.iceberg.rest.RESTCatalog
import org.apache.iceberg.{Schema, Table}
import zio.test.TestAspect.timeout
import zio.{Scope, ZIO, ZLayer}
import zio.test.{Spec, TestAspect, TestEnvironment, ZIOSpecDefault, assertTrue}

import java.util.UUID

object JdbcToIcebergTypeConverterTests extends ZIOSpecDefault:
  private val settings = new IcebergCatalogSettings:
    override val namespace = "test"
    override val warehouse = "polaris"
    override val catalogUri = "http://localhost:8181/api/catalog"
    override val additionalProperties: Map[String, String] = IcebergCatalogCredential.oAuth2Properties
    override val s3CatalogFileIO: S3CatalogFileIO = S3CatalogFileIO
    override val stagingLocation: Option[String] = Some("s3://tmp/polaris/test")
  private val testAspects = timeout(zio.Duration.fromSeconds(60)) @@ TestAspect.withLiveClock

  private val services = JdbcToIcebergTypeConverter.layer >+> ZLayer.succeed(settings) >+> IcebergS3CatalogWriter.layer

  private def crateDataRows[T](lastElement: T, arcaneType: ArcaneType): (ArcaneSchema, List[DataRow]) =
    val schema = Seq(MergeKeyField, Field("colA", IntType), Field("colB", StringType), Field("colC", arcaneType))
    val rows = List(
      DataCell(MergeKeyField.name, MergeKeyField.fieldType, "key1")
        :: DataCell("colA", IntType, 1)
        :: DataCell("colB", StringType, "abc")
        :: DataCell("colC", arcaneType, lastElement)
        :: Nil,
    )
    (schema, rows)

  def spec: Spec[TestEnvironment & Scope, Throwable] = suite(classOf[JdbcToIcebergTypeConverter].getName)(
    test("convert ByteArray to Iceberg-compatible type") {
      for
        writer <- ZIO.service[CatalogWriter[RESTCatalog, Table, Schema]]
        (schema, rows) = crateDataRows(Array[Byte](1, 2, 3, 4, 5), ByteArrayType)
        table <- writer.write(rows, UUID.randomUUID.toString, schema)
      yield assertTrue(table.history().asScala.nonEmpty)
    },
    test("convert Timestamp to Iceberg-compatible type") {
      for
        writer <- ZIO.service[CatalogWriter[RESTCatalog, Table, Schema]]
        (schema, rows) = crateDataRows(java.sql.Timestamp(System.currentTimeMillis()), TimestampType)
        table <- writer.write(rows, UUID.randomUUID.toString, schema)
      yield assertTrue(table.history().asScala.nonEmpty)
    }
  ).provide(services) @@ testAspects
