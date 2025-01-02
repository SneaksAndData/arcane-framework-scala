package com.sneaksanddata.arcane.framework
package services.lakehouse

import models.ArcaneType.{IntType, StringType}
import models.{DataCell, Field, MergeKeyField}
import services.lakehouse.base.IcebergCatalogSettings

import org.scalatest.*
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.*

import java.util.UUID
import scala.concurrent.Future
import scala.jdk.CollectionConverters.*
import scala.language.postfixOps
import services.lakehouse.SchemaConversions.*

class IcebergS3CatalogWriterTests extends flatspec.AsyncFlatSpec with Matchers:
  private implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global
  private val settings = new IcebergCatalogSettings:
    override val namespace = "test"
    override val warehouse = "polaris"
    override val catalogUri = "http://localhost:8181/api/catalog"
    override val additionalProperties: Map[String, String] = IcebergCatalogCredential.oAuth2Properties
    override val s3CatalogFileIO: S3CatalogFileIO = S3CatalogFileIO
    override val stagingLocation: Option[String] = Some("s3://tmp/polaris/test")

  private val schema = Seq(MergeKeyField, Field(name = "colA", fieldType = IntType), Field(name = "colB", fieldType = StringType))
  private val icebergWriter = IcebergS3CatalogWriter(settings)

  it should "create a table when provided schema and rows" in {
    val rows = Seq(
      List(DataCell(name = MergeKeyField.name, Type = MergeKeyField.fieldType, value = "key1"), DataCell(name = "colA", Type = IntType, value = 1), DataCell(name = "colB", Type = StringType, value = "abc")),
      List(DataCell(name = MergeKeyField.name, Type = MergeKeyField.fieldType, value = "key2"), DataCell(name = "colA", Type = IntType, value = 2), DataCell(name = "colB", Type = StringType, value = "def")),
      List(DataCell(name = MergeKeyField.name, Type = MergeKeyField.fieldType, value = "key3"), DataCell(name = "colA", Type = IntType, value = 2), DataCell(name = "colB", Type = StringType, value = "iop")),
      List(DataCell(name = MergeKeyField.name, Type = MergeKeyField.fieldType, value = "key4"), DataCell(name = "colA", Type = IntType, value = 3), DataCell(name = "colB", Type = StringType, value = "tyr"))
    )

    icebergWriter.write(
      data = rows,
      name = UUID.randomUUID.toString,
      schema = schema
    ).map(tbl => tbl.history().asScala.isEmpty should equal(false))
  }

  it should "create an empty table" in {
    icebergWriter.write(
      data = Seq(),
      name = UUID.randomUUID.toString,
      schema = schema
    ).map(tbl => tbl.history().asScala.isEmpty should equal(false))
  }

  it should "delete table successfully after creating it" in {
    val tblName = UUID.randomUUID.toString
    icebergWriter.write(
      data = Seq(List(
        DataCell(name = MergeKeyField.name, Type = MergeKeyField.fieldType, value = "key1"), DataCell(name = "colA", Type = IntType, value = 1), DataCell(name = "colB", Type = StringType, value = "abc"),
      )),
      name = tblName,
      schema = schema
    ).flatMap { _ => icebergWriter.delete(tblName) }.map {
      _ should equal(true)
    }
  }

  it should "create a table and then append rows to it" in {
    val tblName = UUID.randomUUID.toString
    val initialData = Seq(List(
      DataCell(name = MergeKeyField.name, Type = MergeKeyField.fieldType, value = "key1"), DataCell(name = "colA", Type = IntType, value = 1), DataCell(name = "colB", Type = StringType, value = "abc"),
    ))
    val appendData = Seq(List(
      DataCell(name = MergeKeyField.name, Type = MergeKeyField.fieldType, value = "key2"), DataCell(name = "colA", Type = IntType, value = 2), DataCell(name = "colB", Type = StringType, value = "def"),
    ))
    icebergWriter.write(
      data = initialData,
      name = tblName,
      schema = schema
    ).flatMap { _ => icebergWriter.append(appendData, tblName, schema = schema) }.map {
      // expect 2 data transactions: append initialData, append appendData
      // table creation has no data so no data snapshot there
      _.currentSnapshot().sequenceNumber() should equal(2)
    }
  }
