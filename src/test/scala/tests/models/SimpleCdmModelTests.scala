package com.sneaksanddata.arcane.framework
package tests.models

import models.cdm.{SimpleCdmModel, given_Conversion_SimpleCdmEntity_ArcaneSchema}
import models.schemas.ArcaneType.{BooleanType, DateTimeOffsetType, DoubleType, LongType, StringType, TimestampType}
import models.schemas.{ArcaneSchema, IndexedField, MergeKeyField}

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.should
import upickle.default.*

import scala.io.Source
import scala.util.Using

class SimpleCdmModelTests extends AnyFlatSpec with Matchers {
  it should "deserialize the model.json file correctly" in {
    val serialized = Using(Source.fromURL(getClass.getResource("/cdm_model.json"))) {
      _.getLines().mkString("\n")
    }.get

    val result = read[SimpleCdmModel](serialized)

    (
      result.entities.size should be(3),
      result.entities.head.attributes.size should be(26),
      result.entities(1).attributes.size should be(12),
      result.entities(2).attributes.size should be(16)
    )
  }

  it should "convert a SimpleCdmEntity to an indexed source schema" in {
    val serialized = Using(Source.fromURL(getClass.getResource("/cdm_model.json"))) {
      _.getLines().mkString("\n")
    }.get

    val entity                         = read[SimpleCdmModel](serialized).entities.head
    val schema: ArcaneSchema           = entity
    val indexedFields: Seq[IndexedField] = schema.collect { case field: IndexedField => field }

    schema.map(_.name) should equal(entity.attributes.map(_.name))
    indexedFields.map(_.fieldId) should equal(entity.attributes.indices)
    schema.exists(_.name == MergeKeyField.name) should be(false)

    schema.find(_.name == "Id").map(_.fieldType) should contain(StringType)
    schema.find(_.name == "iseuro").map(_.fieldType) should contain(LongType)
    schema.find(_.name == "roundingprecision").map(_.fieldType) should contain(DoubleType)
    schema.find(_.name == "modifieddatetime").map(_.fieldType) should contain(TimestampType)
    schema.find(_.name == "createdon").map(_.fieldType) should contain(DateTimeOffsetType)
    schema.find(_.name == "IsDelete").map(_.fieldType) should contain(BooleanType)
  }
}
