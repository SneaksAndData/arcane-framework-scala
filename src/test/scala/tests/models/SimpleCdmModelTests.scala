package com.sneaksanddata.arcane.framework
package tests.models

import models.ArcaneSchema
import models.cdm.{SimpleCdmModel, given_Conversion_SimpleCdmEntity_ArcaneSchema}

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
      result.entities.head.attributes.size should be (26),
      result.entities(1).attributes.size should be(12),
      result.entities(2).attributes.size should be(16)
    )
  }

  it should "generate valid ArcaneSchema" in {
    val serialized = Using(Source.fromURL(getClass.getResource("/cdm_model.json"))) {
      _.getLines().mkString("\n")
    }.get

    val result: Seq[ArcaneSchema] = read[SimpleCdmModel](serialized).entities.map(implicitly)

    noException should be thrownBy result.map(schema => schema.mergeKey).toList
  }
}
