package com.sneaksanddata.arcane.framework
package tests.models

import com.sneaksanddata.arcane.framework.models.schemas.ArcaneType.{BigDecimalType, IntType, ListType, StringType}
import org.scalatest.Inspectors.forAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.should

class ArcaneSchemaTests extends AnyFlatSpec with Matchers {
  it should "compare types correctly" in {
    forAll(
      Seq(
        (StringType, StringType, true),
        (StringType, IntType, false),
        (BigDecimalType(16, 3), BigDecimalType(16, 3), true),
        (BigDecimalType(16, 3), BigDecimalType(16, 2), false),
        (ListType(StringType, 3), ListType(StringType, 2), true)
      )
    ) { case (typeA, typeB, expectedResult) =>
      (typeA == typeB) should be(expectedResult)
    }
  }
}
