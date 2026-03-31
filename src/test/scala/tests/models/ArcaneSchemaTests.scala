package com.sneaksanddata.arcane.framework
package tests.models

import models.schemas.ArcaneType.{BigDecimalType, IntType, ListType, StringType, StructType}
import models.schemas.{ArcaneSchema, IndexedField, IndexedMergeKeyField, MergeKeyField}

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
        (ListType(StringType, 3), ListType(StringType, 2), true),
        (
          ListType(
            StructType(ArcaneSchema(Seq(IndexedField("colA", StringType, 1), IndexedField("colB", IntType, 2)))),
            1
          ),
          ListType(
            StructType(ArcaneSchema(Seq(IndexedField("colB", IntType, 2), IndexedField("colA", StringType, 1)))),
            2
          ),
          true
        )
      )
    ) { case (typeA, typeB, expectedResult) =>
      (typeA == typeB) should be(expectedResult)
    }
  }

  it should "get missing fields correctly" in {
    forAll(
      Seq(
        (
          ArcaneSchema(Seq(IndexedField("colA", StringType, 1))),
          ArcaneSchema.empty(),
          Seq(IndexedField("colA", StringType, 1))
        ),
        (
          ArcaneSchema(Seq(IndexedField("colA", StringType, 1), IndexedField("colB", IntType, 2))),
          ArcaneSchema(Seq(IndexedField("colA", StringType, 1))),
          Seq(IndexedField("colB", IntType, 2))
        ),
        (
          ArcaneSchema(Seq(MergeKeyField)),
          ArcaneSchema(Seq(IndexedField("colA", StringType, 1), IndexedField("colB", IntType, 2))),
          Seq(MergeKeyField)
        ),
        (
          ArcaneSchema(Seq(IndexedMergeKeyField(0), IndexedField("colA", StringType, 1))),
          ArcaneSchema(
            Seq(IndexedMergeKeyField(1), IndexedField("colA", StringType, 2), IndexedField("colB", IntType, 3))
          ),
          Seq()
        ),
        (
          ArcaneSchema(
            Seq(IndexedMergeKeyField(1), IndexedField("colA", StringType, 2), IndexedField("colB", IntType, 3))
          ),
          ArcaneSchema(Seq(IndexedMergeKeyField(0), IndexedField("colA", StringType, 1))),
          Seq(IndexedField("colB", IntType, 3))
        ),
        (
          ArcaneSchema(
            Seq(
              IndexedMergeKeyField(1),
              IndexedField(
                "colA",
                StructType(
                  ArcaneSchema(Seq(IndexedField("nestedColA", StringType, 2), IndexedField("nestedColB", IntType, 3)))
                ),
                4
              ),
              IndexedField("colB", IntType, 5)
            )
          ),
          ArcaneSchema(
            Seq(
              IndexedMergeKeyField(1),
              IndexedField(
                "colA",
                StructType(
                  ArcaneSchema(Seq(IndexedField("nestedColB", IntType, 2), IndexedField("nestedColA", StringType, 3)))
                ),
                4
              ),
              IndexedField("colB", IntType, 5)
            )
          ),
          Seq()
        ),
        (
          ArcaneSchema(
            Seq(
              IndexedMergeKeyField(1),
              IndexedField(
                "colA",
                StructType(
                  ArcaneSchema(
                    Seq(
                      IndexedField("nestedColA", StringType, 2),
                      IndexedField("nestedColB", IntType, 3),
                      IndexedField("nestedColC", IntType, 4)
                    )
                  )
                ),
                5
              ),
              IndexedField("colB", IntType, 6)
            )
          ),
          ArcaneSchema(
            Seq(
              IndexedMergeKeyField(1),
              IndexedField(
                "colA",
                StructType(
                  ArcaneSchema(Seq(IndexedField("nestedColA", StringType, 2), IndexedField("nestedColB", IntType, 3)))
                ),
                4
              ),
              IndexedField("colB", IntType, 5)
            )
          ),
          Seq(
            IndexedField(
              "colA",
              StructType(
                ArcaneSchema(
                  Seq(
                    IndexedField("nestedColA", StringType, 2),
                    IndexedField("nestedColB", IntType, 3),
                    IndexedField("nestedColC", IntType, 4)
                  )
                )
              ),
              5
            )
          )
        )
      )
    ) { case (schemaA, schemaB, expected) =>
      schemaB.getMissingFields(schemaA) should be(expected)
    }
  }
}
