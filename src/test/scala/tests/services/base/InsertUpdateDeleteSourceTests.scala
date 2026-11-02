package com.sneaksanddata.arcane.framework
package tests.services.base

import models.schemas.ArcaneType.{LongType, StringType}
import models.schemas.*
import models.settings.sources.modification.*
import services.base.InsertUpdateDeleteSource

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import zio.{Task, ZIO}

class InsertUpdateDeleteSourceTests extends AnyFlatSpec with Matchers:

  private class TestInsertUpdateDeleteSource(mods: Seq[DataRowModification]) extends InsertUpdateDeleteSource(mods):
    override type ShardMetadata = String
    override type WatermarkType = String

    override protected def getPrimaryKey: Task[FrozenSurrogateMergeKey] =
      ZIO.succeed(FrozenSurrogateMergeKey(Set("id")))

    override protected val getVersionField: Task[FrozenSurrogateVersion] =
      ZIO.succeed(FrozenSurrogateVersion("version"))

    override protected def getSourceSchema: Task[ArcaneSchema] =
      ZIO.succeed(
        Seq(
          Field("id", StringType),
          Field("version", LongType),
          Field("payload", StringType),
          MergeKeyField,
          VersionField
        )
      )

    override def getShards(rangeStart: WatermarkType, rangeEnd: WatermarkType) =
      zio.stream.ZStream.empty

    override def deleteShards(prefix: String): Task[Unit] = ZIO.unit

    override def empty: ArcaneSchema = Seq.empty

    def testApplyFieldSelector[T](
        target: Seq[T],
        comparator: T => String,
        fieldSelector: FieldSelector
    ): Seq[T] =
      applyFieldSelector(target, comparator, fieldSelector)

  it should "retain essential merge key and version fields when an include rule is applied" in {
    val source = new TestInsertUpdateDeleteSource(Seq.empty)

    val schema = Seq(
      Field("id", StringType),
      Field("version", LongType),
      Field("payload", StringType),
      MergeKeyField,
      VersionField
    )

    val selector = IncludeFieldSelectorImpl(IncludeFieldSelector(Set("payload")))
    val filtered = source.testApplyFieldSelector(schema, _.name, selector)

    filtered.map(_.name) should contain theSameElementsAs Seq(
      "payload",
      MergeKeyField.name,
      VersionField.name
    )
  }

  it should "prevent essential merge key and version fields from being excluded when an exclude rule is applied" in {
    val source = new TestInsertUpdateDeleteSource(Seq.empty)

    val schema = Seq(
      Field("id", StringType),
      Field("version", LongType),
      Field("payload", StringType),
      MergeKeyField,
      VersionField
    )

    val selector = ExcludeFieldSelectorImpl(
      ExcludeFieldSelector(
        Set(
          "payload",
          MergeKeyField.name.toLowerCase,
          VersionField.name.toLowerCase
        )
      )
    )
    val filtered = source.testApplyFieldSelector(schema, _.name, selector)

    filtered.map(_.name) should contain theSameElementsAs Seq(
      "id",
      "version",
      MergeKeyField.name,
      VersionField.name
    )
  }
