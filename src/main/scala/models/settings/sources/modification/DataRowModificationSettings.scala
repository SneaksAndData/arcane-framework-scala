package com.sneaksanddata.arcane.framework
package models.settings.sources.modification

import models.settings.{CompositeSetting, Mergeable}
import models.settings.sources.*

import upickle.default.*
import upickle.implicits.key

import java.time.OffsetDateTime

/** A modification applied to source data rows and their corresponding schema.
  */
sealed trait DataRowModification

/** Merge key with preset key field names. For internal usage only.
  */
case class FrozenSurrogateMergeKey(fieldNames: Set[String]) extends DataRowModification

/** Version mod with preset field name. For internal usage only.
  */
case class FrozenSurrogateVersion(fieldName: String) extends DataRowModification

/** Timestamp mod with a preset timestamp. for internal usage only.
  */
case class FrozenSurrogateTimestamp(timestamp: OffsetDateTime) extends DataRowModification

/** Adds the time at which Arcane loaded a batch to its schema and data rows.
  */
case class SurrogateTimestamp() derives ReadWriter

/** ADT composed with settings for the load-timestamp modification.
  */
case class SurrogateTimestampImpl(surrogateTimestamp: SurrogateTimestamp) extends DataRowModification

sealed trait FieldSelector extends DataRowModification

/** Field selector which limits the resulting DataRow/Schema to only contain fields from an include list.
  */
case class IncludeFieldSelector(fields: Set[String]) derives ReadWriter

/** ADT composed with settings for the IncludeFieldSelector.
  */
case class IncludeFieldSelectorImpl(include: IncludeFieldSelector) extends FieldSelector

/** Field selector which removes blacklisted fields from the resulting DataRow/Schema.
  */
case class ExcludeFieldSelector(fields: Set[String]) derives ReadWriter

/** ADT composed with settings for the ExcludeFieldSelector.
  */
case class ExcludeFieldSelectorImpl(exclude: ExcludeFieldSelector) extends FieldSelector

/** Fields selector that signals to DRM API that no further operations are necessary
  */
case class FrozenFieldSelector(fields: Seq[String]) extends DataRowModification

/** Field selector modification excludes provided fields or only selects fields from an include list.
  */
case class FieldSelectorSetting(
    include: Option[IncludeFieldSelector] = None,
    exclude: Option[ExcludeFieldSelector] = None
) extends CompositeSetting[FieldSelector] derives ReadWriter:
  override def resolve: FieldSelector =
    if include.isDefined then IncludeFieldSelectorImpl(include.get)
    if exclude.isDefined then ExcludeFieldSelectorImpl(exclude.get)

    throw new RuntimeException("Invalid fieldSelector setting: neither `include`, nor `exclude` sections are defined.")

case class SurrogateTimestampSetting() extends CompositeSetting[DataRowModification] derives ReadWriter:
  override def resolve: DataRowModification = SurrogateTimestampImpl(SurrogateTimestamp())

case class SupportedModifications(
    fieldSelector: Option[FieldSelectorSetting] = None,
    surrogateTimestamp: Option[SurrogateTimestampSetting] = None
) derives ReadWriter

object NoModifications extends SupportedModifications(None, None)

/** Settings for modifications applied to source data rows and their corresponding schemas.
  */
trait DataRowModificationSettings extends Mergeable:
  /** Data-row modifications to apply, in their configured order.
    */
  val modifications: Seq[DataRowModification]

/** Default serializable implementation of [[DataRowModificationSettings]].
  *
  * An empty `modifications` array disables schema modification.
  *
  * @param modificationSettings
  *   serialized modification entries to resolve and apply in order
  */
case class DefaultDataRowModificationSettings(
    @key("modifications") modificationSettings: SupportedModifications
) extends DataRowModificationSettings,
      Mergeable derives ReadWriter:

  /** Resolved internal modification definitions.
    */
  override val modifications: Seq[DataRowModification] = Seq(
    modificationSettings.fieldSelector.map(_.resolve),
    modificationSettings.surrogateTimestamp.map(_.resolve)
  ).collect { case Some(v) =>
    v
  }

  override type MergeableFrom = OverrideDataRowModificationSettings
  override type MergeResult   = DefaultDataRowModificationSettings

  override def merge(overrides: Option[MergeableFrom]): MergeResult =
    DefaultDataRowModificationSettings(
      modificationSettings = overrides.flatMap(_.modificationSettings).getOrElse(this.modificationSettings)
    )
