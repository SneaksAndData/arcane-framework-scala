package com.sneaksanddata.arcane.framework
package models.settings.sources.modification

import models.settings.sources.*

import upickle.default.*
import upickle.implicits.key

/** A modification applied to source data rows and their corresponding schema.
  */
sealed trait DataRowModification

/** Merge key with preset key field names. For internal usage only.
  */
case class FrozenSurrogateMergeKey(fieldNames: Set[String]) extends DataRowModification

/** Version mod with preset field name. For internal usage only.
  */
case class FrozenSurrogateVersion(fieldName: String) extends DataRowModification

/** Adds the time at which Arcane loaded a batch to its schema and data rows.
  */
case class LoadTimestamp() derives ReadWriter

/** ADT composed with settings for the load-timestamp modification.
  */
case class LoadTimestampImpl(loadTimestamp: LoadTimestamp) extends DataRowModification

/** Selects the fields included in the modified schema and data rows.
  *
  * @param includeFields
  *   fields to include
  * @param excludeFields
  *   fields to exclude
  */
case class FieldSelector(
    includeFields: Seq[String] = Seq.empty,
    excludeFields: Seq[String] = Seq.empty
) derives ReadWriter

/** ADT composed with settings for the field-selection modification.
  */
case class FieldSelectorImpl(fieldSelector: FieldSelector) extends DataRowModification

/** Serializable representation of one schema modification.
  *
  * This proxy class allows the supported modification settings to be deserialized without serializing the internal
  * [[DataRowModification]] ADT directly. Exactly one modification must be configured in each entry. Multiple
  * modifications are expressed as separate entries in [[DefaultDataRowModificationSettings.modificationSettings]].
  *
  * @param loadTimestamp
  *   settings for adding a batch load timestamp
  * @param fieldSelector
  *   settings for selecting fields
  */
case class DataRowModificationSetting(
    loadTimestamp: Option[LoadTimestamp] = None,
    fieldSelector: Option[FieldSelector] = None
) derives ReadWriter:

  /** Resolves this serialized settings entry into its internal schema-modification representation.
    *
    * @throws IllegalArgumentException
    *   when the entry contains either no modification or more than one modification
    */
  def resolveSetting: DataRowModification =
    val configured = Seq(
      loadTimestamp.map(LoadTimestampImpl(_)),
      fieldSelector.map(FieldSelectorImpl(_))
    ).flatten

    require(
      configured.size == 1,
      s"Exactly one schema modification must be configured, but found ${configured.size}"
    )

    configured.head

/** Settings for modifications applied to source data rows and their corresponding schemas.
  */
trait DataRowModificationSettings:
  /** Data-row modifications to apply, in their configured order.
    */
  val modifications: Seq[DataRowModification]

/** Default serializable implementation of [[DefaultDataRowModificationSettings]].
  *
  * An empty `modifications` array disables schema modification.
  *
  * @param modificationSettings
  *   serialized modification entries to resolve and apply in order
  */
case class DefaultDataRowModificationSettings(
    @key("modifications") modificationSettings: Seq[DataRowModificationSetting]
) extends DataRowModificationSettings derives ReadWriter:
  /** Resolved internal modification definitions.
    */
  override val modifications: Seq[DataRowModification] = modificationSettings.map(_.resolveSetting)
