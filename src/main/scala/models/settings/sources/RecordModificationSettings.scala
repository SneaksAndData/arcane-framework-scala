package com.sneaksanddata.arcane.framework
package models.settings.sources

import upickle.default.*
import upickle.implicits.key

/** A modification applied to source records, including their schema and data rows.
  */
sealed trait RecordModification

/** Adds an Arcane-generated merge key to the source schema and data rows.
  */
case class SurrogateMergeKey() derives ReadWriter

/** ADT composed with settings for the surrogate merge-key modification.
  */
case class SurrogateMergeKeyImpl(surrogateMergeKey: SurrogateMergeKey) extends RecordModification

/** Adds an Arcane-generated version to the source schema and data rows.
  */
case class SurrogateVersion() derives ReadWriter

/** ADT composed with settings for the surrogate-version modification.
  */
case class SurrogateVersionImpl(surrogateVersion: SurrogateVersion) extends RecordModification

/** Adds the time at which Arcane loaded a batch to its schema and data rows.
  */
case class LoadTimestamp() derives ReadWriter

/** ADT composed with settings for the load-timestamp modification.
  */
case class LoadTimestampImpl(loadTimestamp: LoadTimestamp) extends RecordModification

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
case class FieldSelectorImpl(fieldSelector: FieldSelector) extends RecordModification

/** Serializable representation of one schema modification.
  *
  * This proxy class allows the supported modification settings to be deserialized without serializing the internal
  * [[RecordModification]] ADT directly. Exactly one modification must be configured in each entry. Multiple
  * modifications are expressed as separate entries in [[DefaultRecordModificationSettings.modificationSettings]].
  *
  * @param surrogateMergeKey
  *   settings for adding a surrogate merge key
  * @param surrogateVersion
  *   settings for adding a surrogate version
  * @param loadTimestamp
  *   settings for adding a batch load timestamp
  * @param fieldSelector
  *   settings for selecting fields
  */
case class RecordModificationSetting(
    surrogateMergeKey: Option[SurrogateMergeKey] = None,
    surrogateVersion: Option[SurrogateVersion] = None,
    loadTimestamp: Option[LoadTimestamp] = None,
    fieldSelector: Option[FieldSelector] = None
) derives ReadWriter:

  /** Resolves this serialized settings entry into its internal schema-modification representation.
    *
    * @throws IllegalArgumentException
    *   when the entry contains either no modification or more than one modification
    */
  def resolveSetting: RecordModification =
    val configured = Seq(
      surrogateMergeKey.map(SurrogateMergeKeyImpl(_)),
      surrogateVersion.map(SurrogateVersionImpl(_)),
      loadTimestamp.map(LoadTimestampImpl(_)),
      fieldSelector.map(FieldSelectorImpl(_))
    ).flatten

    require(
      configured.size == 1,
      s"Exactly one schema modification must be configured, but found ${configured.size}"
    )

    configured.head

/** Settings for modifications applied to source records, including their schemas and data rows.
  */
trait RecordModificationSettings:
  /** Record modifications to apply, in their configured order.
    */
  val modifications: Seq[RecordModification]

/** Default serializable implementation of [[RecordModificationSettings]].
  *
  * An empty `modifications` array disables schema modification.
  *
  * @param modificationSettings
  *   serialized modification entries to resolve and apply in order
  */
case class DefaultRecordModificationSettings(
    @key("modifications") modificationSettings: Seq[RecordModificationSetting] = Seq.empty
) extends RecordModificationSettings derives ReadWriter:
  /** Resolved internal modification definitions.
    */
  override val modifications: Seq[RecordModification] = modificationSettings.map(_.resolveSetting)
