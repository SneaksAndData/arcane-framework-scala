package com.sneaksanddata.arcane.framework
package models.schemas

/** A schema that is guaranteed to have an IndexedMergeKeyField or a merge-key field. Refer to ArcaneSchema.Conversion.
  *
  * Use [[MergeableArcaneSchema.apply]] to construct one explicitly, or rely on the iceberg-side conversion
  * `given Conversion[org.apache.iceberg.Schema, MergeableArcaneSchema]` in
  * `services.iceberg.SchemaConversions`, which re-tags the merge-key column.
  *
  * `MergeableArcaneSchema <: ArcaneSchema`, so it can be passed to any API expecting a plain `ArcaneSchema`.
  */
final class MergeableArcaneSchema private (fields: Seq[ArcaneSchemaField]) extends ArcaneSchema(fields)

object MergeableArcaneSchema:
  def apply(fields: Seq[ArcaneSchemaField]): MergeableArcaneSchema =
    require(
      fields.exists {
        case MergeKeyField           => true
        case IndexedMergeKeyField(_) => true
        case _                       => false
      },
      "MergeableArcaneSchema requires a MergeKeyField or IndexedMergeKeyField"
    )
    new MergeableArcaneSchema(fields)
