package com.sneaksanddata.arcane.framework
package models.schemas

/** A schema that is guaranteed to have an IndexedMergeKeyField or a merge-key field. Refer to ArcaneSchema.Conversion.
  */
opaque type MergeableArcaneSchema <: ArcaneSchema = ArcaneSchema

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
    ArcaneSchema(fields)
