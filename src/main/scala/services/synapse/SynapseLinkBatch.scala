package com.sneaksanddata.arcane.framework
package services.synapse

import models.schemas.{ArcaneSchema, DataRow}
import services.storage.models.base.StoredBlob

import java.time.OffsetDateTime

case class SchemaEnrichedBlob(blob: StoredBlob, schema: ArcaneSchema)

case class SchemaEnrichedContent(content: String, schema: ArcaneSchema)

case class SynapseBatchVersion(versionNumber: String, waterMarkTime: OffsetDateTime, blob: StoredBlob)

/** Batch type for Synapse Link is a list of DataRow elements
  */
type SynapseLinkBatch = DataRow
