package com.sneaksanddata.arcane.framework
package services.synapse

import models.schemas.{ArcaneSchema, DataRow}
import services.storage.models.base.StoredBlob

case class SchemaEnrichedBlob(blob: StoredBlob, schema: ArcaneSchema, latestVersion: String)

case class SchemaEnrichedContent(content: String, schema: ArcaneSchema, latestVersion: String)

/**
 * Batch type for Synapse Link is a list of DataRow elements
 */
type SynapseLinkBatch = DataRow
type SynapseLinkVersionedBatch = (DataRow, String)
