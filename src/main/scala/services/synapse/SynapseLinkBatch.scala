package com.sneaksanddata.arcane.framework
package services.synapse

import models.{ArcaneSchema, DataRow}

import services.storage.models.base.StoredBlob

case class SchemaEnrichedBlob(blob: StoredBlob, schema: ArcaneSchema)

case class SchemaEnrichedContent(content: String, schema: ArcaneSchema)

/**
 * Batch type for Synapse Link is a list of DataRow elements
 */
type SynapseLinkBatch = DataRow

/**
 * Versioned batch type for Synapse Link is a list of Datarow elements paired with a date folder name (YYYY-MM-DDTHH.mm.SSZ)
 */
type SynapseLinkVersionedBatch = (DataRow, String)
