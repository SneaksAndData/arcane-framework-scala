package com.sneaksanddata.arcane.framework
package services.synapse

import models.DataRow

/**
 * Batch type for Synapse Link is a list of DataRow elements
 */
type SynapseLinkBatch = Seq[DataRow]

/**
 * Versioned batch type for Synapse Link is a list of Datarow elements paired with a date folder name (YYYY-MM-DDTHH.mm.SSZ)
 */
type SynapseLinkVersionedBatch = (Seq[DataRow], String)
