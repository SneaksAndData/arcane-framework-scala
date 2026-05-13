package com.sneaksanddata.arcane.framework
package services.synapse

import models.schemas.{ArcaneSchema, DataRow}
import services.storage.models.base.StoredBlob

case class SchemaEnrichedBlob(blob: StoredBlob, schema: ArcaneSchema)

case class SchemaEnrichedContent(content: String, schema: ArcaneSchema)
