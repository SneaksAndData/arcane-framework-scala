package com.sneaksanddata.arcane.framework
package services.base

import models.schemas.ArcaneSchema
import services.streaming.base.{JsonWatermark, SourceWatermark}

trait StreamingSource extends SchemaProvider[ArcaneSchema] with ShardProvider
// TODO:  final override type SchemaType = ArcaneSchema
