package com.sneaksanddata.arcane.framework
package services.base

import models.schemas.ArcaneSchema

trait StreamingSource extends SchemaProvider[ArcaneSchema] with ShardProvider
