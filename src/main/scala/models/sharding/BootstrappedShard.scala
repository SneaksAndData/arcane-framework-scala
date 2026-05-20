package com.sneaksanddata.arcane.framework
package models.sharding

import services.streaming.base.StructuredZStream

/** A shard of source data that has been successfully bootstrapped and is ready for staging
  */
trait BootstrappedShard extends SourceShard:
  val shardStream: StructuredZStream
  val shardSourceEntityName: String
