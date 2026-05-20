package com.sneaksanddata.arcane.framework
package models.backfill

import upickle.ReadWriter

/** Backfill data summary recorded into target
  */
trait SourceBackfill:
  val id: String
  val shardCount: Int
  val watermarkValue: String

case class DefaultSourceBackfill(
    override val id: String,
    override val shardCount: Int,
    override val watermarkValue: String
) extends SourceBackfill derives ReadWriter
