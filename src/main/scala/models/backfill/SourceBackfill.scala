package com.sneaksanddata.arcane.framework
package models.backfill

import upickle.ReadWriter

/** Backfill data summary recorded into target
  */
trait SourceBackfill:
  val id: String
  val watermarkValue: String
  val backfillStart: String
  val backfillEnd: String
  val shardSources: Seq[String]

case class DefaultSourceBackfill(
    override val id: String,
    override val backfillStart: String,
    override val backfillEnd: String,
    override val watermarkValue: String,
    override val shardSources: Seq[String]
) extends SourceBackfill derives ReadWriter
