package com.sneaksanddata.arcane.framework
package models.settings.sources.pullstream

import upickle.ReadWriter

case class DefaultPullStreamSourceSettings(
    override val pullIndexKey: String,
    override val pullIndexValue: String,
    override val watermarkFieldName: String,
    override val pageSize: Option[Int],
    override val region: String,
    override val tableName: String,
    override val endpoint: Option[String],
    override val jsonPointerExpression: Option[String],
    override val jsonArrayPointers: Map[String, Map[String, String]]
) extends PullStreamSourceSettings derives ReadWriter
