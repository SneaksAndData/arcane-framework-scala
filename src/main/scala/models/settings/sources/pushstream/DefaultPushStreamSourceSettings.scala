package com.sneaksanddata.arcane.framework
package models.settings.sources.pushstream

import upickle.ReadWriter

case class DefaultPushStreamSourceSettings(
    override val sourceTableName: String,
    override val targetTableName: String,
    override val primaryKeyFieldName: String,
    override val primaryKeyValue: String,
    override val watermarkFieldName: String,
    override val region: String,
    override val tableName: String,
    override val endpoint: Option[String]
) extends PushStreamSourceSettings derives ReadWriter
