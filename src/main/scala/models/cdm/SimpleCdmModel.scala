package com.sneaksanddata.arcane.framework
package models.cdm

import models.*
import models.schemas.*

import upickle.default.*

import scala.language.implicitConversions

/** Attribute in Microsoft Common Data Model, simplified compared to native SDK
  * @param name
  *   Attribute name
  * @param dataType
  *   String literal for the attribute data type
  * @param maxLength
  *   max length property - not used
  */
case class SimpleCdmAttribute(name: String, dataType: String, maxLength: Int) derives ReadWriter

/** Entity (Table) in Microsoft Common Data Model, simplified compared to native SDK
  * @param entityType
  *   CDM entity type
  * @param name
  *   Entity name
  * @param description
  *   Docstring for the entity
  * @param attributes
  *   Entity fields
  */
case class SimpleCdmEntity(
    @upickle.implicits.key("$type")
    entityType: String,
    name: String,
    description: String,
    attributes: Seq[SimpleCdmAttribute]
) derives ReadWriter

/** Synapse Link container model, containing all entities enabled for the export
  * @param name
  *   Model name
  * @param description
  *   Docstring for the model
  * @param version
  *   Model version
  * @param entities
  *   Included entities
  */
case class SimpleCdmModel(name: String, description: String, version: String, entities: Seq[SimpleCdmEntity])
    derives ReadWriter

given Conversion[SimpleCdmAttribute, ArcaneSchemaField] with
  override def apply(entity: SimpleCdmAttribute): ArcaneSchemaField = entity.dataType match
    case "guid"           => Field(name = entity.name, fieldType = ArcaneType.StringType)
    case "string"         => Field(name = entity.name, fieldType = ArcaneType.StringType)
    case "int64"          => Field(name = entity.name, fieldType = ArcaneType.LongType)
    case "decimal"        => Field(name = entity.name, fieldType = ArcaneType.DoubleType)
    case "dateTime"       => Field(name = entity.name, fieldType = ArcaneType.TimestampType)
    case "dateTimeOffset" => Field(name = entity.name, fieldType = ArcaneType.DateTimeOffsetType)
    case "boolean"        => Field(name = entity.name, fieldType = ArcaneType.BooleanType)
    case _                => Field(name = entity.name, fieldType = ArcaneType.StringType)

given Conversion[SimpleCdmEntity, ArcaneSchema] with
  override def apply(entity: SimpleCdmEntity): ArcaneSchema = entity.attributes.map(implicitly) :+ MergeKeyField

object SimpleCdmModel:
  // number of fields in the schema of each entity which do not originate from CDM
  // currently MergeKeyField only
  val systemFieldCount: Int = 1

  def apply(json: String): SimpleCdmModel = read[SimpleCdmModel](json)
