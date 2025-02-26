package com.sneaksanddata.arcane.framework
package services.cdm

import models.ArcaneSchema
import models.cdm.{SimpleCdmEntity, SimpleCdmModel, given_Conversion_SimpleCdmEntity_ArcaneSchema}
import services.base.SchemaProvider
import services.cdm.CdmTableSettings
import services.mssql.given_CanAdd_ArcaneSchema
import services.storage.services.AzureBlobStorageReader

import zio.{Task, ZIO, ZLayer}

import scala.concurrent.Future

/**
 * A provider of a schema for a data produced by Microsoft Synapse Link.
 *
 * @param azureBlobStorageReader The reader for the Azure Blob Storage.
 * @param tableLocation The location of the table.
 * @param tableName The name of the table.
 */
class CdmSchemaProvider(azureBlobStorageReader: AzureBlobStorageReader, tableLocation: String, tableName: String)
  extends SchemaProvider[ArcaneSchema]:

  implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global

  override lazy val getSchema: Task[SchemaType] = getEntity.map(toArcaneSchema)

  def getEntity: Task[SimpleCdmEntity] =
    SimpleCdmModel(tableLocation, azureBlobStorageReader).flatMap(_.entities.find(_.name == tableName) match
      case None => ZIO.fail(new Exception(s"Table $tableName not found in model $tableLocation"))
      case Some(entity) => ZIO.succeed(entity)
    )

  override def empty: SchemaType = ArcaneSchema.empty()
  
  private def toArcaneSchema(simpleCdmModel: SimpleCdmEntity): ArcaneSchema = simpleCdmModel

object CdmSchemaProvider:
  
  private type Environment = AzureBlobStorageReader & CdmTableSettings
  
  val layer: ZLayer[Environment, Nothing, CdmSchemaProvider] = 
      ZLayer {
        for
          context <- ZIO.service[CdmTableSettings]
          settings <- ZIO.service[AzureBlobStorageReader]
        yield CdmSchemaProvider(settings, context.rootPath, context.name)
      }
