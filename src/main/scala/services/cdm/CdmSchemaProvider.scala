package com.sneaksanddata.arcane.framework
package services.cdm

import models.ArcaneSchema
import models.cdm.{SimpleCdmEntity, SimpleCdmModel, given_Conversion_SimpleCdmEntity_ArcaneSchema}
import services.base.SchemaProvider
import services.mssql.given_CanAdd_ArcaneSchema
import services.storage.models.azure.AdlsStoragePath
import services.storage.services.AzureBlobStorageReader

import zio.stream.ZStream
import zio.{Task, ZIO, ZLayer}

import java.io.IOException

/**
 * A provider of a schema for a data produced by Microsoft Synapse Link.
 *
 * @param azureBlobStorageReader The reader for the Azure Blob Storage.
 * @param tableLocation          The location of the table.
 * @param tableName              The name of the table.
 */
class CdmSchemaProvider(azureBlobStorageReader: AzureBlobStorageReader, tableLocation: String, tableName: String)
  extends SchemaProvider[ArcaneSchema]:

  /**
   * @inheritdoc
   */
  override lazy val getSchema: Task[SchemaType] = getEntity.map(toArcaneSchema)

  private def getEntity: Task[SimpleCdmEntity] =
    for modelPath <- ZIO.fromTry(AdlsStoragePath(tableLocation).map(_ + "model.json"))
        reader = ZIO.fromAutoCloseable(azureBlobStorageReader.streamBlobContent(modelPath)).refineToOrDie[IOException]
        stream = ZStream.fromReaderScoped(reader)
        json <- stream.runCollect.map(_.mkString)
        model = SimpleCdmModel(json)
    yield model.entities.find(_.name == tableName).getOrElse(throw new Exception(s"Table $tableName not found in model $tableLocation"))

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
