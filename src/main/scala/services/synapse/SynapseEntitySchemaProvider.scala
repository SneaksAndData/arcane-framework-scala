package com.sneaksanddata.arcane.framework
package services.synapse

import models.cdm.{SimpleCdmEntity, SimpleCdmModel, given_Conversion_SimpleCdmEntity_ArcaneSchema}
import models.schemas.{ArcaneSchema, given_CanAdd_ArcaneSchema}
import services.base.SchemaProvider
import services.storage.base.BlobStorageReader
import services.storage.models.azure.AdlsStoragePath
import services.storage.services.azure.AzureBlobStorageReader

import zio.{Task, ZIO, ZLayer}

/** A provider of a schema for a data produced by Microsoft Synapse Link.
  *
  * @param azureBlobStorageReader
  *   The reader for the Azure Blob Storage.
  * @param tableLocation
  *   The location of the table.
  * @param tableName
  *   The name of the table.
  */
class SynapseEntitySchemaProvider(
    azureBlobStorageReader: BlobStorageReader[AdlsStoragePath],
    tableLocation: String,
    tableName: String
) extends SchemaProvider[ArcaneSchema]:

  /** @inheritdoc
    */
  override lazy val getSchema: Task[SchemaType] = getEntity.map(toArcaneSchema)

  /** @inheritdoc
    */
  private def getEntity: Task[SimpleCdmEntity] = for
    modelPath  <- ZIO.fromTry(AdlsStoragePath(tableLocation).map(_ + "model.json"))
    schemaData <- azureBlobStorageReader.readBlobContent(modelPath)
    model <- ZIO
      .attempt(SimpleCdmModel(schemaData))
      .orDieWith(e => Throwable(s"Unable to parse model.json file under location ${modelPath.toHdfsPath}", e))
    modelSchema <- ZIO
      .attempt(model.entities.find(_.name == tableName).get)
      .orDieWith(e =>
        Throwable(
          s"Table model not found in entities array of the model.json file under location ${modelPath.toHdfsPath}",
          e
        )
      )
  yield modelSchema

  /** @inheritdoc
    */
  override def empty: SchemaType = ArcaneSchema.empty()

  /** @inheritdoc
    */
  private def toArcaneSchema(simpleCdmModel: SimpleCdmEntity): ArcaneSchema = simpleCdmModel

object SynapseEntitySchemaProvider:

  private type Environment = AzureBlobStorageReader & SynapseLinkTableSettings

  val layer: ZLayer[Environment, Nothing, SynapseEntitySchemaProvider] =
    ZLayer {
      for
        tableSettings <- ZIO.service[SynapseLinkTableSettings]
        reader        <- ZIO.service[AzureBlobStorageReader]
      yield SynapseEntitySchemaProvider(reader, tableSettings.rootPath, tableSettings.name)
    }
