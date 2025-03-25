package com.sneaksanddata.arcane.framework
package services.cdm

import models.ArcaneSchema
import models.cdm.{SimpleCdmEntity, SimpleCdmModel, given_Conversion_SimpleCdmEntity_ArcaneSchema}

import com.sneaksanddata.arcane.framework.models.given_CanAdd_ArcaneSchema
import services.base.SchemaProvider
import services.storage.models.azure.AdlsStoragePath
import services.storage.services.AzureBlobStorageReader
import services.storage.base.BlobStorageReader

import com.sneaksanddata.arcane.framework.excpetions.StreamFailException
import com.sneaksanddata.arcane.framework.logging.ZIOLogAnnotations.zlog
import zio.DurationOps.*
import zio.stream.ZStream
import zio.{Schedule, Task, ZIO, ZLayer}

import java.io.IOException
import java.time.Duration

/**
 * A provider of a schema for a data produced by Microsoft Synapse Link.
 *
 * @param azureBlobStorageReader The reader for the Azure Blob Storage.
 * @param tableLocation          The location of the table.
 * @param tableName              The name of the table.
 * 
 */
class CdmSchemaProvider(azureBlobStorageReader: BlobStorageReader[AdlsStoragePath], tableLocation: String, tableName: String, retrySettings: Option[RetrySettings]) extends SchemaProvider[ArcaneSchema]:

  private val defaultRetrySettings = new RetrySettings:
    override val initialDelay: Duration = Duration.ofSeconds(1)
    override val retryAttempts: Int = 10
    override val maxDuration: Duration = Duration.ofSeconds(30)

  private val retryPolicy =
    (Schedule.exponential(retrySettings.getOrElse(defaultRetrySettings).initialDelay).jittered(0.0, 1.0) || Schedule.spaced(retrySettings.getOrElse(defaultRetrySettings).maxDuration))
    && Schedule.recurs(retrySettings.getOrElse(defaultRetrySettings).retryAttempts)

  /**
   * @inheritdoc
   */
  override lazy val getSchema: Task[SchemaType] = getEntity.map(toArcaneSchema)

  /**
   * @inheritdoc
   */
  private def getEntity: Task[SimpleCdmEntity] =
    val task = for modelPath <- ZIO.fromTry(AdlsStoragePath(tableLocation).map(_ + "model.json"))
        reader = ZIO.fromAutoCloseable(azureBlobStorageReader.streamBlobContent(modelPath)).refineToOrDie[IOException]
        stream = ZStream.fromReaderScoped(reader)
        json <- stream.runCollect.map(_.mkString)
        model <- ZIO.attempt(SimpleCdmModel(json))
    yield model.entities.find(_.name == tableName).getOrElse(throw new Exception(s"Table $tableName not found in model $tableLocation"))

    task.tapErrorCause(cause => zlog(template="Error reading schema from %s", cause=cause, values=tableLocation))
      .catchSome({
        case e: upickle.core.AbortException => ZIO.die(new StreamFailException("Model file is empty, not retrying the stream", e))
      })
      .retry(retryPolicy)

  /**
   * @inheritdoc
   */
  override def empty: SchemaType = ArcaneSchema.empty()

  /**
   * @inheritdoc
   */
  private def toArcaneSchema(simpleCdmModel: SimpleCdmEntity): ArcaneSchema = simpleCdmModel

object CdmSchemaProvider:

  private type Environment = AzureBlobStorageReader & CdmTableSettings

  val layer: ZLayer[Environment, Nothing, CdmSchemaProvider] =
    ZLayer {
      for
        tableSettings <- ZIO.service[CdmTableSettings]
        reader <- ZIO.service[AzureBlobStorageReader]
      yield CdmSchemaProvider(reader, tableSettings.rootPath, tableSettings.name, tableSettings.retrySettings)
    }
