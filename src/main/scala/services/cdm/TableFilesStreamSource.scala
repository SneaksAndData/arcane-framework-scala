package com.sneaksanddata.arcane.framework
package services.cdm

import models.settings.VersionedDataGraphBuilderSettings
import services.cdm.AzureBlobStorageReaderExtensions.{SchemaEnrichedBlobStream, getLastCommitDate, streamTableContent}
import services.storage.base.BlobStorageReader
import services.storage.models.azure.AdlsStoragePath

import com.sneaksanddata.arcane.framework.services.synapse.SynapseLinkTableSettings
import zio.Schedule
import zio.stream.ZStream

import java.time.Duration

/**
 * The data source that produces a stream list of files contained in the Azure Blob Storage exported from
 * Microsoft Dynamics 365 using Microsoft Synapse Link.
 */
class TableFilesStreamSource(settings: VersionedDataGraphBuilderSettings,
                             reader: BlobStorageReader[AdlsStoragePath],
                             containerRoot: AdlsStoragePath,
                             tableSettings: SynapseLinkTableSettings):

  /**
   * Read a table snapshot starting from the last commit date - lookBackInterval
   * @return A stream of rows for this table
   */
  def lookBackStream: SchemaEnrichedBlobStream =
    streamFilenamesWithTimeGap(settings.lookBackInterval)

  /**
   * Read a table snapshot starting from the last commit date - changeCapturePeriod, repeat every changeCaptureInterval
   * @return A stream of rows for this table
   */
  def changeCaptureStream: SchemaEnrichedBlobStream =
    streamFilenamesWithTimeGap(settings.changeCapturePeriod).repeat(Schedule.spaced(settings.changeCaptureInterval))


  private def streamFilenamesWithTimeGap(interval: Duration) =
    for
      latestPrefix <- ZStream.fromZIO(reader.getLastCommitDate(containerRoot))
      p <- reader.streamTableContent(containerRoot, latestPrefix.minus(interval), latestPrefix, tableSettings.name)
    yield p
