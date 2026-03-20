package com.sneaksanddata.arcane.framework
package models.settings.sources.synapse

import models.settings.azure.{AzureStorageConnectionSettings, DefaultAzureStorageConnectionSettings}
import models.settings.sources.SourceSettings
import models.serialization.AdlsStoragePathRW.*
import services.storage.models.azure.AdlsStoragePath

import upickle.ReadWriter
import upickle.implicits.key

trait MicrosoftSynapseLinkConnectionSettings extends SourceSettings:

  /** The name of the entity in the source system
    */
  val entityName: String

  /** The root directory of the Azure Synapse link data export
    */
  val baseLocation: AdlsStoragePath

  /** Connection configuration for the Synapse storage account
    */
  val storageConnection: AzureStorageConnectionSettings

case class DefaultMicrosoftSynapseLinkConnectionSettings(
    override val entityName: String,
    override val baseLocation: AdlsStoragePath,
    override val storageConnection: DefaultAzureStorageConnectionSettings
) extends MicrosoftSynapseLinkConnectionSettings derives ReadWriter
