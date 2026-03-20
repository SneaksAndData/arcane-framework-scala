package com.sneaksanddata.arcane.framework
package tests.shared

import models.settings.azure.{CredentialTypeSetting, DefaultAzureHttpClientSettings, DefaultAzureStorageConnectionSettings, SharedKey}
import services.storage.services.azure.AzureBlobStorageReader

import java.time.Duration

object TestAzureStorageInfo:
  val endpoint                 = "http://localhost:10001/devstoreaccount1"
  val container                = "cdm-e2e"
  val malformedSchemaContainer = "cdm-e2e-malformed-schema"
  val storageAccount           = "devstoreaccount1"
  val accessKey = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="

  val storageReader: AzureBlobStorageReader  = AzureBlobStorageReader(
    DefaultAzureStorageConnectionSettings(
      storageAccount,
      Some(endpoint),
      DefaultAzureHttpClientSettings(
        3,
        Duration.ofSeconds(1),
        Duration.ofMillis(100),
        Duration.ofSeconds(10),
        1000
      ),
      CredentialTypeSetting(
        Some(SharedKey(accessKey)),
        None
      )
    )
  )
