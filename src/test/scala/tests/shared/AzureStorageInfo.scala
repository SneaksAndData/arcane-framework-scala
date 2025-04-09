package com.sneaksanddata.arcane.framework
package tests.shared

import services.storage.services.AzureBlobStorageReader

import com.azure.storage.common.StorageSharedKeyCredential

object AzureStorageInfo:
  val endpoint = "http://localhost:10001/devstoreaccount1"
  val container = "cdm-e2e"
  val malformedSchemaContainer = "cdm-e2e-malformed-schema"
  val storageAccount = "devstoreaccount1"
  val accessKey = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="

  val credential: StorageSharedKeyCredential = StorageSharedKeyCredential(storageAccount, accessKey)
  val storageReader: AzureBlobStorageReader = AzureBlobStorageReader(storageAccount, endpoint, credential)
