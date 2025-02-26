package com.sneaksanddata.arcane.framework
package services.cdm

import services.storage.models.azure.AdlsStoragePath

/**
 * A request to clean up the source.
 * @param prefix The prefix of the source to clean up.
 */
case class SourceCleanupRequest(prefix: AdlsStoragePath)

/**
 * The result of a source cleanup.
 * @param blobName The name of the blob.
 * @param deleteMarker The name of the delete marker.
 */
case class SourceCleanupResult(blobName: AdlsStoragePath, deleteMarker: AdlsStoragePath)

/**
 * The result of a source deletion.
 * @param blobName The name of the blob.
 * @param succeed True if the deletion succeeded, false otherwise.
 */
case class SourceDeletionResult(blobName: AdlsStoragePath, succeed: Boolean)
