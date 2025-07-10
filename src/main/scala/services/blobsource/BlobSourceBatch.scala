package com.sneaksanddata.arcane.framework
package services.blobsource

import models.schemas.DataRow
import services.blobsource.readers.BlobSourceReader

type BlobSourceBatch          = BlobSourceReader#OutputRow
type BlobSourceVersionedBatch = (BlobSourceReader#OutputRow, Long)
