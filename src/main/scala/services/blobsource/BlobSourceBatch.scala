package com.sneaksanddata.arcane.framework
package services.blobsource

import models.schemas.DataRow

type BlobSourceBatch          = DataRow
type BlobSourceVersionedBatch = (DataRow, Long)
