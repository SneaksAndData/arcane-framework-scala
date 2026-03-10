package com.sneaksanddata.arcane.framework
package models.serialization

import software.amazon.awssdk.regions.Region
import upickle.ReadWriter
import upickle.default.readwriter

/** Read-writer support for AWS S3 region
  */
object S3RegionRW:
  implicit val rw: ReadWriter[Region] = readwriter[String].bimap[Region](
    region => region.toString,
    str => Region.of(str)
  )
