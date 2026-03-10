package com.sneaksanddata.arcane.framework
package models.serialization

import upickle.ReadWriter
import upickle.default.readwriter

import java.net.URI

/** Read-writer support for AWS S3 region
  */
object UriRW:
  implicit val rw: ReadWriter[URI] = readwriter[String].bimap[URI](
    url => url.toString,
    str => URI.create(str)
  )
