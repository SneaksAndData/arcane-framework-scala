package com.sneaksanddata.arcane.framework
package models.serialization

import services.storage.models.azure.AdlsStoragePath

import upickle.ReadWriter
import upickle.default.readwriter

object AdlsStoragePathRW:
  implicit val rw: ReadWriter[AdlsStoragePath] = readwriter[String].bimap[AdlsStoragePath](
    adlsPath => adlsPath.toHdfsPath,
    str => AdlsStoragePath(str).get
  )
