package com.sneaksanddata.arcane.framework
package services.synapse.versioning

import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter

object SynapseVersionExtensions:
  extension (version: SynapseVersionType)
    def asDate: OffsetDateTime =
      val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH.mm.ssX")
      OffsetDateTime.parse(version, formatter)
