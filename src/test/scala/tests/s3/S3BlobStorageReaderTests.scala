package com.sneaksanddata.arcane.framework
package tests.s3

import zio.Scope
import zio.test.{Spec, TestEnvironment, ZIOSpecDefault}

object S3BlobStorageReaderTests extends ZIOSpecDefault {
  override def spec: Spec[TestEnvironment & Scope, Any] = suite("S3BlobStorageReader")(
    
  )
}
