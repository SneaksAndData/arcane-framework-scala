package com.sneaksanddata.arcane.framework
package tests.synapse

import services.storage.models.base.StoredBlob
import services.synapse.SynapseAzureBlobReaderExtensions.asWatermark
import services.synapse.versioning.SynapseWatermark

import org.scalatest.flatspec._
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers._

class SynapseWatermarkTests extends AnyFlatSpec with Matchers:
  it should "serialize and deserialize" in {
    val wm1 = StoredBlob(
      name = "2026-01-01T00.00.00Z",
      createdOn = None
    ).asWatermark

    SynapseWatermark.fromJson(wm1.toJson) should be(wm1)
  }

  it should "compare correctly" in {
    val wm1 = StoredBlob(
      name = "2026-01-01T00.00.00Z",
      createdOn = None
    ).asWatermark

    val wmGreater = StoredBlob(
      name = "2026-01-01T01.00.00Z",
      createdOn = None
    ).asWatermark

    val wmEqual = StoredBlob(
      name = "2026-01-01T00.00.00Z",
      createdOn = None
    ).asWatermark

    val wmLess = StoredBlob(
      name = "2025-12-01T00.00.00Z",
      createdOn = None
    ).asWatermark

    val beComparable = (be > wmLess and be < wmGreater) and be(wmEqual)

    wm1 should beComparable
  }
