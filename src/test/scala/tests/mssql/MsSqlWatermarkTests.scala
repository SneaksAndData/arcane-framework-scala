package com.sneaksanddata.arcane.framework
package tests.mssql

import services.mssql.versioning.MsSqlWatermark

import org.scalatest.flatspec.*
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.*

import java.time.OffsetDateTime

class MsSqlWatermarkTests extends AnyFlatSpec with Matchers:
  it should "serialize and deserialize" in {
    val wm1 = MsSqlWatermark.fromChangeTrackingVersion(version = 1234L, commitTime = OffsetDateTime.now())

    MsSqlWatermark.fromJson(wm1.toJson) should be(wm1)
  }

  it should "compare correctly" in {
    val timestamp = OffsetDateTime.now()
    val wm1       = MsSqlWatermark.fromChangeTrackingVersion(version = 1234L, commitTime = timestamp)

    val wmGreater = MsSqlWatermark.fromChangeTrackingVersion(version = 12345L, commitTime = timestamp.plusHours(1))

    val wmEqual = MsSqlWatermark.fromChangeTrackingVersion(version = 1234L, commitTime = timestamp)

    val wmLess = MsSqlWatermark.fromChangeTrackingVersion(version = 1233L, commitTime = timestamp.minusHours(1))

    val beComparable = (be > wmLess and be < wmGreater) and be(wmEqual)

    wm1 should beComparable
  }
