package com.sneaksanddata.arcane.framework
package tests.utils

import utils.HashUtils

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks.*

class HashUtilsTests extends AnyFlatSpec with Matchers:
  private val testCases = Table(
    ("input", "expectedHash"),
    ("", "00000000000000000000000000000000"),
    ("foo", "6145f501578671e2877dba2be487af7e"),
    ("Hello, world!", "df65d6d2d12d51f164c5f3a85066322c"),
    ("æøåąčę", "5ba747b2f2bbf7a0b8096afceae1a1d3")
  )

  // Equivalent output to Trino expression: lower(to_hex(murmur3(to_utf8('<input>'))))
  "murmur3" should "produce Trino-compatible Murmur3 x64 128-bit hashes" in {
    forAll(testCases) { (input, expectedHash) =>
      HashUtils.murmur3(input) shouldBe expectedHash
    }
  }
