package com.sneaksanddata.arcane.framework
package utils

import java.nio.charset.StandardCharsets

object HashUtils:
  /** Equivalent to Trino expression: lower(to_hex(murmur3(to_utf8('<input>')))).
    *
    * Trino docs for murmur3: https://trino.io/docs/current/functions/binary.html#hashing-functions.
    *
    * Requirements to match the above Trino expression:
    *   - Input encoding: UTF-8
    *   - Murmur3 flavor: x64 128 bit
    *   - Seed: 0
    *   - Representation: two 64-bit output integers serialized in little-endian before hex encoding
    * @param input
    * @return
    *   murmur3 hashed input representation
    */
  def murmur3(input: String): String =
    val hasher = com.google.common.hash.Hashing.murmur3_128()
    hasher.hashString(input, StandardCharsets.UTF_8).toString
