package com.sneaksanddata.arcane.framework
package utils

import java.nio.charset.StandardCharsets
import org.apache.commons.codec.digest.MurmurHash3

object HashUtils:
  /** Equivalent to Trino expression: lower(to_hex(murmur3(utf_8('<input>')))).
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
    val inputBytes    = input.getBytes(StandardCharsets.UTF_8)
    val Array(h1, h2) = MurmurHash3.hash128x64(inputBytes)
    val h1Rev         = java.lang.Long.reverseBytes(h1)
    val h2Rev         = java.lang.Long.reverseBytes(h2)
    f"$h1Rev%016x$h2Rev%016x"
