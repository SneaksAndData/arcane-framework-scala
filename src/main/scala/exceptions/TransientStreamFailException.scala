package com.sneaksanddata.arcane.framework
package exceptions

/** Exception thrown when stream fails with recovery (code 2)
  */
case class TransientStreamFailException(message: String, cause: Exception) extends RuntimeException(message, cause)
