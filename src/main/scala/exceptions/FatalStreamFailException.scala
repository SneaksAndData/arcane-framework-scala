package com.sneaksanddata.arcane.framework
package exceptions

/** Exception thrown when stream fails without recovery (code 1)
  */
case class FatalStreamFailException(message: String) extends RuntimeException(message)
