package com.sneaksanddata.arcane.framework
package excpetions

/**
 * Exception thrown when stream fails.
 * This exception should not be caught, as it indicates a hard failure in the stream.
 */
class StreamFailException(message: String, cause: Exception) extends Exception(message, cause)
