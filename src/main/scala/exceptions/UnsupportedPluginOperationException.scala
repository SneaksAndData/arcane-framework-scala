package com.sneaksanddata.arcane.framework
package exceptions

import zio.{Task, ZIO}

/** Exception thrown when operation is unsupported (code 3)
  */
case class UnsupportedPluginOperationException(message: String) extends RuntimeException(message)

/** Fails with an [[UnsupportedPluginOperationException]] naming the operation and the component that does not implement
  * it.
  *
  * Intended for members a trait forces an implementer to declare but which it cannot meaningfully provide, so that the
  * unimplemented path fails loudly and identifiably instead of returning an empty or default result.
  */
def unsupported(operation: String, component: String): Task[Nothing] =
  ZIO.fail(UnsupportedPluginOperationException(s"$operation is not supported by $component"))
