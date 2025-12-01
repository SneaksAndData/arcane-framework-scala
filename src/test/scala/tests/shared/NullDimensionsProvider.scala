package com.sneaksanddata.arcane.framework
package tests.shared

import services.base.DimensionsProvider

import scala.collection.immutable.SortedMap

object NullDimensionsProvider extends DimensionsProvider:
  override def getDimensions: SortedMap[String, String] = SortedMap()
