package com.sneaksanddata.arcane.framework
package utils

import zio.{Task, ZIO}

object MetadataUtils:
  /** Read the current framework version
    */
  def getFrameworkVersion: Task[String] = for pkg <- ZIO.attempt(MetadataUtils.getClass.getPackage)
  yield Option(pkg.getImplementationVersion).getOrElse(pkg.getSpecificationVersion)
