package com.sneaksanddata.arcane.framework
package models

import models.settings.sources.modification.FrozenSurrogateVersion

import zio.Task

trait VersionProvider:
  protected def getVersionField: Task[FrozenSurrogateVersion]
