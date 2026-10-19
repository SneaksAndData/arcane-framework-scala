package com.sneaksanddata.arcane.framework
package models

import models.settings.sources.modification.FrozenSurrogateMergeKey

import zio.Task

trait PrimaryKeyProvider:
  protected def getPrimaryKey: Task[FrozenSurrogateMergeKey]
