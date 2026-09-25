package com.sneaksanddata.arcane.framework
package tests.shared

import models.settings.sources.{BufferingStrategy, SourceBufferingSettings, Unbounded, UnboundedImpl}

object TestSourceBufferingSettings extends SourceBufferingSettings:
  override val bufferingStrategy: BufferingStrategy = UnboundedImpl(Unbounded())
  override val bufferingEnabled: Boolean            = true

  override type MergeableFrom = this.type
  override type MergeResult   = this.type
  override def merge(overrides: Option[MergeableFrom]): MergeResult = ???
