package com.sneaksanddata.arcane.framework
package models.settings.sources.modification

import models.settings.CompositeSetting

import upickle.default.*
import upickle.implicits.key

/** A partial override of `DataRowModificationSettings` where every field is optional to support override/patch-style
  * JSON deserialization.
  */
trait OverrideDataRowModificationSettings:
  /** Optional override for the ordered list of modification settings.
    */
  val modificationSettings: Option[Seq[CompositeSetting[DataRowModification]]]

/** Default implementation for `OverrideDataRowModificationSettings` using optional values.
  */
case class DefaultOverrideDataRowModificationSettings(
    @key("modifications") override val modificationSettings: Option[Seq[CompositeSetting[DataRowModification]]] = None
) extends OverrideDataRowModificationSettings derives ReadWriter
