package com.sneaksanddata.arcane.framework
package models.settings.sources

import models.settings.FieldSelectionRuleSettings

import upickle.ReadWriter

/** A partial override of `StreamSourceSettings` where every field is optional to support override/patch-style JSON
  * deserialization.
  */
trait OverrideStreamSourceSettings:

  /** The specific source configuration type that this override applies to.
    */
  type SourceSettingsType <: SourceSettings

  /** Optional override for the specific source configuration.
    */
  val configuration: Option[SourceSettingsType]

  /** Optional override for the source buffering configuration.
    */
  val buffering: Option[SourceBufferingSettings]

  /** Optional override for the field selection rule used when reading source data.
    */
  val fieldSelectionRule: Option[FieldSelectionRuleSettings]
