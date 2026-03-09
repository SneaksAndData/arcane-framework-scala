package com.sneaksanddata.arcane.framework
package models.settings.sources

import models.settings.FieldSelectionRuleSettings

/** The basic settings related to the data source
  */
trait SourceSettings:

  /** How often to check for changes in the source data
    */
  val changeCaptureIntervalSeconds: Int
