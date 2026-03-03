package com.sneaksanddata.arcane.framework
package models.settings.sources.synapse

import models.settings.sources.SourceSettings

/** Synapse-link specific source settings
  */
trait SynapseSourceSettings extends SourceSettings:

  /** The name of the entity in the source system
    */
  val entityName: String

  /** The root directory of the Azure Synapse link data export
    */
  val baseLocation: String
