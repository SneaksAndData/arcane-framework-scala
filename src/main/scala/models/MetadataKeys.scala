package com.sneaksanddata.arcane.framework
package models

object MetadataKeys:
  private val metadataKeyPrefix = "arcane"
  private val pluginGroup       = "plugin"
  private val frameworkGroup    = "core"

  val watermarkKey = "watermark"
  val migrationKey = "migration-identifier"

  /** Get full name of a metadata key for a framework-scoped key name
    */
  def getFrameworkKey(name: String): String = s"$metadataKeyPrefix.$frameworkGroup.$name"

  /** Get full name of a metadata key for a specific plugin and key name
    */
  def getPluginKey(pluginName: String, name: String): String = s"$metadataKeyPrefix.$pluginGroup.$pluginName.$name"
