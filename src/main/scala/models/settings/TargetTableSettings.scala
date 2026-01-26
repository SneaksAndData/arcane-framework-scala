package com.sneaksanddata.arcane.framework
package models.settings

/** Settings for the target table
  */
trait TargetTableSettings:
  /** The name of the target table
    */
  val targetTableFullName: String

  /** The maintenance settings for the target table
    */
  val maintenanceSettings: TableMaintenanceSettings

  /** Retrieve names for each component of a target table name
    * @return
    */
  def targetTableNameParts: (Warehouse: String, Namespace: String, Name: String) =
    targetTableFullName.split('.').toList match
      case warehouse :: namespace :: name :: _ => (Warehouse = warehouse, Namespace = namespace, Name = name)
      case _ =>
        throw new RuntimeException(
          s"Invalid table name format for $targetTableFullName. Must be {warehouse}.{namespace}.{name}"
        )
