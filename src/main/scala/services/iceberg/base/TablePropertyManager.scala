package com.sneaksanddata.arcane.framework
package services.iceberg.base

import zio.Task

/** Object responsible for managing table properties in Iceberg Catalog
  */
trait TablePropertyManager:
  /** Adds or updates a comment on the table
    *
    * @param tableName
    *   Name of the table
    * @param text
    *   Comment text
    * @return
    */
  def comment(tableName: String, text: String): Task[Unit]

  /** Reads a specified table property
    *
    * @param tableName
    *   Name of the table
    * @return
    */
  def getProperty(tableName: String, propertyName: String): Task[String]

  /**
   * Get table partition information
   * @param tableName
   *   Name of the table
   * @return
   */
  def getPartitions(tableName: String): Task[Unit]

  /**
   * Return table size from catalog metadata
   * @param tableName
   *   Name of the table
   * @return
   */
  def getTableSize(tableName: String): Task[(Records: Long, Size: Long)]
