package com.sneaksanddata.arcane.framework
package services.cdm

import java.time.Duration

/**
 * Defines the retry settings for the CDM schema provider.
 */
trait RetrySettings:

  /**
   * The initial delay before the first retry.
   */
  val initialDelay: Duration

  /**
   * The number of retry attempts.
   */
  val retryAttempts: Int

/**
 * Settings for a CdmTable object
 * @param name Name of the table
 * @param rootPath HDFS-style path that includes table blob prefix, for example abfss://container@account.dfs.core.windows.net/path/to/table
 * @param retrySettings Optional retry settings for the table, if not provided default settings defined in the implementation will be used.
 */
case class CdmTableSettings(name: String, rootPath: String, retrySettings: Option[RetrySettings])
