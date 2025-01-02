package com.sneaksanddata.arcane.framework
package services.cdm

/**
 * Settings for a CdmTable object
 * @param name Name of the table
 * @param rootPath HDFS-style path that includes table blob prefix, for example abfss://container@account.dfs.core.windows.net/path/to/table
 */
case class CdmTableSettings(name: String, rootPath: String)
