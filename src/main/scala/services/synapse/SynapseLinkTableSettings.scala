package com.sneaksanddata.arcane.framework
package services.synapse

import java.time.Duration

/**
 * Settings for a CdmTable object
 * @param name Name of the table
 * @param rootPath HDFS-style path that includes table blob prefix, for example abfss://container@account.dfs.core.windows.net/path/to/table
 */
case class SynapseLinkTableSettings(name: String, rootPath: String)
