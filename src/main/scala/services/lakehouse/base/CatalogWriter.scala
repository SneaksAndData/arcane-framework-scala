package com.sneaksanddata.arcane.framework
package services.lakehouse.base

import models.{ArcaneSchema, DataRow}

import zio.Task

import scala.concurrent.Future


/**
 * CatalogFileIO marks a class that holds implementation of a filesystem used by the catalog
 */
sealed trait CatalogFileIO:
  val implClass: String

/**
 * S3CatalogFileIO implements S3-based filesystem when used by a catalog
 */
trait S3CatalogFileIO extends CatalogFileIO:
  override val implClass: String = "org.apache.iceberg.aws.s3.S3FileIO"
  /**
   * S3 endpoint to use with this IO implementation
   */
  val endpoint: String

  val pathStyleEnabled = "true"
  /**
   * Static access key identifier to use with this IO implementation
   */
  val accessKeyId: String
  /**
   * Static secret access key to use with this IO implementation
   */
  val secretAccessKey: String
  /**
   * S3 region to use with this IO implementation
   */
  val region: String

/**
 * Singleton for S3CatalogFileIO
 */
object S3CatalogFileIO extends S3CatalogFileIO:
  override val secretAccessKey: String = scala.util.Properties.envOrElse("ARCANE_FRAMEWORK__S3_CATALOG_SECRET_ACCESS_KEY", "")
  override val accessKeyId: String = scala.util.Properties.envOrElse("ARCANE_FRAMEWORK__S3_CATALOG_ACCESS_KEY_ID", "")
  override val endpoint: String = scala.util.Properties.envOrElse("ARCANE_FRAMEWORK__S3_CATALOG_ENDPOINT", "")
  override val region: String = scala.util.Properties.envOrElse("ARCANE_FRAMEWORK__S3_CATALOG_REGION", "us-east-1")

trait CatalogWriterBuilder[CatalogImpl, TableImpl, SchemaImpl]:
  /**
   * Initialize the catalog connection
   * @return CatalogWriter instance ready to perform data operations
   */
  def initialize(): CatalogWriter[CatalogImpl, TableImpl, SchemaImpl]
  
trait CatalogWriter[CatalogImpl, TableImpl, SchemaImpl] extends AutoCloseable:
  implicit val catalog: CatalogImpl
  implicit val catalogProperties: Map[String, String]
  implicit val catalogName: String

  /**
   * Creates a table published to the configured Catalog from the data provided.
   * @param data Rows to append to the table
   * @param name Name for the table in the catalog
   * @return Reference to the created table
   */
  def write(data: Iterable[DataRow], name: String, schema: SchemaImpl): Task[TableImpl]

  /**
   * Deletes the specified table from the catalog
   * @param tableName Table to delete
   * @return true if successful, false otherwise
   */
  def delete(tableName: String): Task[Boolean]

  /**
   * Appends provided rows to the table.
   * @param data Rows to append
   * @param name Table to append to
   * @return Reference to the updated table
   */
  def append(data: Iterable[DataRow], name: String, schema: SchemaImpl): Task[TableImpl]
