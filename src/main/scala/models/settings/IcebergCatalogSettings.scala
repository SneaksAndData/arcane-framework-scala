package com.sneaksanddata.arcane.framework
package models.settings

/** Connection information for Iceberg REST Catalog
  */
trait IcebergCatalogSettings:
  /** The namespace (schema) of the catalog.
    */
  val namespace: String

  /** The warehouse name of the catalog.
    */
  val warehouse: String

  /** The catalog server URI.
    */
  val catalogUri: String

  /** The catalog additional properties.
    */
  val additionalProperties: Map[String, String]
