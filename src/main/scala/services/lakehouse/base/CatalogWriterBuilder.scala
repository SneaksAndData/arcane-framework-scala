package com.sneaksanddata.arcane.framework
package services.lakehouse.base

/**
 * CatalogWriterBuilder is a trait that defines the methods to initialize a CatalogWriter instance
 */
trait CatalogWriterBuilder[CatalogImpl, TableImpl, SchemaImpl]:
  /**
   * Initialize the catalog connection
   *
   * @return CatalogWriter instance ready to perform data operations
   */
  def initialize(): CatalogWriter[CatalogImpl, TableImpl, SchemaImpl]
