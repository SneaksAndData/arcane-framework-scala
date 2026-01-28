package com.sneaksanddata.arcane.framework
package services.iceberg.base

import org.apache.iceberg.catalog.SessionCatalog.SessionContext
import org.apache.iceberg.rest.RESTSessionCatalog
import zio.Task

/** Object responsible for creating catalog clients. Takes care of recycling expired instances and provided the current
  * active one to the caller
  */
trait CatalogFactory:
  /** Sets up a new session context for the catalog client
    * @return
    */
  def getSessionContext: SessionContext

  /** Create a new RESTSessionCatalog isntance
    * @return
    */
  def newCatalog: Task[RESTSessionCatalog]

  /** Retrieve current active RESTSessionCatalog instance
    * @return
    */
  def getCatalog: Task[RESTSessionCatalog]
