package com.sneaksanddata.arcane.framework
package models.maintenance

trait MaintenanceRequest:
  def toSqlExpression: String
