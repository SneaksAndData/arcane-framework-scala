package com.sneaksanddata.arcane.framework
package services.mssql.base

/** Represents a summary of a column in a table. The first element is the name of the column, and the second element is
  * true if the column is a primary key.
  */
type ColumnSummary = (String, Boolean)

/** Represents a query to be executed on a Microsoft SQL Server database.
  */
type MsSqlQuery = String
