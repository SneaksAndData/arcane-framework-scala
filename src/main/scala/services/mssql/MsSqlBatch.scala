package com.sneaksanddata.arcane.framework
package services.mssql

import models.schemas.DataRow
import services.mssql.base.{CanPeekHead, QueryResult}
import services.mssql.query.LazyQueryResult

/** Represents a batch of data.
 */
type DataBatch = QueryResult[LazyQueryResult.OutputType] & CanPeekHead[LazyQueryResult.OutputType]

/** Batch type for Microsoft Sql Server is a list of DataRow elements
 */
type MsSqlBatch          = DataBatch
type MsSqlVersionedBatch = (DataBatch, Long)