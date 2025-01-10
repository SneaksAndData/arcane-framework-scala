package com.sneaksanddata.arcane.framework
package services.consumers

import services.streaming.graph_builders.BackfillDataGraphBuilder

import org.slf4j.{Logger, LoggerFactory}
import zio.{ZIO, ZLayer}

import java.sql.{Connection, DriverManager, ResultSet}
import scala.concurrent.Future
import scala.util.Try


/**
 * Options for the JDBC consumer.
 */
trait JdbcConsumerOptions:
  /**
   * The connection URL.
   */
  val connectionUrl: String

  /**
   * Checks if the connection URL is valid.
   * @return True if the connection URL is valid, false otherwise.
   */
  final def isValid: Boolean = Try(DriverManager.getDriver(connectionUrl)).isSuccess

/**
 * The result of applying a batch.
 */
type BatchApplicationResult = Boolean

/**
 * The result of applying a batch.
 */
class BatchArchivationResult

/**
 * A consumer that consumes batches from a JDBC source.
 *
 * @param options The options for the consumer.
 */
class JdbcConsumer[Batch <: StagedVersionedBatch](val options: JdbcConsumerOptions) extends AutoCloseable:
//  require(options.isValid, "Invalid JDBC url provided for the consumer")

  implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global
  
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  private lazy val sqlConnection: Connection = DriverManager.getConnection(options.connectionUrl)
  
  def getPartitionValues(batchName: String, partitionFields: List[String]): Future[Map[String, List[String]]] =
    Future.sequence(partitionFields
      .map(partitionField =>
        val query = s"SELECT DISTINCT $partitionField FROM $batchName"
        Future(sqlConnection.prepareStatement(query).executeQuery())
          .map(collectPartitionColumn(_, partitionField))
          .map(values => partitionField -> values.toList)
      )).map(_.toMap)

  
  def applyBatch(batch: Batch): Future[BatchApplicationResult] =
    Future{
      logger.debug(s"Executing batch query: ${batch.batchQuery.query}")
      sqlConnection.prepareStatement(batch.batchQuery.query).execute()
    }
    
  def archiveBatch(batch: Batch): Future[BatchArchivationResult] =
    Future(sqlConnection.prepareStatement(batch.archiveExpr).execute())
      .flatMap(_ => Future(sqlConnection.prepareStatement(s"DROP TABLE ${batch.name}").execute()))
      .map(_ => new BatchArchivationResult)

  def close(): Unit = sqlConnection.close()

  private def collectPartitionColumn(resultSet: ResultSet, columnName: String): Seq[String] =
    // do not fail on closed result sets
    if resultSet.isClosed then
      Seq.empty
    else
      val current = resultSet.getString(columnName)
      if resultSet.next() then
        collectPartitionColumn(resultSet, columnName) :+ current
      else
        resultSet.close()
        Seq(current)


object JdbcConsumer:
  /**
   * Factory method to create JdbcConsumer.
   * @param options The options for the consumer.
   * @return The initialized JdbcConsumer instance
   */
  def apply[Batch <: StagedVersionedBatch](options: JdbcConsumerOptions): JdbcConsumer[Batch] =
    new JdbcConsumer[Batch](options)

  /**
   * The ZLayer that creates the JdbcConsumer.
   */
  val layer: ZLayer[JdbcConsumerOptions, Nothing, JdbcConsumer[StagedVersionedBatch]] =
    ZLayer.scoped {
      ZIO.fromAutoCloseable {
        for connectionOptions <- ZIO.service[JdbcConsumerOptions] yield JdbcConsumer(connectionOptions)
      }
    }
