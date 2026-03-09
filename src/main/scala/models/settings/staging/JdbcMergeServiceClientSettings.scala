package com.sneaksanddata.arcane.framework
package models.settings.staging

import models.serialization.ZIODurationRW.*

import upickle.ReadWriter
import upickle.default.*
import zio.Duration

import java.sql.DriverManager
import scala.util.Try

/** Retry modes available for the client
  */
enum JdbcQueryRetryMode derives ReadWriter:
  /** Always retry
    */
  case Always

  /** Only retry in backfill mode
    */
  case BackfillOnly

  /** Never retry
    */
  case Never

trait JdbcMergeServiceClientSettings:
  /** The connection URL.
    */
  val connectionUrl: String

  /** Optional extra connection parameters for the merge client (tags, session properties etc.)
    */
  val extraConnectionParameters: Map[String, String]

  /** Enable query retries for JDBC merge
    */
  val queryRetryMode: JdbcQueryRetryMode

  /** Exp retry base duration
    */
  val queryRetryBaseDuration: Duration

  /** Exp retry scale factor
    */
  val queryRetryScaleFactor: Double

  /** Exp retry max attempts
    */
  val queryRetryMaxAttempts: Int

  /** Exception messages to retry
    */
  val queryRetryOnMessageContents: List[String]

  /** Checks if the connection URL is valid.
    *
    * @return
    *   True if the connection URL is valid, false otherwise.
    */
  final def isValid: Boolean = Try(DriverManager.getDriver(connectionUrl)).isSuccess

  final def getConnectionString: String = Seq(
    connectionUrl,
    extraConnectionParameters
      .map { case (key, value) =>
        s"$key=$value"
      }
      .mkString("&")
  ).mkString("&")

case class DefaultJdbcMergeServiceClientSettings(
                                                  override val queryRetryMode: JdbcQueryRetryMode,
                                                  override val queryRetryBaseDuration: Duration,
                                                  override val queryRetryOnMessageContents: List[String],
                                                  override val queryRetryScaleFactor: Double,
                                                  override val queryRetryMaxAttempts: Int,
                                                  override val extraConnectionParameters: Map[String, String],
                                                  override val connectionUrl: String
                                                ) extends JdbcMergeServiceClientSettings derives ReadWriter
