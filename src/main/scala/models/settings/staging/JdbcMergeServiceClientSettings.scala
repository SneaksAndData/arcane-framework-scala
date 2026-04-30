package com.sneaksanddata.arcane.framework
package models.settings.staging

import models.serialization.ZIODurationRW.*
import models.settings.database.JdbcConnectionUrl
import models.settings.database.JdbcConnectionExtensions.*

import upickle.ReadWriter
import upickle.default.*
import upickle.implicits.key
import zio.Duration

import java.sql.DriverManager
import scala.util.Try

/** Retry modes available for the client
  */
sealed trait JdbcQueryRetryMode

/** Always retry
  */
case class Always() derives ReadWriter
case class AlwaysImpl(alwaysMode: Always) extends JdbcQueryRetryMode

/** Never retry
  */
case class Never() derives ReadWriter
case class NeverImpl(neverMode: Never) extends JdbcQueryRetryMode

/** Only retry in backfill mode
  */
case class BackfillOnly() derives ReadWriter
case class BackfillOnlyImpl(backfillOnly: BackfillOnly) extends JdbcQueryRetryMode

case class JdbcQueryRetryModeSettings(
    always: Option[Always] = None,
    never: Option[Never] = None,
    backfillOnly: Option[BackfillOnly] = None
) derives ReadWriter:
  def resolveRetryMode: JdbcQueryRetryMode = never
    .map(NeverImpl(_))
    .getOrElse(
      backfillOnly
        .map(BackfillOnlyImpl(_))
        .getOrElse(
          always
            .map(AlwaysImpl(_))
            .getOrElse(NeverImpl(Never()))
        )
    )

trait JdbcMergeServiceClientSettings:
  /** The connection URL.
    */
  val connectionUrl: JdbcConnectionUrl

  /** Credential type to be used by JDBC client
    */
  val credentialType: JdbcCredentialType

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

  final def getConnectionString(catalog: String, schema: String, credential: JdbcCredentialType): String =
    val baseUrl = connectionUrl
      .withDefaultCatalog(catalog)
      .withDefaultSchema(schema)
      .withUrlParameters(extraConnectionParameters)

    credential match {
      case AnonymousCredentialImpl(_)           => baseUrl
      case BasicCredentialImpl(basicCredential) => baseUrl.withUrlParameters(basicCredential.asParameters)
    }

case class DefaultJdbcMergeServiceClientSettings(
    @key("credentialType") credentialSetting: JdbcCredentialTypeSetting,
    @key("queryRetryMode") queryRetryModeSettings: JdbcQueryRetryModeSettings,
    override val queryRetryBaseDuration: Duration,
    override val queryRetryOnMessageContents: List[String],
    override val queryRetryScaleFactor: Double,
    override val queryRetryMaxAttempts: Int,
    override val extraConnectionParameters: Map[String, String],
    @key("connectionUrl") connectionString: Option[String] = None
) extends JdbcMergeServiceClientSettings derives ReadWriter:
  override val connectionUrl: String =
    connectionString.getOrElse(sys.env("ARCANE_FRAMEWORK__MERGE_SERVICE_CONNECTION_URI"))
  override val queryRetryMode: JdbcQueryRetryMode = queryRetryModeSettings.resolveRetryMode
  override val credentialType: JdbcCredentialType = credentialSetting.resolveCredentialType
