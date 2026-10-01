package com.sneaksanddata.arcane.framework
package models.settings.staging

import models.serialization.ZIODurationRW.*
import models.settings.database.JdbcConnectionUrl

import upickle.ReadWriter
import zio.Duration

/** A partial override of `JdbcMergeServiceClientSettings` where every field is optional to support override/patch-style
  * JSON deserialization.
  */
trait OverrideJdbcMergeServiceClientSettings:
  /** Optional override for the JDBC connection URL.
    */
  val connectionUrl: Option[JdbcConnectionUrl]

  /** Optional override for the credential type used by the merge client.
    */
  val credentialType: Option[JdbcCredentialTypeSetting]

  /** Optional override for the extra JDBC connection parameters.
    */
  val extraConnectionParameters: Option[Map[String, String]]

  /** Optional override for the retry strategy applied to merge queries.
    */
  val queryRetryMode: Option[JdbcQueryRetryModeSettings]

  /** Optional override for the base retry delay.
    */
  val queryRetryBaseDuration: Option[Duration]

  /** Optional override for the retry scale factor.
    */
  val queryRetryScaleFactor: Option[Double]

  /** Optional override for the maximum number of retry attempts.
    */
  val queryRetryMaxAttempts: Option[Int]

  /** Optional override for the exception message text that triggers a retry.
    */
  val queryRetryOnMessageContents: Option[List[String]]

/** Default implementation for `OverrideJdbcMergeServiceClientSettings` using optional values.
  */
case class DefaultOverrideJdbcMergeServiceClientSettings(
    override val connectionUrl: Option[JdbcConnectionUrl] = None,
    override val credentialType: Option[JdbcCredentialTypeSetting] = None,
    override val extraConnectionParameters: Option[Map[String, String]] = None,
    override val queryRetryMode: Option[JdbcQueryRetryModeSettings] = None,
    override val queryRetryBaseDuration: Option[Duration] = None,
    override val queryRetryScaleFactor: Option[Double] = None,
    override val queryRetryMaxAttempts: Option[Int] = None,
    override val queryRetryOnMessageContents: Option[List[String]] = None
) extends OverrideJdbcMergeServiceClientSettings derives ReadWriter
