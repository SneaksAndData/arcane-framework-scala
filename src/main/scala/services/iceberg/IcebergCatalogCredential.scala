package com.sneaksanddata.arcane.framework
package services.iceberg

import org.apache.iceberg.CatalogProperties
import org.apache.iceberg.rest.auth.OAuth2Properties

import java.time.Duration

private trait IcebergCatalogCredential:
  val credential: String
  val oauth2Uri: String
  val oauth2Scope: String
  val oauth2StaticToken: String // rename
  val oauth2TokenRefreshEnabled: Boolean
  val oauth2SessionTimeoutMs: Long

/**
 * Singleton holding information required by Iceberg REST Catalog Auth API
 */
object IcebergCatalogCredential extends IcebergCatalogCredential:
  override val credential: String = sys.env.getOrElse("ARCANE_FRAMEWORK__S3_CATALOG_AUTH_CLIENT_ID", "") + ":" + sys.env.getOrElse("ARCANE_FRAMEWORK__S3_CATALOG_AUTH_CLIENT_SECRET", "")
  override val oauth2Uri: String = sys.env.get("ARCANE_FRAMEWORK__S3_CATALOG_AUTH_CLIENT_URI") match
    case Some(uri) => uri
    case None => throw InstantiationError("ARCANE_FRAMEWORK__S3_CATALOG_AUTH_CLIENT_URI environment variable is not set")

  override val oauth2Scope: String = sys.env.get("ARCANE_FRAMEWORK__S3_CATALOG_AUTH_SCOPE") match
    case Some(scope) => scope
    case None => throw InstantiationError("ARCANE_FRAMEWORK__S3_CATALOG_AUTH_SCOPE environment variable is not set")

  override val oauth2StaticToken: String = sys.env.getOrElse("ARCANE_FRAMEWORK__S3_CATALOG_AUTH_STATIC_TOKEN", "")
  override val oauth2TokenRefreshEnabled: Boolean = sys.env.getOrElse("ARCANE_FRAMEWORK__S3_CATALOG_AUTH_TOKEN_REFRESH_ENABLED", "false").toLowerCase == "true"
  override val oauth2SessionTimeoutMs: Long = sys.env.getOrElse("ARCANE_FRAMEWORK__S3_CATALOG_AUTH_SESSION_TIMEOUT_MILLIS", Duration.ofMinutes(55).toMillis.toString).toLong

  final val oAuth2Properties: Map[String, String] =
    val authProperties = if oauth2StaticToken != "" then Map(
      OAuth2Properties.TOKEN -> oauth2StaticToken,
      OAuth2Properties.OAUTH2_SERVER_URI -> oauth2Uri,
      OAuth2Properties.SCOPE -> oauth2Scope
    ) else Map(
      OAuth2Properties.CREDENTIAL -> credential,
      OAuth2Properties.OAUTH2_SERVER_URI -> oauth2Uri,
      OAuth2Properties.SCOPE -> oauth2Scope
    )
    
    authProperties ++ Map(
      OAuth2Properties.TOKEN_REFRESH_ENABLED -> oauth2TokenRefreshEnabled.toString.toLowerCase,
      CatalogProperties.AUTH_SESSION_TIMEOUT_MS -> oauth2SessionTimeoutMs.toString,
    )
