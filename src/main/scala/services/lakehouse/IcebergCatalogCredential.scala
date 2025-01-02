package com.sneaksanddata.arcane.framework
package services.lakehouse

import org.apache.iceberg.rest.auth.OAuth2Properties

private trait IcebergCatalogCredential:
  val credential: String
  val oauth2Uri: String
  val oauth2Scope: String
  val oauth2InitToken: String

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

  override val oauth2InitToken: String = sys.env.getOrElse("ARCANE_FRAMEWORK__S3_CATALOG_AUTH_INIT_TOKEN", "")

  final val oAuth2Properties: Map[String, String] =
    if oauth2InitToken != "" then Map(
      OAuth2Properties.TOKEN -> oauth2InitToken,
      OAuth2Properties.OAUTH2_SERVER_URI -> oauth2Uri,
      OAuth2Properties.SCOPE -> oauth2Scope
    ) else Map(
        OAuth2Properties.CREDENTIAL -> credential,
        OAuth2Properties.OAUTH2_SERVER_URI -> oauth2Uri,
        OAuth2Properties.SCOPE -> oauth2Scope
      )
