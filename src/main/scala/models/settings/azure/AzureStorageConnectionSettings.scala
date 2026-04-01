package com.sneaksanddata.arcane.framework
package models.settings.azure

import upickle.ReadWriter
import upickle.implicits.key

sealed trait CredentialType

/** SharedKeyCredential
  */
case class SharedKey(
    @key("accessKey") accessKeySetting: Option[String] = None
) derives ReadWriter:
  val accessKey: String = accessKeySetting.getOrElse(sys.env("ARCANE_FRAMEWORK__AZURE_STORAGE_ACCESS_KEY"))

case class SharedKeyImpl(sharedKey: SharedKey) extends CredentialType

/** Default credential chain
  */
case class Default() derives ReadWriter
case class DefaultImpl(default: Default) extends CredentialType

trait AzureStorageConnectionSettings:
  /** Storage account name
    */
  val accountName: String

  /** Storage endpoint
    */
  val endpoint: String

  /** Azure credential type to use
    */
  val credentialType: CredentialType

  /** Http client retry configuration
    */
  val httpClient: AzureHttpClientSettings

case class CredentialTypeSetting(
    sharedKey: Option[SharedKey] = None,
    credentialChain: Option[Default] = None
) derives ReadWriter:
  def resolveCredentialType: CredentialType = sharedKey
    .map(SharedKeyImpl(_))
    .getOrElse(
      credentialChain
        .map(DefaultImpl(_))
        .getOrElse(DefaultImpl(Default()))
    )

case class DefaultAzureStorageConnectionSettings(
    override val accountName: String,
    @key("endpoint") endpointSetting: Option[String],
    override val httpClient: DefaultAzureHttpClientSettings,
    @key("credentialType") credentialTypeSetting: CredentialTypeSetting
) extends AzureStorageConnectionSettings derives ReadWriter:
  override val credentialType: CredentialType = credentialTypeSetting.resolveCredentialType
  override val endpoint: String = endpointSetting.getOrElse(s"https://$accountName.blob.core.windows.net/")
