package com.sneaksanddata.arcane.framework
package models.settings.azure

import upickle.ReadWriter
import upickle.implicits.key

sealed trait CredentialType

/** SharedKeyCredential
  */
case class SharedKey(value: String) derives ReadWriter
case class SharedKeyImpl(sharedKey: SharedKey) extends CredentialType

/** Default credential chain
  */
case class Default() derives ReadWriter
case class DefaultImpl(default: Default) extends CredentialType

trait AzureStorageConnectionSettings:
  /** Storage account name
    */
  val accountName: String

  /** Optional storage endpoint
    */
  val endpoint: Option[String]

  /** Azure credential type to use
    */
  val credentialType: CredentialType

  /** Http client retry configuration
    */
  val httpClient: AzureHttpClientSettings

case class CredentialTypeSetting(
    sharedKey: Option[SharedKey],
    default: Option[Default]
) derives ReadWriter:
  def resolveCredentialType: CredentialType = sharedKey
    .map(SharedKeyImpl(_))
    .getOrElse(
      default
        .map(DefaultImpl(_))
        .getOrElse(DefaultImpl(Default()))
    )

case class DefaultAzureStorageConnectionSettings(
    override val accountName: String,
    override val endpoint: Option[String],
    override val httpClient: DefaultAzureHttpClientSettings,
    @key("credentialType") credentialTypeSetting: CredentialTypeSetting
) extends AzureStorageConnectionSettings derives ReadWriter:
  override val credentialType: CredentialType = credentialTypeSetting.resolveCredentialType
