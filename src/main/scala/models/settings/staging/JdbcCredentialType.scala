package com.sneaksanddata.arcane.framework
package models.settings.staging

import upickle.ReadWriter
import upickle.implicits.key

/** Credential type for the JDBC connection
  */
sealed trait JdbcCredentialType

/** Standard username-password credential
  */
case class BasicCredential(
    @key("user") userSetting: Option[String] = None,
    @key("password") passwordSetting: Option[String] = None
) derives ReadWriter:
  private val user: String     = userSetting.getOrElse(sys.env("ARCANE_FRAMEWORK__JDBC_MERGE_SERVICE_USER"))
  private val password: String = passwordSetting.getOrElse(sys.env("ARCANE_FRAMEWORK__JDBC_MERGE_SERVICE_PASSWORD"))

  def asParameters: Map[String, String] = Map(
    "user"     -> user,
    "password" -> password
  )

/** ADT proxy for BasicCredential
  */
case class BasicCredentialImpl(credential: BasicCredential) extends JdbcCredentialType

/** No auth credential
  */
case class AnonymousCredential() derives ReadWriter

/** ADT proxy for AnonymousCredential
  */
case class AnonymousCredentialImpl(credential: AnonymousCredential) extends JdbcCredentialType

/** Setting class proxy
  */
case class JdbcCredentialTypeSetting(
    basic: Option[BasicCredential] = None,
    anonymous: Option[AnonymousCredential] = None
) derives ReadWriter:
  def resolveCredentialType: JdbcCredentialType = basic
    .map(BasicCredentialImpl(_))
    .getOrElse(anonymous.map(AnonymousCredentialImpl(_)).getOrElse(AnonymousCredentialImpl(AnonymousCredential())))
