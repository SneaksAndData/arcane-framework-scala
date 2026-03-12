package com.sneaksanddata.arcane.framework
package models.settings

import upickle.default.*
import upickle.implicits.key

/** Represents a field selection rule for a streaming batch. The field selection rule is used to determine which fields
  * should be included in the result set of a query.
  */
sealed trait FieldSelectionRule

/** All fields should be included in the result set.
  */
case class AllFields() derives ReadWriter

/** ADT composed with settings class for AllFields
  */
case class AllFieldsImpl(allFields: AllFields) extends FieldSelectionRule

/** Only the specified fields should be excluded from the result set.
  */
case class IncludeFields(fields: Set[String]) derives ReadWriter

/** ADT composed with settings class for IncludeFields
  */
case class IncludeFieldsImpl(includeFields: IncludeFields) extends FieldSelectionRule

/** All fields except the specified fields should be included in the result set.
  */
case class ExcludeFields(fields: Set[String]) derives ReadWriter

/** ADT composed with settings class for ExcludeFields
  */
case class ExcludeFieldsImpl(excludeFields: ExcludeFields) extends FieldSelectionRule

/** Proxy class that composes settings and makes them mutually exclusive
  */
case class FieldSelectionRuleSetting(
    all: Option[AllFields] = None,
    include: Option[IncludeFields] = None,
    exclude: Option[ExcludeFields] = None
) derives ReadWriter:
  def resolveSetting: FieldSelectionRule = all
    .map(AllFieldsImpl(_))
    .getOrElse(
      include
        .map(IncludeFieldsImpl(_))
        .getOrElse(
          exclude
            .map(ExcludeFieldsImpl(_))
            .getOrElse(AllFieldsImpl(AllFields()))
        )
    )

/** Marker trait for a field selection rule classes
  */
trait FieldSelectionRuleSettings:
  /** The field selection rule to use.
    */
  val rule: FieldSelectionRule

  /** The set of essential fields that must ALWAYS be included in the field selection rule. Fields from this list are
    * used in SQL queries and ALWAYS must be present in the result set. This list is provided by the Arcane streaming
    * plugin and should not be configurable.
    */
  val essentialFields: Set[String]

  val isServerSide: Boolean

case class DefaultFieldSelectionRuleSettings(
    override val essentialFields: Set[String],
    @key("rule") ruleSetting: FieldSelectionRuleSetting,
    override val isServerSide: Boolean
) extends FieldSelectionRuleSettings derives ReadWriter:
  override val rule: FieldSelectionRule = ruleSetting.resolveSetting
