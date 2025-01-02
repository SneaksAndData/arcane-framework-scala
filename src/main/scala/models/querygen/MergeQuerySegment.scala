package com.sneaksanddata.arcane.framework
package models.querygen

sealed trait MergeQuerySegment:
  val segmentPrefix: String
  val segmentCondition: Option[String]
  val segmentAction: String

  override def toString: String = Seq(segmentPrefix, segmentCondition.map(c => s"AND $c").getOrElse(""), "THEN", segmentAction).mkString(" ")

trait OnSegment extends MergeQuerySegment:
  final val segmentPrefix: String = "ON"
  final val segmentAction: String = ""
  final override def toString: String = Seq(segmentPrefix, segmentCondition.getOrElse("")).mkString(" ")

trait WhenMatchedUpdate extends MergeQuerySegment:
  final val segmentPrefix: String = "WHEN MATCHED"
  final val segmentAction: String = "UPDATE"
  val columns: Seq[String]

  override final def toString: String =
    val columnSet = columns.map(col => s"$col = ${MergeQueryCommons.SOURCE_ALIAS}.$col").mkString(",\n")
    Seq(segmentPrefix, segmentCondition.map(c => s"AND $c").getOrElse(""), "THEN", segmentAction, "SET\n", columnSet).mkString(" ")

trait WhenMatchedDelete extends MergeQuerySegment:
  final val segmentPrefix: String = "WHEN MATCHED"
  final val segmentAction: String = "DELETE"

trait WhenNotMatchedInsert extends MergeQuerySegment:
  val segmentPrefix: String = "WHEN NOT MATCHED"
  val segmentAction: String = "INSERT"
  val columns: Seq[String]

  override final def toString: String =
    val columnList = columns.map(col => s"$col").mkString(",")
    val columnSet = columns.map(col => s"${MergeQueryCommons.SOURCE_ALIAS}.$col").mkString(",\n")
    Seq(segmentPrefix, segmentCondition.map(c => s"AND $c").getOrElse(""), "THEN", segmentAction, s"($columnList)", "VALUES", s"($columnSet)").mkString(" ")

given WhenNotMatchedWildcardInsert: MergeQuerySegment with
  val segmentPrefix: String = "WHEN NOT MATCHED"
  val segmentCondition: Option[String] = None
  val segmentAction: String = "INSERT *"


object OnSegment:
  private def generateInClause(content: String, partName: String): String = s"${MergeQueryCommons.TARGET_ALIAS}.$partName IN ($content)"

  def apply(partitionValues: Map[String, List[String]], mergeKey: String): OnSegment = new OnSegment {
    override val segmentCondition: Option[String] =
      val baseCondition = s"${MergeQueryCommons.TARGET_ALIAS}.$mergeKey = ${MergeQueryCommons.SOURCE_ALIAS}.$mergeKey"
      partitionValues
        .map(values => generateInClause(values._2.map(part => s"'$part'").mkString(","), values._1)) match
        case partExpr if partExpr.nonEmpty => Some(s"$baseCondition AND ${partExpr.mkString(" AND ")}")
        case _ => Some(baseCondition)
  }