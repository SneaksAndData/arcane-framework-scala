package com.sneaksanddata.arcane.framework
package models.settings.sources

import upickle.ReadWriter
import upickle.default.*

enum DataRowSchemaVersion derives ReadWriter:
  case V0, V1

  def modifications: Seq[DataRowModification] = this match
    case V0 => Seq.empty
    case V1 => Seq(SurrogateMergeKeyImpl(SurrogateMergeKey()))
