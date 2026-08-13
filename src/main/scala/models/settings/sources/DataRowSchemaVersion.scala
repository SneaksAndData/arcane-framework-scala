package com.sneaksanddata.arcane.framework
package models.settings.sources

import upickle.ReadWriter
import upickle.default.*

enum DataRowSchemaVersion derives ReadWriter:
  case V0, V1

  def usesCommonMergeKey: Boolean = this == V1
