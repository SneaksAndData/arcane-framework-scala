package com.sneaksanddata.arcane.framework
package models.settings.database

object JdbcConnectionExtensions:
  extension (url: JdbcConnectionUrl)
    def withParameters(extraParameters: Map[String, String]): String = if extraParameters.isEmpty then url
    else
      Seq(
        url,
        extraParameters
          .map { case (key, value) =>
            s"$key=$value"
          }
          .mkString("&")
      ).mkString("&")
