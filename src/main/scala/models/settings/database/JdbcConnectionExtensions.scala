package com.sneaksanddata.arcane.framework
package models.settings.database

object JdbcConnectionExtensions:
  extension (url: JdbcConnectionUrl)
    /** Attaches extra parameters as URL query parameters
      * @return
      */
    def withUrlParameters(extraParameters: Map[String, String]): String = if extraParameters.isEmpty then url
    else
      Seq(
        url,
        extraParameters
          .map { case (key, value) =>
            s"$key=$value"
          }
          .mkString("&")
      ).mkString("&")

    /** Attaches extra parameters as connection string parameters with `;` separator
      */
    def withConnectionParameters(extraParameters: Map[String, String]): String = if extraParameters.isEmpty then url
    else
      Seq(
        url,
        extraParameters
          .map { case (key, value) =>
            s"$key=$value"
          }
          .mkString(";")
      ).mkString(";")
