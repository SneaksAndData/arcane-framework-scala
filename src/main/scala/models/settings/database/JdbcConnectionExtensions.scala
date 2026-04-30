package com.sneaksanddata.arcane.framework
package models.settings.database

import java.net.URI

object JdbcConnectionExtensions:
  extension (url: JdbcConnectionUrl)
    /** Attaches a default catalog as URL query path segment
      * @return
      */
    def withDefaultCatalog(catalog: String): String =
      // only allow this for base URI
      val uri = new URI(url)
      require(
        uri.isAbsolute && Option(uri.getFragment).isEmpty,
        "Cannot attach catalog to the provided JDBC URL. Please provide an absolute URL with an empty path."
      )
      s"$url/$catalog"

    /** Attaches a default schema as URL query path segment
      * @return
      */
    def withDefaultSchema(schema: String): String = s"$url/$schema"

    /** Attaches extra parameters as URL query parameters
      * @return
      */
    def withUrlParameters(extraParameters: Map[String, String]): String = if extraParameters.isEmpty then url
    else {
      val paramString = extraParameters
        .map { case (key, value) =>
          s"$key=$value"
        }
        .mkString("&")
      // check if URL has parameters
      Option(new URI(url).getQuery) match
        case Some(_) =>
          Seq(
            s"$url/?",
            paramString
          ).mkString("")
        case None =>
          Seq(
            url,
            paramString
          ).mkString("&")

    }

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
