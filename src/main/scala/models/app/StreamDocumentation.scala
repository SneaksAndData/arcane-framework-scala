package com.sneaksanddata.arcane.framework
package models.app

import zio.{IO, ZIO}

/** Documentation settings for the stream and target table metadata.
  *
  * @param pluginName
  *   The name of the plugin managing the stream.
  * @param pluginGithubRepoLink
  *   The link to the GitHub repository of the plugin.
  */
case class StreamDocumentation(
    pluginName: String,
    pluginGithubRepoLink: String
):
  def asComment: String = s"Table managed by $pluginName $pluginGithubRepoLink"

object StreamDocumentation:
  val fallbackPluginName: String     = "UNSPECIFIED_PLUGIN"
  val fallbackGithubRepoLink: String = "UNKNOWN_REPOSITORY"

  def fromEnvironment: IO[SecurityException, StreamDocumentation] = for
    pluginName <- zio.System.envOrElse("STREAMCONTEXT__PLUGIN_NAME", fallbackPluginName)
    pluginRepo <- zio.System.envOrElse("STREAMCONTEXT__PLUGIN_GITHUB_REPO_LINK", fallbackGithubRepoLink)
  yield StreamDocumentation(pluginName, pluginRepo)
