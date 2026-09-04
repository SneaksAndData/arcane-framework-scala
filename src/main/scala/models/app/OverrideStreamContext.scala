package com.sneaksanddata.arcane.framework
package models.app

import models.settings.observability.{DefaultOverrideObservabilitySettings, OverrideObservabilitySettings}
import models.settings.sink.{DefaultOverrideSinkSettings, OverrideSinkSettings}
import models.settings.sources.OverrideStreamSourceSettings
import models.settings.staging.{DefaultOverrideStagingSettings, OverrideStagingSettings}
import models.settings.streaming.{
  DefaultOverrideStreamModeSettings,
  DefaultOverrideThroughputSettings,
  OverrideStreamModeSettings,
  OverrideThroughputSettings,
  StreamModeSettings
}

import upickle.ReadWriter

/** The stream mode that can be overridden by the stream override provided by the arcane operator with the environment
  * variable.
  */
trait OverrideStreamContext:
  val streamMode: Option[OverrideStreamModeSettings]
  val sink: Option[OverrideSinkSettings]
  val source: Option[OverrideStreamSourceSettings]
  val staging: Option[OverrideStagingSettings]
  val observability: Option[OverrideObservabilitySettings]
  val throughput: Option[OverrideThroughputSettings]

/** The stream mode that can be overridden by the stream override provided by the arcane operator with the environment
  * variable. This class should be extended with the specific source settings for the stream.
  */
abstract class DefaultOverrideStreamContext(
    override val streamMode: Option[DefaultOverrideStreamModeSettings] = None,
    override val sink: Option[DefaultOverrideSinkSettings] = None,
    override val staging: Option[DefaultOverrideStagingSettings] = None,
    override val observability: Option[DefaultOverrideObservabilitySettings] = None,
    override val throughput: Option[DefaultOverrideThroughputSettings] = None
) extends OverrideStreamContext

object OverrideStreamContext:
  def apply[Spec <: OverrideStreamContext](value: String)(implicit rw: ReadWriter[Spec]): Spec = upickle.read(value)
  def fromEnvironmentOverrides[Spec <: OverrideStreamContext](envVarName: String)(implicit
      rw: ReadWriter[Spec]
  ): Option[Spec] =
    sys.env.get(envVarName).map(env => apply(env))
