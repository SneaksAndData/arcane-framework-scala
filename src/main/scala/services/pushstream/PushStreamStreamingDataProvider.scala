package com.sneaksanddata.arcane.framework
package services.pushstream

import models.settings.streaming.ChangeCaptureSettings
import services.metrics.DeclaredMetrics
import services.streaming.base.DefaultStreamDataProvider

class PushStreamStreamingDataProvider(
    dataProvider: PushStreamSourceDataProvider,
    settings: ChangeCaptureSettings,
    declaredMetrics: DeclaredMetrics
) extends DefaultStreamDataProvider(
      dataProvider,
      settings,
      declaredMetrics
    )
