package com.sneaksanddata.arcane.framework
package services.pushstream

import services.streaming.base.DefaultStreamDataProvider

import com.sneaksanddata.arcane.framework.models.settings.streaming.ChangeCaptureSettings
import com.sneaksanddata.arcane.framework.services.metrics.DeclaredMetrics

class PushStreamStreamingDataProvider(
                                       dataProvider: PushStreamSourceDataProvider,
                                       settings: ChangeCaptureSettings,
                                       declaredMetrics: DeclaredMetrics
                                     ) extends DefaultStreamDataProvider(
  dataProvider, settings, declaredMetrics
)
