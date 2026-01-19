//package com.sneaksanddata.arcane.framework
//package services.streaming.base
//
//import models.settings.{BackfillSettings, VersionedDataGraphBuilderSettings}
//import services.blobsource.providers.BlobSourceDataProvider
//
//import com.sneaksanddata.arcane.framework.models.app.StreamContext
//import com.sneaksanddata.arcane.framework.models.schemas.DataRow
//import zio.stream.ZStream
//
//class DefaultStreamDataProvider(    dataProvider: BlobSourceDataProvider,
//                                    settings: VersionedDataGraphBuilderSettings,
//
//                                    backfillSettings: BackfillSettings,
//                                    streamContext: StreamContext) extends StreamDataProvider:
//  
//  override def stream: ZStream[Any, Throwable, DataRow] = if streamContext.IsBackfilling then {
//    // pending https://github.com/SneaksAndData/arcane-framework-scala/issues/181 to avoid asInstanceOf
//    dataProvider.requestBackfill
//  } else
//    ZStream
//      .unfoldZIO(dataProvider.firstVersion)(nextVersion)
//      .flatMap {
//        case (newVersion, previousVersion) if newVersion.versionNumber > previousVersion.versionNumber =>
//          dataProvider.requestChanges(previousVersion)
//        case _ => ZStream.empty
//      }
//      .map(_.asInstanceOf[DataRow])
