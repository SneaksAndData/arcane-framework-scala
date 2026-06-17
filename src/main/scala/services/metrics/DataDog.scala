package com.sneaksanddata.arcane.framework
package services.metrics

import services.metrics.base.MetricTagProvider

import jnr.unixsocket.{UnixDatagramChannel, UnixSocketAddress}
import zio.metrics.connectors.datadog.DatadogPublisherConfig
import zio.metrics.connectors.statsd.{DatagramSocketConfig, SocketWriter, StatsdClient, statsdUDS}
import zio.metrics.connectors.{MetricsConfig, datadog}
import zio.metrics.jvm.DefaultJvmMetrics
import zio.{Chunk, Scope, URLayer, ZIO, ZIOAspect, ZLayer}

import java.nio.ByteBuffer
import java.nio.channels.DatagramChannel
import scala.collection.SortedMap
import scala.util.{Failure, Success, Try}

trait DogSocketWriter {
  def write(byteBuffer: ByteBuffer): Try[Long]
}

class UdsWriter(channel: DatagramChannel, address: UnixSocketAddress) extends DogSocketWriter {
  def write(byteBuffer: ByteBuffer): Try[Long] = Try(channel.send(byteBuffer, address).toLong)
}

class DogStatsd(writer: DogSocketWriter) extends StatsdClient {

  override def send(chunk: Chunk[Byte]): Long =
    writer.write(ByteBuffer.wrap(chunk.toArray)) match {
      case Success(value) =>
        value
      case Failure(_) =>
        0L
    }
}

/** DataDog metrics configuration and layer setup. This module provides the necessary configurations and layers to
  * integrate DataDog metrics into the application using ZIO.
  */
object DataDog {

  /** Environment required to run the DataDog metrics layer.
    */
  type Environment = DatagramSocketConfig & MetricsConfig & DatadogPublisherConfig

  object UdsPublisher {
    def udsWriter(path: String): ZIO[Scope, Throwable, DogSocketWriter] =
      for {
        channel <- ZIO.fromAutoCloseable(ZIO.attempt {
          val channel = UnixDatagramChannel.open()
          channel.configureBlocking(false)
          channel
        })
      } yield new UdsWriter(channel, new UnixSocketAddress(path))

    private def statsdClientLayer = ZLayer.scoped {
      for {
        config <- ZIO.service[DatagramSocketConfig]
        writer <- udsWriter(config.path).orDie
      } yield new DogStatsd(writer)
    }

    /** Layer that provides the DataDog metrics configuration.
      */
    val layer: ZLayer[Environment, Nothing, Unit] = statsdClientLayer >>> datadog.live

    val jvmLayer = ZLayer {
      for tagProvider <- ZIO.service[MetricTagProvider]
      yield (DefaultJvmMetrics.liveV2 >>> statsdUDS >>> datadog.live).build @@ ZIOAspect.tagged(
        Option(tagProvider.getTags).getOrElse(SortedMap.empty[String, String]).toList*
      )
    }

  }
}
