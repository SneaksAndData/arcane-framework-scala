import zio.metrics.{MetricKey, MetricKeyType}
import zio.metrics.connectors.MetricsConfig
import zio.metrics.connectors.datadog.{DataDogEventProcessor, DataDogListener, DatadogConfig}
import zio.{Chunk, Scope, Unsafe, ZIO, ZLayer}
import zio.metrics.connectors.statsd.{MetricClient, StatsdClient, StatsdConfig}
import zio.metrics.connectors.internal.MetricsClient
import zio.internal.RingBuffer

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.DatagramChannel
import scala.util.{Failure, Success, Try}

object StatsdClient {

  private class Live(channel: DatagramChannel) extends StatsdClient {

    override def send(chunk: Chunk[Byte]): Long =
      write(chunk.toArray)

    private def write(ab: Array[Byte]): Long =
      Try(channel.write(ByteBuffer.wrap(ab)).toLong) match {
        case Success(value) =>
          // println(s"Sent UDP data [$value]")
          value
        case Failure(_)     =>
          // t.printStackTrace()
          0L
      }

  }

  private def channelZIO(host: String, port: Int): ZIO[Scope, Throwable, DatagramChannel] =
    ZIO.fromAutoCloseable(ZIO.attempt {
      val channel = DatagramChannel.open()
      channel.connect(new InetSocketAddress(host, port))
      channel.configureBlocking(false)
      channel
    })

  def make: ZIO[Scope & StatsdConfig, Nothing, StatsdClient] =
    for {
      config  <- ZIO.service[StatsdConfig]
      channel <- channelZIO(config.host, config.port).orDie
      client   = new Live(channel)
    } yield client

}

object DataDog:
  
  val layer: ZLayer[DatadogConfig & MetricsConfig & StatsdClient, Nothing, Unit] =
    ZLayer.scoped(
      for {
        config <- ZIO.service[DatadogConfig]
        
        clt <- ZIO.service[StatsdClient]
        
        queue = RingBuffer.apply[(MetricKey[MetricKeyType.Histogram], Double)](config.maxQueueSize)
        listener = new DataDogListener(queue)
        _ <- Unsafe.unsafe(unsafe =>
          ZIO.acquireRelease(ZIO.succeed(MetricClient.addListener(listener)(unsafe)))(_ =>
            ZIO.succeed(MetricClient.removeListener(listener)(unsafe)),
          ),
        )
        _ <- DataDogEventProcessor.make(clt, queue)
        _ <- MetricsClient.make(datadogHandler(clt, config))
      } yield (),
    )

    private def getEntityChangeData(startDate: OffsetDateTime): ZStream[Any, Throwable, SchemaEnrichedBlob] = for
      eligibleDate <- reader.getEligibleDates(storagePath, startDate)
      batchBlob <- enrichWithSchema(eligibleDate).filter(seb => !seb.blob.name.endsWith("/1.csv")).concat(enrichWithSchema(eligibleDate).filter(seb => seb.blob.name.endsWith("/1.csv")))
    yield batchBlob