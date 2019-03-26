package com.david.ts.producer

import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.Producer
import akka.stream._
import akka.stream.scaladsl.Source
import com.david.ts.producer.Domain.SalesRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{ ByteArraySerializer, StringSerializer }
import org.apache.log4j.Logger

import scala.concurrent.duration._
import scala.util.Random

object ProducerMain extends App {
  Thread.sleep(10000)
  import akka.actor._
  lazy val logger = Logger.getLogger(getClass)
  logger.info(s"Starting Producer2")

  implicit val system = ActorSystem()
  implicit val mater = ActorMaterializer()
  import io.circe._, io.circe.generic.auto._, io.circe.parser._, io.circe.syntax._
  val kafkaEndpoint = System.getProperty("kafka_endpoint", "localhost:9092")
  logger.info(s"Connecting to kafka endpoint $kafkaEndpoint")
  val producerSettings = ProducerSettings(system, new ByteArraySerializer, new StringSerializer)
    .withBootstrapServers(kafkaEndpoint).withCloseTimeout(5 minutes)

  val random = new Random()
  var i = -1
  val s = Source
    .tick(0.seconds, 5.seconds, "").map { _ =>
      i = i + 1
      random.nextInt(1000)
    }
  val testWaterMark = false
  s.mapConcat { _ =>
    (testWaterMark, i) match {
      case (false, _) => ProducerGenerator.generateRecords(10).toList
      case (_, a) if (a % 42 == 0) =>
        logger.info("Producing a late Record...")
        ProducerGenerator.generateRecords(1, late = true).toList
      case (_, a) if (a % 6 == 0) => ProducerGenerator.generateRecords(2).toList
      case _ => List[SalesRecord]()
    }

  }.map { record =>
    logger.info(s"Producing record: $record")
    new ProducerRecord[Array[Byte], String]("shops_records", record.asJson.toString)
  }.runWith(Producer.plainSink(producerSettings))
}
