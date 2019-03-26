package com.david.ts.producer

import java.util.Date

import com.david.ts.producer.Domain.SalesRecord
import com.david.ts.producer.ProducerMain.{ getClass, logger, s }
import org.apache.log4j.Logger

import scala.util.Random
object ProducerGenerator {
  lazy val logger = Logger.getLogger(getClass)
  val productCost: Map[Int, Double] = Map(1 -> 9.99, 2 -> 40, 3 -> 15, 4 -> 22, 5 -> 34)
  val shopsProb: Map[Int, Double] = Map(1 -> 0.5, 2 -> 0.5)

  val productsByShopProb: Map[Int, Map[Int, Double]] = Map(
    1 -> Map(1 -> 0.3, 2 -> 0.35, 3 -> 0.15, 4 -> 0.2),
    2 -> Map(1 -> 0.35, 2 -> 0.35, 3 -> 0.15, 4 -> 0.15)
  )

  val numberItemsProb: Map[Int, Double] = Map(1 -> 0.75, 2 -> 0.2, 3 -> 0.05)

  val randomDelay = new Random()

  import java.text.SimpleDateFormat

  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
  def generateRecords(numberRecords: Int, late: Boolean = false): Seq[SalesRecord] = {
    val randomPartners = new Random()

    val output = (1 to numberRecords).map { i =>
      val shopId: Int = randomValue(shopsProb)
      val product = randomValue(productsByShopProb(shopId))
      val numberOfItems = randomValue(numberItemsProb)
      val now = System.currentTimeMillis()
      val delay = randomDelay.nextInt(50)
      val timestamp = late match {
        case a if (a) =>
          logger.info("Late Record...")
          import java.util.Calendar
          val dNow = new Date()
          // Instantiate a Date object
          val cal = Calendar.getInstance
          cal.setTime(dNow)
          cal.add(Calendar.MINUTE, -5)
          cal.getTimeInMillis
        case _ => now
      }
      val time = sdf.format(new Date(timestamp))
      SalesRecord(time, shopId, product, numberOfItems, productCost(product) * numberOfItems)
    }
    output
  }

  private final def randomValue[A](dist: Map[A, Double]): A = {
    val p = scala.util.Random.nextDouble
    val it = dist.iterator
    var accum = 0.0
    while (it.hasNext) {
      val (item, itemProb) = it.next
      accum += itemProb
      if (accum >= p)
        return item // return so that we don't have to search through the whole distribution
    }
    sys.error(f"this should never happen") // needed so it will compile
  }
}
