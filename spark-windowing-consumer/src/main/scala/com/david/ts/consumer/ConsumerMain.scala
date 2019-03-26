package com.david.ts.consumer

import org.apache.log4j.Logger
import org.apache.spark._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._
import org.apache.spark.streaming._

object ConsumerMain {
  case class SalesRecord(transactionTimestamp: Long, shopId: Int, productId: Int, amount: Int, totalCost: Double)
  def main(args: Array[String]): Unit = {
    Thread.sleep(20000)
    lazy val logger = Logger.getLogger(getClass)
    logger.info(s"Starting Main")
    val kafkaEndpoint = args(0)
    logger.info(s"Connecting to kafka endpoint $kafkaEndpoint")

    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.SparkSession

    val spark = SparkSession
      .builder
      .appName("StructuredConsumerWindowing")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    val schema = StructType(
      List(
        StructField("transactionTimestamp", TimestampType, true),
        StructField("shopId", IntegerType, true),
        StructField("productId", IntegerType, true),
        StructField("amount", IntegerType, true),
        StructField("totalCost", DoubleType, true)
      )
    )
    val inputStream = spark
      .readStream.format("kafka")
      .option("kafka.bootstrap.servers", kafkaEndpoint)
      .option("auto.offset.reset", "latest")
      .option("value.deserializer", "StringDeserializer")
      .option("subscribe", "shops_records")
      .load
    inputStream.printSchema()

    val initial = inputStream.selectExpr("CAST(value AS STRING)").toDF("value")
    initial.printSchema()
    initial.select(from_json($"value", schema)).printSchema()

    val aggregation = initial.select(from_json($"value", schema).alias("tmp")).select("tmp.*")
    aggregation.printSchema()

    val windows = aggregation
      .withWatermark("transactionTimestamp", "5 minutes")
      .groupBy(window($"transactionTimestamp", "1 minute", "1 minute"), $"shopId")
    val aggregatedDF = windows.agg(sum("totalCost"), count("*"))

    val dfcount = aggregatedDF.writeStream.outputMode("complete").option("truncate", false).format("console").start()

    dfcount.awaitTermination()

    logger.info(s"End Consumer")
  }
}

/*
| 2019-03-24 09:35:05 INFO  BlockManagerMasterEndpoint:54 - Registering block manager 10.0.0.5:44199 with 366.3 MB RAM, BlockManagerId(0, 10.0.0.5, 44199, None)
| root
|  |-- key: binary (nullable = true)
|  |-- value: binary (nullable = true)
|  |-- topic: string (nullable = true)
|  |-- partition: integer (nullable = true)
|  |-- offset: long (nullable = true)
|  |-- timestamp: timestamp (nullable = true)
|  |-- timestampType: integer (nullable = true)
|
| root
|  |-- value: string (nullable = true)
|
| root
|  |-- jsontostructs(value): struct (nullable = true)
|  |    |-- transactionTimestamp: timestamp (nullable = true)
|  |    |-- shopId: integer (nullable = true)
|  |    |-- productId: integer (nullable = true)
|  |    |-- amount: integer (nullable = true)
|  |    |-- totalCost: double (nullable = true)


|
| root
|  |-- transactionTimestamp: timestamp (nullable = true)
|  |-- shopId: integer (nullable = true)
|  |-- productId: integer (nullable = true)
|  |-- amount: integer (nullable = true)
|  |-- totalCost: double (nullable = true)



| -------------------------------------------
| Batch: 100
| -------------------------------------------
| +--------------------+------+------------------+--------+
| |              window|shopId|    sum(totalCost)|count(1)|
| +--------------------+------+------------------+--------+
| |[2019-03-25 23:14...|    10| 38366.32000000001|    1200|
| |[2019-03-25 23:12...|     6|           5320.32|     163|
| |[2019-03-25 23:12...|    10| 29799.79999999999|     952|
| |[2019-03-25 23:13...|    10|          68890.97|    2168|
| |[2019-03-25 23:08...|     5|            247.99|       7|
| |[2019-03-25 23:08...|    10|           1354.96|      48|
| |[2019-03-25 23:12...|     5|           4262.43|     145|
| |[2019-03-25 23:09...|     5|             194.0|       5|
| |[2019-03-25 23:14...|     1|1357.8200000000002|      44|
| |[2019-03-25 23:14...|     6|7737.2300000000005|     226|
| |[2019-03-25 23:07...|     1|              22.0|       1|
| |[2019-03-25 23:14...|     5|5006.5099999999975|     146|
| |[2019-03-25 23:09...|     6|151.98000000000002|       5|
| |[2019-03-25 23:13...|     1|           2379.74|      75|
| |[2019-03-25 23:08...|     6|            300.97|      10|
| |[2019-03-25 23:07...|     5|59.980000000000004|       3|
| |[2019-03-25 23:13...|     6|14150.499999999996|     397|
| |[2019-03-25 23:12...|     1|            651.94|      21|
| |[2019-03-25 23:09...|    10|            701.97|      21|
| |[2019-03-25 23:09...|     1|             146.0|       3|
| +--------------------+------+------------------+--------+
| only showing top 20 rows
|



| -------------------------------------------
| Batch: 26
| -------------------------------------------
| +------------------------------------------+------+--------------+--------+
| |window                                    |shopId|sum(totalCost)|count(1)|
| +------------------------------------------+------+--------------+--------+
| |[2019-03-26 07:57:00, 2019-03-26 07:58:00]|2     |9.99          |1       |
| +------------------------------------------+------+--------------+--------+
|
| -------------------------------------------
| Batch: 27
| -------------------------------------------
| +------------------------------------------+------+--------------+--------+
| |window                                    |shopId|sum(totalCost)|count(1)|
| +------------------------------------------+------+--------------+--------+
| |[2019-03-26 08:02:00, 2019-03-26 08:03:00]|1     |257.98        |8       |
| +------------------------------------------+------+--------------+--------+
|
| -------------------------------------------
| Batch: 28
| -------------------------------------------
| +------------------------------------------+------+--------------+--------+
| |window                                    |shopId|sum(totalCost)|count(1)|
| +------------------------------------------+------+--------------+--------+
| |[2019-03-26 08:02:00, 2019-03-26 08:03:00]|1     |337.98        |10      |
| +------------------------------------------+------+--------------+--------+
|
| -------------------------------------------
| Batch: 29
| -------------------------------------------
| +------------------------------------------+------+--------------+--------+
| |window                                    |shopId|sum(totalCost)|count(1)|
| +------------------------------------------+------+--------------+--------+
| |[2019-03-26 08:02:00, 2019-03-26 08:03:00]|1     |352.98        |11      |
| |[2019-03-26 08:02:00, 2019-03-26 08:03:00]|2     |194.0         |6       |
| +------------------------------------------+------+--------------+--------+
|
| -------------------------------------------
| Batch: 30
| -------------------------------------------
| +------------------------------------------+------+--------------+--------+
| |window                                    |shopId|sum(totalCost)|count(1)|
| +------------------------------------------+------+--------------+--------+
| |[2019-03-26 08:02:00, 2019-03-26 08:03:00]|1     |432.98        |12      |
| |[2019-03-26 08:02:00, 2019-03-26 08:03:00]|2     |203.99        |7       |
| +------------------------------------------+------+--------------+--------+
|
| -------------------------------------------
| Batch: 31
| -------------------------------------------
| +------------------------------------------+------+--------------+--------+
| |window                                    |shopId|sum(totalCost)|count(1)|
| +------------------------------------------+------+--------------+--------+
| |[2019-03-26 08:02:00, 2019-03-26 08:03:00]|1     |476.98        |13      |
| |[2019-03-26 08:02:00, 2019-03-26 08:03:00]|2     |283.99        |8       |
| +------------------------------------------+------+--------------+--------+
|
| -------------------------------------------
| Batch: 32
| -------------------------------------------
| +------------------------------------------+------+--------------+--------+
| |window                                    |shopId|sum(totalCost)|count(1)|
| +------------------------------------------+------+--------------+--------+
| |[2019-03-26 08:02:00, 2019-03-26 08:03:00]|1     |506.98        |15      |
| +------------------------------------------+------+--------------+--------+
|
| -------------------------------------------
| Batch: 33
| -------------------------------------------
| +------------------------------------------+------+--------------+--------+
| |window                                    |shopId|sum(totalCost)|count(1)|
| +------------------------------------------+------+--------------+--------+
| |[2019-03-26 08:03:00, 2019-03-26 08:04:00]|2     |22.0          |1       |
| +------------------------------------------+------+--------------+--------+








| -------------------------------------------
| Batch: 19
| -------------------------------------------
| +------------------------------------------+------+------------------+--------+
| |window                                    |shopId|sum(totalCost)    |count(1)|
| +------------------------------------------+------+------------------+--------+
| |[2019-03-26 09:02:00, 2019-03-26 09:03:00]|2     |657.8800000000001 |24      |
| |[2019-03-26 09:01:00, 2019-03-26 09:02:00]|2     |1758.8000000000002|55      |
| |[2019-03-26 09:02:00, 2019-03-26 09:03:00]|1     |790.95            |26      |
| |[2019-03-26 09:01:00, 2019-03-26 09:02:00]|1     |1645.8400000000001|45      |
| +------------------------------------------+------+------------------+--------+
|
| -------------------------------------------
| Batch: 20
| -------------------------------------------
| +------------------------------------------+------+------------------+--------+
| |window                                    |shopId|sum(totalCost)    |count(1)|
| +------------------------------------------+------+------------------+--------+
| |[2019-03-26 09:02:00, 2019-03-26 09:03:00]|2     |753.8800000000001 |27      |
| |[2019-03-26 09:01:00, 2019-03-26 09:02:00]|2     |1758.8000000000002|55      |
| |[2019-03-26 09:02:00, 2019-03-26 09:03:00]|1     |974.9200000000001 |33      |
| |[2019-03-26 09:01:00, 2019-03-26 09:02:00]|1     |1645.8400000000001|45      |
| +------------------------------------------+------+------------------+--------+
|
| -------------------------------------------
| Batch: 21
| -------------------------------------------
| +------------------------------------------+------+------------------+--------+
| |window                                    |shopId|sum(totalCost)    |count(1)|
| +------------------------------------------+------+------------------+--------+
| |[2019-03-26 09:02:00, 2019-03-26 09:03:00]|2     |843.8700000000001 |29      |
| |[2019-03-26 09:01:00, 2019-03-26 09:02:00]|2     |1758.8000000000002|55      |
| |[2019-03-26 09:02:00, 2019-03-26 09:03:00]|1     |1138.8700000000001|41      |
| |[2019-03-26 09:01:00, 2019-03-26 09:02:00]|1     |1645.8400000000001|45      |



TESTING WATERMARK AND UPDATE



park-windowing_producer.1.zyz2knsdnxy2@dave    |  INFO [default-akka.actor.default-dispatcher-2] (ProducerMain.scala:48) - Producing record: SalesRecord(2019-03-26 08:50:30.174,1,2,1,40.0)
spark-windowing_producer.1.zyz2knsdnxy2@dave    |  INFO [default-akka.actor.default-dispatcher-2] (ProducerMain.scala:48) - Producing record: SalesRecord(2019-03-26 08:50:30.174,1,3,3,45.0)
spark-windowing_producer.1.zyz2knsdnxy2@dave    |  INFO [default-akka.actor.default-dispatcher-2] (ProducerMain.scala:48) - Producing record: SalesRecord(2019-03-26 08:51:30.174,1,2,1,40.0)
spark-windowing_producer.1.zyz2knsdnxy2@dave    |  INFO [default-akka.actor.default-dispatcher-2] (ProducerMain.scala:48) - Producing record: SalesRecord(2019-03-26 08:51:30.174,1,4,1,22.0)
spark-windowing_producer.1.zyz2knsdnxy2@dave    |  INFO [default-akka.actor.default-dispatcher-2] (ProducerMain.scala:48) - Producing record: SalesRecord(2019-03-26 08:52:30.174,1,2,1,40.0)
spark-windowing_producer.1.zyz2knsdnxy2@dave    |  INFO [default-akka.actor.default-dispatcher-2] (ProducerMain.scala:48) - Producing record: SalesRecord(2019-03-26 08:52:30.174,1,2,2,80.0)
spark-windowing_producer.1.zyz2knsdnxy2@dave    |  INFO [default-akka.actor.default-dispatcher-2] (ProducerMain.scala:48) - Producing record: SalesRecord(2019-03-26 08:53:30.174,2,2,1,40.0)
spark-windowing_producer.1.zyz2knsdnxy2@dave    |  INFO [default-akka.actor.default-dispatcher-2] (ProducerMain.scala:48) - Producing record: SalesRecord(2019-03-26 08:53:30.175,2,2,1,40.0)
spark-windowing_producer.1.zyz2knsdnxy2@dave    |  INFO [default-akka.actor.default-dispatcher-2] (ProducerMain.scala:48) - Producing record: SalesRecord(2019-03-26 08:54:30.174,2,3,1,15.0)
spark-windowing_producer.1.zyz2knsdnxy2@dave    |  INFO [default-akka.actor.default-dispatcher-2] (ProducerMain.scala:48) - Producing record: SalesRecord(2019-03-26 08:54:30.174,1,2,1,40.0)
spark-windowing_producer.1.zyz2knsdnxy2@dave    |  INFO [default-akka.actor.default-dispatcher-2] (ProducerMain.scala:48) - Producing record: SalesRecord(2019-03-26 08:55:30.175,1,2,1,40.0)
spark-windowing_producer.1.zyz2knsdnxy2@dave    |  INFO [default-akka.actor.default-dispatcher-2] (ProducerMain.scala:48) - Producing record: SalesRecord(2019-03-26 08:55:30.175,1,1,2,19.98)
spark-windowing_producer.1.zyz2knsdnxy2@dave    |  INFO [default-akka.actor.default-dispatcher-2] (ProducerMain.scala:41) - Producing a late Record...
spark-windowing_producer.1.zyz2knsdnxy2@dave    |  INFO [default-akka.actor.default-dispatcher-2] (ProducerGenerator.scala:38) - Late Record...
spark-windowing_producer.1.zyz2knsdnxy2@dave    |  INFO [default-akka.actor.default-dispatcher-2] (ProducerMain.scala:48) - Producing record: SalesRecord(2019-03-26 08:51:30.174,1,4,2,44.0)
spark-windowing_producer.1.zyz2knsdnxy2@dave    |  INFO [default-akka.actor.default-dispatcher-5] (ProducerMain.scala:48) - Producing record: SalesRecord(2019-03-26 08:57:30.175,1,2,2,80.0)
spark-windowing_producer.1.zyz2knsdnxy2@dave    |  INFO [default-akka.actor.default-dispatcher-5] (ProducerMain.scala:48) - Producing record: SalesRecord(2019-03-26 08:57:30.175,1,4,1,22.0)







spark-windowing_spark-consumer.1.vrt6g47bceoa@dave    | -------------------------------------------
spark-windowing_spark-consumer.1.vrt6g47bceoa@dave    | Batch: 1
spark-windowing_spark-consumer.1.vrt6g47bceoa@dave    | -------------------------------------------
spark-windowing_spark-consumer.1.vrt6g47bceoa@dave    | +------------------------------------------+------+--------------+--------+
spark-windowing_spark-consumer.1.vrt6g47bceoa@dave    | |window                                    |shopId|sum(totalCost)|count(1)|
spark-windowing_spark-consumer.1.vrt6g47bceoa@dave    | +------------------------------------------+------+--------------+--------+
spark-windowing_spark-consumer.1.vrt6g47bceoa@dave    | |[2019-03-26 08:50:00, 2019-03-26 08:51:00]|1     |85.0          |2       |
spark-windowing_spark-consumer.1.vrt6g47bceoa@dave    | +------------------------------------------+------+--------------+--------+
spark-windowing_spark-consumer.1.vrt6g47bceoa@dave    |
spark-windowing_spark-consumer.1.vrt6g47bceoa@dave    | -------------------------------------------
spark-windowing_spark-consumer.1.vrt6g47bceoa@dave    | Batch: 2
spark-windowing_spark-consumer.1.vrt6g47bceoa@dave    | -------------------------------------------
spark-windowing_spark-consumer.1.vrt6g47bceoa@dave    | +------------------------------------------+------+--------------+--------+
spark-windowing_spark-consumer.1.vrt6g47bceoa@dave    | |window                                    |shopId|sum(totalCost)|count(1)|
spark-windowing_spark-consumer.1.vrt6g47bceoa@dave    | +------------------------------------------+------+--------------+--------+
spark-windowing_spark-consumer.1.vrt6g47bceoa@dave    | |[2019-03-26 08:51:00, 2019-03-26 08:52:00]|1     |62.0          |2       |
spark-windowing_spark-consumer.1.vrt6g47bceoa@dave    | +------------------------------------------+------+--------------+--------+
spark-windowing_spark-consumer.1.vrt6g47bceoa@dave    |
spark-windowing_spark-consumer.1.vrt6g47bceoa@dave    | -------------------------------------------
spark-windowing_spark-consumer.1.vrt6g47bceoa@dave    | Batch: 3
spark-windowing_spark-consumer.1.vrt6g47bceoa@dave    | -------------------------------------------
spark-windowing_spark-consumer.1.vrt6g47bceoa@dave    | +------------------------------------------+------+--------------+--------+
spark-windowing_spark-consumer.1.vrt6g47bceoa@dave    | |window                                    |shopId|sum(totalCost)|count(1)|
spark-windowing_spark-consumer.1.vrt6g47bceoa@dave    | +------------------------------------------+------+--------------+--------+
spark-windowing_spark-consumer.1.vrt6g47bceoa@dave    | |[2019-03-26 08:52:00, 2019-03-26 08:53:00]|1     |120.0         |2       |
spark-windowing_spark-consumer.1.vrt6g47bceoa@dave    | +------------------------------------------+------+--------------+--------+
spark-windowing_spark-consumer.1.vrt6g47bceoa@dave    |
spark-windowing_spark-consumer.1.vrt6g47bceoa@dave    | -------------------------------------------
spark-windowing_spark-consumer.1.vrt6g47bceoa@dave    | Batch: 4
spark-windowing_spark-consumer.1.vrt6g47bceoa@dave    | -------------------------------------------
spark-windowing_spark-consumer.1.vrt6g47bceoa@dave    | +------------------------------------------+------+--------------+--------+
spark-windowing_spark-consumer.1.vrt6g47bceoa@dave    | |window                                    |shopId|sum(totalCost)|count(1)|
spark-windowing_spark-consumer.1.vrt6g47bceoa@dave    | +------------------------------------------+------+--------------+--------+
spark-windowing_spark-consumer.1.vrt6g47bceoa@dave    | |[2019-03-26 08:53:00, 2019-03-26 08:54:00]|2     |40.0          |1       |
spark-windowing_spark-consumer.1.vrt6g47bceoa@dave    | +------------------------------------------+------+--------------+--------+
spark-windowing_spark-consumer.1.vrt6g47bceoa@dave    |
spark-windowing_spark-consumer.1.vrt6g47bceoa@dave    | -------------------------------------------
spark-windowing_spark-consumer.1.vrt6g47bceoa@dave    | Batch: 5
spark-windowing_spark-consumer.1.vrt6g47bceoa@dave    | -------------------------------------------
spark-windowing_spark-consumer.1.vrt6g47bceoa@dave    | +------------------------------------------+------+--------------+--------+
spark-windowing_spark-consumer.1.vrt6g47bceoa@dave    | |window                                    |shopId|sum(totalCost)|count(1)|
spark-windowing_spark-consumer.1.vrt6g47bceoa@dave    | +------------------------------------------+------+--------------+--------+
spark-windowing_spark-consumer.1.vrt6g47bceoa@dave    | |[2019-03-26 08:53:00, 2019-03-26 08:54:00]|2     |80.0          |2       |
spark-windowing_spark-consumer.1.vrt6g47bceoa@dave    | +------------------------------------------+------+--------------+--------+
spark-windowing_spark-consumer.1.vrt6g47bceoa@dave    |
spark-windowing_spark-consumer.1.vrt6g47bceoa@dave    | -------------------------------------------
spark-windowing_spark-consumer.1.vrt6g47bceoa@dave    | Batch: 6
spark-windowing_spark-consumer.1.vrt6g47bceoa@dave    | -------------------------------------------
spark-windowing_spark-consumer.1.vrt6g47bceoa@dave    | +------------------------------------------+------+--------------+--------+
spark-windowing_spark-consumer.1.vrt6g47bceoa@dave    | |window                                    |shopId|sum(totalCost)|count(1)|
spark-windowing_spark-consumer.1.vrt6g47bceoa@dave    | +------------------------------------------+------+--------------+--------+
spark-windowing_spark-consumer.1.vrt6g47bceoa@dave    | |[2019-03-26 08:54:00, 2019-03-26 08:55:00]|2     |15.0          |1       |
spark-windowing_spark-consumer.1.vrt6g47bceoa@dave    | |[2019-03-26 08:54:00, 2019-03-26 08:55:00]|1     |40.0          |1       |
spark-windowing_spark-consumer.1.vrt6g47bceoa@dave    | +------------------------------------------+------+--------------+--------+
spark-windowing_spark-consumer.1.vrt6g47bceoa@dave    |
spark-windowing_spark-consumer.1.vrt6g47bceoa@dave    | -------------------------------------------
spark-windowing_spark-consumer.1.vrt6g47bceoa@dave    | Batch: 7
spark-windowing_spark-consumer.1.vrt6g47bceoa@dave    | -------------------------------------------
spark-windowing_spark-consumer.1.vrt6g47bceoa@dave    | +------------------------------------------+------+------------------+--------+
spark-windowing_spark-consumer.1.vrt6g47bceoa@dave    | |window                                    |shopId|sum(totalCost)    |count(1)|
spark-windowing_spark-consumer.1.vrt6g47bceoa@dave    | +------------------------------------------+------+------------------+--------+
spark-windowing_spark-consumer.1.vrt6g47bceoa@dave    | |[2019-03-26 08:55:00, 2019-03-26 08:56:00]|1     |59.980000000000004|2       |
spark-windowing_spark-consumer.1.vrt6g47bceoa@dave    | +------------------------------------------+------+------------------+--------+
spark-windowing_spark-consumer.1.vrt6g47bceoa@dave    |
spark-windowing_spark-consumer.1.vrt6g47bceoa@dave    | -------------------------------------------
spark-windowing_spark-consumer.1.vrt6g47bceoa@dave    | Batch: 8
spark-windowing_spark-consumer.1.vrt6g47bceoa@dave    | -------------------------------------------
spark-windowing_spark-consumer.1.vrt6g47bceoa@dave    | +------------------------------------------+------+--------------+--------+
spark-windowing_spark-consumer.1.vrt6g47bceoa@dave    | |window                                    |shopId|sum(totalCost)|count(1)|
spark-windowing_spark-consumer.1.vrt6g47bceoa@dave    | +------------------------------------------+------+--------------+--------+
spark-windowing_spark-consumer.1.vrt6g47bceoa@dave    | |[2019-03-26 08:51:00, 2019-03-26 08:52:00]|1     |106.0         |3       |
spark-windowing_spark-consumer.1.vrt6g47bceoa@dave    | +------------------------------------------+------+--------------+--------+
spark-windowing_spark-consumer.1.vrt6g47bceoa@dave    |






 */ 