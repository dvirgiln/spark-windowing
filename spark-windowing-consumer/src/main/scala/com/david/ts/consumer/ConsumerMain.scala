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

    val df = spark
      .readStream

    val df2 = df.format("kafka")
      .option("kafka.bootstrap.servers", kafkaEndpoint)
      .option("auto.offset.reset", "latest")
      .option("value.deserializer", "StringDeserializer")
      .option("subscribe", "shops_records")
      .load
    df2.printSchema()

    val initial = df2.selectExpr("CAST(value AS STRING)").toDF("value")
    initial.printSchema()
    initial.select(from_json($"value", schema)).printSchema()

    val aggregation = initial.select(from_json($"value", schema).alias("tmp")).select("tmp.*")
    aggregation.printSchema()

    val windows = aggregation
      .withWatermark("transactionTimestamp", "5 minutes")
      .groupBy(window($"transactionTimestamp", "1 minute", "1 minute"), $"shopId")
    val bb = windows.agg(sum("totalCost"), count("*"))

    val dfcount = bb.writeStream.outputMode("complete").option("truncate", false).format("console").start()

    dfcount.awaitTermination()

    logger.info(s"End Consumer")
  }

}

/*
spark-windowing_spark-consumer.1.nyeaonutgdde@dave    | 2019-03-24 09:35:05 INFO  BlockManagerMasterEndpoint:54 - Registering block manager 10.0.0.5:44199 with 366.3 MB RAM, BlockManagerId(0, 10.0.0.5, 44199, None)
spark-windowing_spark-consumer.1.nyeaonutgdde@dave    | root
spark-windowing_spark-consumer.1.nyeaonutgdde@dave    |  |-- key: binary (nullable = true)
spark-windowing_spark-consumer.1.nyeaonutgdde@dave    |  |-- value: binary (nullable = true)
spark-windowing_spark-consumer.1.nyeaonutgdde@dave    |  |-- topic: string (nullable = true)
spark-windowing_spark-consumer.1.nyeaonutgdde@dave    |  |-- partition: integer (nullable = true)
spark-windowing_spark-consumer.1.nyeaonutgdde@dave    |  |-- offset: long (nullable = true)
spark-windowing_spark-consumer.1.nyeaonutgdde@dave    |  |-- timestamp: timestamp (nullable = true)
spark-windowing_spark-consumer.1.nyeaonutgdde@dave    |  |-- timestampType: integer (nullable = true)
spark-windowing_spark-consumer.1.nyeaonutgdde@dave    |
spark-windowing_spark-consumer.1.nyeaonutgdde@dave    | root
spark-windowing_spark-consumer.1.nyeaonutgdde@dave    |  |-- value: string (nullable = true)
spark-windowing_spark-consumer.1.nyeaonutgdde@dave    |
spark-windowing_spark-consumer.1.nyeaonutgdde@dave    | root
spark-windowing_spark-consumer.1.nyeaonutgdde@dave    |  |-- jsontostructs(value): struct (nullable = true)
spark-windowing_spark-consumer.1.nyeaonutgdde@dave    |  |    |-- transactionTimestamp: timestamp (nullable = true)
spark-windowing_spark-consumer.1.nyeaonutgdde@dave    |  |    |-- shopId: integer (nullable = true)
spark-windowing_spark-consumer.1.nyeaonutgdde@dave    |  |    |-- productId: integer (nullable = true)
spark-windowing_spark-consumer.1.nyeaonutgdde@dave    |  |    |-- amount: integer (nullable = true)
spark-windowing_spark-consumer.1.nyeaonutgdde@dave    |  |    |-- totalCost: double (nullable = true)


spark-windowing_spark-consumer.1.k87bbx8aq62o@dave    |
spark-windowing_spark-consumer.1.k87bbx8aq62o@dave    | root
spark-windowing_spark-consumer.1.k87bbx8aq62o@dave    |  |-- transactionTimestamp: timestamp (nullable = true)
spark-windowing_spark-consumer.1.k87bbx8aq62o@dave    |  |-- shopId: integer (nullable = true)
spark-windowing_spark-consumer.1.k87bbx8aq62o@dave    |  |-- productId: integer (nullable = true)
spark-windowing_spark-consumer.1.k87bbx8aq62o@dave    |  |-- amount: integer (nullable = true)
spark-windowing_spark-consumer.1.k87bbx8aq62o@dave    |  |-- totalCost: double (nullable = true)



park-windowing_spark-consumer.1.mb0vqi3yn8j5@dave    | -------------------------------------------
spark-windowing_spark-consumer.1.mb0vqi3yn8j5@dave    | Batch: 100
spark-windowing_spark-consumer.1.mb0vqi3yn8j5@dave    | -------------------------------------------
spark-windowing_spark-consumer.1.mb0vqi3yn8j5@dave    | +--------------------+------+------------------+--------+
spark-windowing_spark-consumer.1.mb0vqi3yn8j5@dave    | |              window|shopId|    sum(totalCost)|count(1)|
spark-windowing_spark-consumer.1.mb0vqi3yn8j5@dave    | +--------------------+------+------------------+--------+
spark-windowing_spark-consumer.1.mb0vqi3yn8j5@dave    | |[2019-03-25 23:14...|    10| 38366.32000000001|    1200|
spark-windowing_spark-consumer.1.mb0vqi3yn8j5@dave    | |[2019-03-25 23:12...|     6|           5320.32|     163|
spark-windowing_spark-consumer.1.mb0vqi3yn8j5@dave    | |[2019-03-25 23:12...|    10| 29799.79999999999|     952|
spark-windowing_spark-consumer.1.mb0vqi3yn8j5@dave    | |[2019-03-25 23:13...|    10|          68890.97|    2168|
spark-windowing_spark-consumer.1.mb0vqi3yn8j5@dave    | |[2019-03-25 23:08...|     5|            247.99|       7|
spark-windowing_spark-consumer.1.mb0vqi3yn8j5@dave    | |[2019-03-25 23:08...|    10|           1354.96|      48|
spark-windowing_spark-consumer.1.mb0vqi3yn8j5@dave    | |[2019-03-25 23:12...|     5|           4262.43|     145|
spark-windowing_spark-consumer.1.mb0vqi3yn8j5@dave    | |[2019-03-25 23:09...|     5|             194.0|       5|
spark-windowing_spark-consumer.1.mb0vqi3yn8j5@dave    | |[2019-03-25 23:14...|     1|1357.8200000000002|      44|
spark-windowing_spark-consumer.1.mb0vqi3yn8j5@dave    | |[2019-03-25 23:14...|     6|7737.2300000000005|     226|
spark-windowing_spark-consumer.1.mb0vqi3yn8j5@dave    | |[2019-03-25 23:07...|     1|              22.0|       1|
spark-windowing_spark-consumer.1.mb0vqi3yn8j5@dave    | |[2019-03-25 23:14...|     5|5006.5099999999975|     146|
spark-windowing_spark-consumer.1.mb0vqi3yn8j5@dave    | |[2019-03-25 23:09...|     6|151.98000000000002|       5|
spark-windowing_spark-consumer.1.mb0vqi3yn8j5@dave    | |[2019-03-25 23:13...|     1|           2379.74|      75|
spark-windowing_spark-consumer.1.mb0vqi3yn8j5@dave    | |[2019-03-25 23:08...|     6|            300.97|      10|
spark-windowing_spark-consumer.1.mb0vqi3yn8j5@dave    | |[2019-03-25 23:07...|     5|59.980000000000004|       3|
spark-windowing_spark-consumer.1.mb0vqi3yn8j5@dave    | |[2019-03-25 23:13...|     6|14150.499999999996|     397|
spark-windowing_spark-consumer.1.mb0vqi3yn8j5@dave    | |[2019-03-25 23:12...|     1|            651.94|      21|
spark-windowing_spark-consumer.1.mb0vqi3yn8j5@dave    | |[2019-03-25 23:09...|    10|            701.97|      21|
spark-windowing_spark-consumer.1.mb0vqi3yn8j5@dave    | |[2019-03-25 23:09...|     1|             146.0|       3|
spark-windowing_spark-consumer.1.mb0vqi3yn8j5@dave    | +--------------------+------+------------------+--------+
spark-windowing_spark-consumer.1.mb0vqi3yn8j5@dave    | only showing top 20 rows
spark-windowing_spark-consumer.1.mb0vqi3yn8j5@dave    |



spark-windowing_spark-consumer.1.lefp7eew2qvm@dave    | -------------------------------------------
spark-windowing_spark-consumer.1.lefp7eew2qvm@dave    | Batch: 26
spark-windowing_spark-consumer.1.lefp7eew2qvm@dave    | -------------------------------------------
spark-windowing_spark-consumer.1.lefp7eew2qvm@dave    | +------------------------------------------+------+--------------+--------+
spark-windowing_spark-consumer.1.lefp7eew2qvm@dave    | |window                                    |shopId|sum(totalCost)|count(1)|
spark-windowing_spark-consumer.1.lefp7eew2qvm@dave    | +------------------------------------------+------+--------------+--------+
spark-windowing_spark-consumer.1.lefp7eew2qvm@dave    | |[2019-03-26 07:57:00, 2019-03-26 07:58:00]|2     |9.99          |1       |
spark-windowing_spark-consumer.1.lefp7eew2qvm@dave    | +------------------------------------------+------+--------------+--------+
spark-windowing_spark-consumer.1.lefp7eew2qvm@dave    |
spark-windowing_spark-consumer.1.lefp7eew2qvm@dave    | -------------------------------------------
spark-windowing_spark-consumer.1.lefp7eew2qvm@dave    | Batch: 27
spark-windowing_spark-consumer.1.lefp7eew2qvm@dave    | -------------------------------------------
spark-windowing_spark-consumer.1.lefp7eew2qvm@dave    | +------------------------------------------+------+--------------+--------+
spark-windowing_spark-consumer.1.lefp7eew2qvm@dave    | |window                                    |shopId|sum(totalCost)|count(1)|
spark-windowing_spark-consumer.1.lefp7eew2qvm@dave    | +------------------------------------------+------+--------------+--------+
spark-windowing_spark-consumer.1.lefp7eew2qvm@dave    | |[2019-03-26 08:02:00, 2019-03-26 08:03:00]|1     |257.98        |8       |
spark-windowing_spark-consumer.1.lefp7eew2qvm@dave    | +------------------------------------------+------+--------------+--------+
spark-windowing_spark-consumer.1.lefp7eew2qvm@dave    |
spark-windowing_spark-consumer.1.lefp7eew2qvm@dave    | -------------------------------------------
spark-windowing_spark-consumer.1.lefp7eew2qvm@dave    | Batch: 28
spark-windowing_spark-consumer.1.lefp7eew2qvm@dave    | -------------------------------------------
spark-windowing_spark-consumer.1.lefp7eew2qvm@dave    | +------------------------------------------+------+--------------+--------+
spark-windowing_spark-consumer.1.lefp7eew2qvm@dave    | |window                                    |shopId|sum(totalCost)|count(1)|
spark-windowing_spark-consumer.1.lefp7eew2qvm@dave    | +------------------------------------------+------+--------------+--------+
spark-windowing_spark-consumer.1.lefp7eew2qvm@dave    | |[2019-03-26 08:02:00, 2019-03-26 08:03:00]|1     |337.98        |10      |
spark-windowing_spark-consumer.1.lefp7eew2qvm@dave    | +------------------------------------------+------+--------------+--------+
spark-windowing_spark-consumer.1.lefp7eew2qvm@dave    |
spark-windowing_spark-consumer.1.lefp7eew2qvm@dave    | -------------------------------------------
spark-windowing_spark-consumer.1.lefp7eew2qvm@dave    | Batch: 29
spark-windowing_spark-consumer.1.lefp7eew2qvm@dave    | -------------------------------------------
spark-windowing_spark-consumer.1.lefp7eew2qvm@dave    | +------------------------------------------+------+--------------+--------+
spark-windowing_spark-consumer.1.lefp7eew2qvm@dave    | |window                                    |shopId|sum(totalCost)|count(1)|
spark-windowing_spark-consumer.1.lefp7eew2qvm@dave    | +------------------------------------------+------+--------------+--------+
spark-windowing_spark-consumer.1.lefp7eew2qvm@dave    | |[2019-03-26 08:02:00, 2019-03-26 08:03:00]|1     |352.98        |11      |
spark-windowing_spark-consumer.1.lefp7eew2qvm@dave    | |[2019-03-26 08:02:00, 2019-03-26 08:03:00]|2     |194.0         |6       |
spark-windowing_spark-consumer.1.lefp7eew2qvm@dave    | +------------------------------------------+------+--------------+--------+
spark-windowing_spark-consumer.1.lefp7eew2qvm@dave    |
spark-windowing_spark-consumer.1.lefp7eew2qvm@dave    | -------------------------------------------
spark-windowing_spark-consumer.1.lefp7eew2qvm@dave    | Batch: 30
spark-windowing_spark-consumer.1.lefp7eew2qvm@dave    | -------------------------------------------
spark-windowing_spark-consumer.1.lefp7eew2qvm@dave    | +------------------------------------------+------+--------------+--------+
spark-windowing_spark-consumer.1.lefp7eew2qvm@dave    | |window                                    |shopId|sum(totalCost)|count(1)|
spark-windowing_spark-consumer.1.lefp7eew2qvm@dave    | +------------------------------------------+------+--------------+--------+
spark-windowing_spark-consumer.1.lefp7eew2qvm@dave    | |[2019-03-26 08:02:00, 2019-03-26 08:03:00]|1     |432.98        |12      |
spark-windowing_spark-consumer.1.lefp7eew2qvm@dave    | |[2019-03-26 08:02:00, 2019-03-26 08:03:00]|2     |203.99        |7       |
spark-windowing_spark-consumer.1.lefp7eew2qvm@dave    | +------------------------------------------+------+--------------+--------+
spark-windowing_spark-consumer.1.lefp7eew2qvm@dave    |
spark-windowing_spark-consumer.1.lefp7eew2qvm@dave    | -------------------------------------------
spark-windowing_spark-consumer.1.lefp7eew2qvm@dave    | Batch: 31
spark-windowing_spark-consumer.1.lefp7eew2qvm@dave    | -------------------------------------------
spark-windowing_spark-consumer.1.lefp7eew2qvm@dave    | +------------------------------------------+------+--------------+--------+
spark-windowing_spark-consumer.1.lefp7eew2qvm@dave    | |window                                    |shopId|sum(totalCost)|count(1)|
spark-windowing_spark-consumer.1.lefp7eew2qvm@dave    | +------------------------------------------+------+--------------+--------+
spark-windowing_spark-consumer.1.lefp7eew2qvm@dave    | |[2019-03-26 08:02:00, 2019-03-26 08:03:00]|1     |476.98        |13      |
spark-windowing_spark-consumer.1.lefp7eew2qvm@dave    | |[2019-03-26 08:02:00, 2019-03-26 08:03:00]|2     |283.99        |8       |
spark-windowing_spark-consumer.1.lefp7eew2qvm@dave    | +------------------------------------------+------+--------------+--------+
spark-windowing_spark-consumer.1.lefp7eew2qvm@dave    |
spark-windowing_spark-consumer.1.lefp7eew2qvm@dave    | -------------------------------------------
spark-windowing_spark-consumer.1.lefp7eew2qvm@dave    | Batch: 32
spark-windowing_spark-consumer.1.lefp7eew2qvm@dave    | -------------------------------------------
spark-windowing_spark-consumer.1.lefp7eew2qvm@dave    | +------------------------------------------+------+--------------+--------+
spark-windowing_spark-consumer.1.lefp7eew2qvm@dave    | |window                                    |shopId|sum(totalCost)|count(1)|
spark-windowing_spark-consumer.1.lefp7eew2qvm@dave    | +------------------------------------------+------+--------------+--------+
spark-windowing_spark-consumer.1.lefp7eew2qvm@dave    | |[2019-03-26 08:02:00, 2019-03-26 08:03:00]|1     |506.98        |15      |
spark-windowing_spark-consumer.1.lefp7eew2qvm@dave    | +------------------------------------------+------+--------------+--------+
spark-windowing_spark-consumer.1.lefp7eew2qvm@dave    |
spark-windowing_spark-consumer.1.lefp7eew2qvm@dave    | -------------------------------------------
spark-windowing_spark-consumer.1.lefp7eew2qvm@dave    | Batch: 33
spark-windowing_spark-consumer.1.lefp7eew2qvm@dave    | -------------------------------------------
spark-windowing_spark-consumer.1.lefp7eew2qvm@dave    | +------------------------------------------+------+--------------+--------+
spark-windowing_spark-consumer.1.lefp7eew2qvm@dave    | |window                                    |shopId|sum(totalCost)|count(1)|
spark-windowing_spark-consumer.1.lefp7eew2qvm@dave    | +------------------------------------------+------+--------------+--------+
spark-windowing_spark-consumer.1.lefp7eew2qvm@dave    | |[2019-03-26 08:03:00, 2019-03-26 08:04:00]|2     |22.0          |1       |
spark-windowing_spark-consumer.1.lefp7eew2qvm@dave    | +------------------------------------------+------+--------------+--------+







TESTING COMPLETE MODE
spark-windowing_spark-consumer.1.6pjyxzcxv2mj@dave    | -------------------------------------------
spark-windowing_spark-consumer.1.6pjyxzcxv2mj@dave    | Batch: 19
spark-windowing_spark-consumer.1.6pjyxzcxv2mj@dave    | -------------------------------------------
spark-windowing_spark-consumer.1.6pjyxzcxv2mj@dave    | +------------------------------------------+------+------------------+--------+
spark-windowing_spark-consumer.1.6pjyxzcxv2mj@dave    | |window                                    |shopId|sum(totalCost)    |count(1)|
spark-windowing_spark-consumer.1.6pjyxzcxv2mj@dave    | +------------------------------------------+------+------------------+--------+
spark-windowing_spark-consumer.1.6pjyxzcxv2mj@dave    | |[2019-03-26 09:02:00, 2019-03-26 09:03:00]|2     |657.8800000000001 |24      |
spark-windowing_spark-consumer.1.6pjyxzcxv2mj@dave    | |[2019-03-26 09:01:00, 2019-03-26 09:02:00]|2     |1758.8000000000002|55      |
spark-windowing_spark-consumer.1.6pjyxzcxv2mj@dave    | |[2019-03-26 09:02:00, 2019-03-26 09:03:00]|1     |790.95            |26      |
spark-windowing_spark-consumer.1.6pjyxzcxv2mj@dave    | |[2019-03-26 09:01:00, 2019-03-26 09:02:00]|1     |1645.8400000000001|45      |
spark-windowing_spark-consumer.1.6pjyxzcxv2mj@dave    | +------------------------------------------+------+------------------+--------+
spark-windowing_spark-consumer.1.6pjyxzcxv2mj@dave    |
spark-windowing_spark-consumer.1.6pjyxzcxv2mj@dave    | -------------------------------------------
spark-windowing_spark-consumer.1.6pjyxzcxv2mj@dave    | Batch: 20
spark-windowing_spark-consumer.1.6pjyxzcxv2mj@dave    | -------------------------------------------
spark-windowing_spark-consumer.1.6pjyxzcxv2mj@dave    | +------------------------------------------+------+------------------+--------+
spark-windowing_spark-consumer.1.6pjyxzcxv2mj@dave    | |window                                    |shopId|sum(totalCost)    |count(1)|
spark-windowing_spark-consumer.1.6pjyxzcxv2mj@dave    | +------------------------------------------+------+------------------+--------+
spark-windowing_spark-consumer.1.6pjyxzcxv2mj@dave    | |[2019-03-26 09:02:00, 2019-03-26 09:03:00]|2     |753.8800000000001 |27      |
spark-windowing_spark-consumer.1.6pjyxzcxv2mj@dave    | |[2019-03-26 09:01:00, 2019-03-26 09:02:00]|2     |1758.8000000000002|55      |
spark-windowing_spark-consumer.1.6pjyxzcxv2mj@dave    | |[2019-03-26 09:02:00, 2019-03-26 09:03:00]|1     |974.9200000000001 |33      |
spark-windowing_spark-consumer.1.6pjyxzcxv2mj@dave    | |[2019-03-26 09:01:00, 2019-03-26 09:02:00]|1     |1645.8400000000001|45      |
spark-windowing_spark-consumer.1.6pjyxzcxv2mj@dave    | +------------------------------------------+------+------------------+--------+
spark-windowing_spark-consumer.1.6pjyxzcxv2mj@dave    |
spark-windowing_spark-consumer.1.6pjyxzcxv2mj@dave    | -------------------------------------------
spark-windowing_spark-consumer.1.6pjyxzcxv2mj@dave    | Batch: 21
spark-windowing_spark-consumer.1.6pjyxzcxv2mj@dave    | -------------------------------------------
spark-windowing_spark-consumer.1.6pjyxzcxv2mj@dave    | +------------------------------------------+------+------------------+--------+
spark-windowing_spark-consumer.1.6pjyxzcxv2mj@dave    | |window                                    |shopId|sum(totalCost)    |count(1)|
spark-windowing_spark-consumer.1.6pjyxzcxv2mj@dave    | +------------------------------------------+------+------------------+--------+
spark-windowing_spark-consumer.1.6pjyxzcxv2mj@dave    | |[2019-03-26 09:02:00, 2019-03-26 09:03:00]|2     |843.8700000000001 |29      |
spark-windowing_spark-consumer.1.6pjyxzcxv2mj@dave    | |[2019-03-26 09:01:00, 2019-03-26 09:02:00]|2     |1758.8000000000002|55      |
spark-windowing_spark-consumer.1.6pjyxzcxv2mj@dave    | |[2019-03-26 09:02:00, 2019-03-26 09:03:00]|1     |1138.8700000000001|41      |
spark-windowing_spark-consumer.1.6pjyxzcxv2mj@dave    | |[2019-03-26 09:01:00, 2019-03-26 09:02:00]|1     |1645.8400000000001|45      |



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