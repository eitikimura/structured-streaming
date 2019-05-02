package com.wavy.spark.structured.stream.kafka

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{from_json, current_timestamp, datediff}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{DataTypes, StructType}

object KafkaReadStream {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("kafka-tutorial")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val brokers = "localhost:9092"
    val topics = "WAVY.TOPIC.1"

    //reading data from Kafka
    val inputDf = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokers)
      .option("subscribe", topics)
      .load()
      .selectExpr( "topic", "timestamp", "CAST(value AS STRING)")
      .as[(String, Long, String)]

    inputDf.printSchema()

    val struct = new StructType()
      .add("name", DataTypes.StringType)
      .add("marital_status", DataTypes.StringType)
      .add("birth_date", DataTypes.TimestampType)
      .add("gender", DataTypes.StringType)
      .add("country", DataTypes.StringType)

    var dataset = inputDf.select(from_json($"value", struct).as("entry"))
                         .selectExpr("entry.name", "entry.marital_status",
                           "entry.birth_date", "entry.gender", "entry.country")

    dataset = dataset.withColumn("age",
       datediff(current_timestamp(), $"birth_date")/365)

    dataset.createOrReplaceTempView("event_records")

    dataset.printSchema()

    val transformation = spark.sql(
      """
        SELECT country, gender, AVG(age) as avg_age, COUNT(1) as num_events
        FROM event_records
        GROUP BY country, gender
        ORDER BY country, gender
      """)

    val consoleOutput = transformation.writeStream
      .outputMode("complete")
      .format("console")
      .trigger(Trigger.ProcessingTime("3 seconds"))
      .start()

    print("wait for processing batch 0...")

    spark.streams.awaitAnyTermination()
    spark.close()
  }
}
