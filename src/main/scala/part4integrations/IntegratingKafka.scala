package part4integrations

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import common._

object IntegratingKafka {

  val spark = SparkSession.builder()
    .appName("IntegratingKafka")
    .master("local[2]")
    .getOrCreate()

  def readFromKafka() = {
    val kafkaDF: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "rockthejvm")
      .load()

    kafkaDF
      .select(col("topic"), expr("cast(value as string) as actualValue"))
      .writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  def writeToKafka() = {
    val carsDF = spark.readStream
      .schema(carsSchema)
      .json("src/main/resources/data/cars")

    val carsKafkaDF = carsDF.selectExpr("upper(Name) as key", "Name as value")
    carsKafkaDF.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "rockthejvm")
      .option("checkpointLocation", "checkpoints") // without checkpoints the writing to Kafka will fail
      .start()
      .awaitTermination()

  }

  def writeCarsToKafka() = {
    val carsDF = spark.readStream
      .schema(carsSchema)
      .json("src/main/resources/data/cars")

    val carsKafkaJsonDF = carsDF.select(
      col("Name").as("key"),
      to_json(struct(col("Name"), col("Horsepower"), col("Origin"))).cast("String").as("value")
    )

    carsKafkaJsonDF.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "rockthejvm")
      .option("checkpointLocation", "checkpoints") // without checkpoints the writing to Kafka will fail
      .start()
      .awaitTermination()

  }

  def main(args: Array[String]): Unit = {
    // readFromKafka()
    // writeToKafka()
    writeCarsToKafka()
  }

}
