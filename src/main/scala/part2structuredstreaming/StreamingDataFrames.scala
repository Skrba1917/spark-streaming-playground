package part2structuredstreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import common._
import org.apache.spark.sql.streaming.Trigger
import scala.concurrent.duration._

object StreamingDataFrames {

  val spark = SparkSession.builder()
    .appName("StreamApp")
    .config("spark.master", "local[2]")
    .getOrCreate()

  def readFromSocket(): Unit = {
    // reads a DF
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    // transformations
    val shortLines: DataFrame = lines.filter(length(col("value")) <= 5)

    // tell between a static vs a streaming DF
    println(shortLines.isStreaming)

    // consumes a DF
    val query = shortLines.writeStream
      .format("console")
      .outputMode("append")
      .start()

    // wait for the stream to finish, because start() is async
    query.awaitTermination()
  }

  def readFromFiles(): Unit = {
    val stocksDF: DataFrame = spark.readStream
      .format("csv")
      .option("header", "false")
      .option("dateFormat", "MMM d yyyy")
      .schema(stocksSchema)
      .load("src/main/resources/data/stocks")

    stocksDF.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()

  }

  def demoTriggers() = {
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    // write the lines DF at the certain trigger
    lines.writeStream
      .format("console")
      .outputMode("append")
      .trigger(
        Trigger.ProcessingTime(2.seconds) // every 2 seconds run the query
        // Trigger.Once()  ==> single batch, then terminates the query
        // Trigger.Continuous(2.seconds) ==> experimental, every 2 seconds create a batch with whatever we have
      )
      .start()
      .awaitTermination()

  }

  def main(args: Array[String]): Unit = {
    // readFromSocket()
    // readFromFiles()
    // demoTriggers()
  }

}
