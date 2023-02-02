package part6advanced

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}

import java.io.PrintStream
import java.net.ServerSocket
import scala.concurrent.duration._
import java.sql.Timestamp

object Watermarks {

  val spark = SparkSession.builder()
    .appName("Watermarks")
    .master("local[2]")
    .getOrCreate()

  import spark.implicits._

  def debugQuery(query: StreamingQuery) = {
    // useful skill for debugging
    new Thread(() => {
      (1 to 100).foreach { i =>
        Thread.sleep(1000)
        val queryEventTime =
          if (query.lastProgress == null) "[]"
          else query.lastProgress.eventTime.toString

        println(s"$i: $queryEventTime")
      }
    }).start()
  }

  def testWatermarks() = {
    val dataDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()
      .as[String]
      .map {line =>
        val tokens = line.split(",")
        val timestamp = new Timestamp(tokens(0).toLong)
        val data = tokens(1)

        (timestamp, data)
      }
      .toDF("created", "color")

    val watermarkedDF = dataDF
      .withWatermark("created", "2 seconds") // adding 2 sec watermark
      .groupBy(window(col("created"), "2 seconds"), col("color"))
      .count()
      .selectExpr("window.*", "color", "count")

    /*
      2 sec watermark means:
      - a window will only be considered until the watermark surpasses the window end
      - an element/a row/a record will be considered if AFTER the watermark
     */

    val query = watermarkedDF.writeStream
      .format("console")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(2.seconds))
      .start

    debugQuery(query)
    query.awaitTermination()

  }

  object DataSender {
    val serverSocket = new ServerSocket(12345)
    val socket = serverSocket.accept() // blocking call
    val printer = new PrintStream(socket.getOutputStream)

    println("socket accepted")

    def example1() = {
      Thread.sleep(7000)
      printer.println("7000,blue")
      Thread.sleep(1000)
      printer.println("8000,green")
      Thread.sleep(4000)
      printer.println("14000,blue")
      Thread.sleep(1000)
      printer.println("9000,red")
      Thread.sleep(3000)
      printer.println("15000,red")
      printer.println("8000,blue")
      Thread.sleep(1000)
      printer.println("13000,green")
      Thread.sleep(500)
      printer.println("21000,green")
      Thread.sleep(3000)
      printer.println("4000,purple") // expect to be dropped
      Thread.sleep(2000)
      printer.println("17000,green")
    }
  }

  def main(args: Array[String]): Unit = {
    // testWatermark()
  }

}
