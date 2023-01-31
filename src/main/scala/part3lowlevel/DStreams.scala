package part3lowlevel

import common._
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream

import java.io.{File, FileWriter}
import java.sql.Date
import java.text.SimpleDateFormat

object DStreams {

  val spark = SparkSession.builder()
    .appName("DStreams")
    .master("local[2]")
    .getOrCreate()

  // entry point to the DStreams API, duration is batch interval
  val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

  // define input sources by creating dstreams
  // define transformations on dstreams
  // call an action on dstreams
  // start ALL computation with ssc.start()
  // await termination

  def readFromSocket() = {
    val socketStream: DStream[String] = ssc.socketTextStream("localhost", 12345)

    // transformation = lazy
    val wordsStream: DStream[String] = socketStream.flatMap(line => line.split(" "))

    // action
    // wordsStream.print()
    wordsStream.saveAsTextFiles("src/main/resources/data/words") // each folder is an rdd = batch and each file is a partition of an rdd

    ssc.start()
    ssc.awaitTermination()
  }

  def createNewFile() = {
    new Thread(() => {
      Thread.sleep(5000)

      val path = "src/main/resources/data/stocks"

      val dir = new File(path)
      val nFiles = dir.listFiles().length
      val newFile = new File(s"$path/newStocks$nFiles.csv")
      newFile.createNewFile()

      val writer = new FileWriter(newFile)
      writer.write(
        """
          |AAPL,May 1 2000,21
          |AAPL,Jun 1 2000,26.19
          |AAPL,Jul 1 2000,25.41
          |AAPL,Aug 1 2000,30.47
          |AAPL,Sep 1 2000,12.88
          |AAPL,Oct 1 2000,9.78
          |""".stripMargin.trim)

      writer.close()

    }).start()
  }

  def readFromFile() = {
    createNewFile() // operates on another thread

    val stocksFilePath = "src/main/resources/data/stocks"

    // monitors a directory for NEW FILES!!!
    val textStream = ssc.textFileStream(stocksFilePath)

    // transformations
    val dateFormat = new SimpleDateFormat("MMM d yyyy")

    val stocksStream = textStream.map {line =>
      val tokens = line.split(",")
      val company = tokens(0)
      val date = new Date(dateFormat.parse(tokens(1)).getTime)
      val price = tokens(2).toDouble

      Stock(company, date, price)
    }

    // action
    stocksStream.print()

    // start the computations
    ssc.start()
    ssc.awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    readFromSocket()
    // readFromFile()
  }

}
