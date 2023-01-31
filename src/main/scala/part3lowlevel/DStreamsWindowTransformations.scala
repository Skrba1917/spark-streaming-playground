package part3lowlevel

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

object DStreamsWindowTransformations {

  val spark = SparkSession.builder()
    .appName("DStreamAdvancedTransformations")
    .master("local[2]")
    .getOrCreate()

  import spark.implicits._

  val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

  def readLines(): DStream[String] = ssc.socketTextStream("localhost", 12345)

  // window = keep all the values emitted between now and X time back
  // window interval updated with every batch
  // window interval must be a multiple of the batch interval
  def linesByWindow() = readLines().window(Seconds(10))

  // first arg window duration, second arg sliding duration
  def linesBySlidingWindow() = readLines().window(Seconds(10), Seconds(5))

  // count the number of elements over a window
  def countLinesByWindow() = readLines().countByWindow(Minutes(60), Seconds(30))

  // aggregate data in a different way over a window
  def sumAllTextByWindow() = readLines().map(_.length).window(Seconds(10), Seconds(5)).reduce(_ + _)

  def sumAllTextByWindowAlt() = readLines().map(_.length).reduceByWindow(_ + _, Seconds(10), Seconds(5))

  // tumbling windows
  def linesByTumblingWindow() = readLines().window(Seconds(10), Seconds(10)) // batch of batches

  def computeWordOccurrencesByWindow() = {

    // for reduce by key and window you need a checkpoint directory set
    ssc.checkpoint("checkpoints")

    readLines()
      .flatMap(_.split(" "))
      .map(word => (word, 1))
      .reduceByKeyAndWindow(
        (a, b) => a + b, // reduction function
        (a, b) => a - b, // inverse function
        Seconds(60), // window duration
        Seconds(30) // sliding duration
      )
  }

  // EXERCISE

  def showMeTheMoney() = readLines()
    .flatMap(line => line.split( " "))
    .filter(_.length >= 10)
    .map(_  => 2)
    .reduce(_ + _)
    .window(Seconds(30), Seconds(10))
    .reduce(_ + _)

  def main(args: Array[String]): Unit = {
    computeWordOccurrencesByWindow()
    // linesByWindow().print()
    ssc.start()
    ssc.awaitTermination()

  }

}
