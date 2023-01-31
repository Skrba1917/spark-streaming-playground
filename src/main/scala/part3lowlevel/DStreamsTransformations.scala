package part3lowlevel

import common.Person
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.io.File
import java.sql.Date
import java.time.{LocalDate, Period}

object DStreamsTransformations {

  val spark = SparkSession.builder()
    .appName("DStreamTransformations")
    .master("local[2]")
    .getOrCreate()

  import spark.implicits._

  val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

  def readPeople() = ssc.socketTextStream("localhost", 9999).map {
    line =>
      val tokens = line.split(":")
      Person(
        tokens(0).toInt,
        tokens(1),
        tokens(2),
        tokens(3),
        tokens(4),
        Date.valueOf(tokens(5)),
        tokens(6),
        tokens(7).toInt
      )
  }

  // map, flatMap, filter
  def peopleAges() = readPeople().map { person =>
    val age = Period.between(person.birthDate.toLocalDate, LocalDate.now()).getYears
    (s"${person.firstName} ${person.lastName}", age)
  }

  def peopleSmallNames() = readPeople().flatMap {person =>
    List(person.firstName, person.middleName)
  }

  def highIncomePeople() = readPeople().filter(_.salary > 80000)

  // count
  def countPeople() = readPeople().count() // the number of entries in every batch

  // count by value
  def countNames() = readPeople().map(_.firstName).countByValue() // operates per batch

  // reduce by key, works on DStream of tuples, works per batch,
  def countNamesReduce() = readPeople().map(_.firstName)
    .map(name => (name, 1))
    .reduceByKey((a, b) => a + b)

  // for each rdd
  def saveToJson() = readPeople().foreachRDD { rdd =>
    val ds = spark.createDataset(rdd)
    val f = new File("src/main/resources/data/people")
    val nFiles = f.listFiles().length
    val path = s"src/main/resources/data/people/people$nFiles.json"

    ds.write.json(path)
  }

  def main(args: Array[String]): Unit = {
    val stream = countNamesReduce()
    stream.print()

    // saveToJson()

    ssc.start()
    ssc.awaitTermination()

  }

}
