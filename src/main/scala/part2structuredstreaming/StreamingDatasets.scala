package part2structuredstreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import common._

object StreamingDatasets {

  val spark = SparkSession.builder()
    .appName("StreamingDatasets")
    .master("local[2]")
    .getOrCreate()

  import spark.implicits._

  def readCars() = {
    spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load() // DF with single string column value
      .select(from_json(col("value"),carsSchema).as("car")) // composite column (struct)
      .selectExpr("car.*") // this is a DF with multiple columns
      .as[Car]
  }

  def showCarNames() = {
    val carsDS = readCars()

    // transformations
    val carNamesDF = carsDS.select(col("name")) // DF

    // collection transformations maintain type info
    val carNamesAlt = carsDS.map(_.Name) // Dataset of String

    carNamesAlt.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  // 3 EXERCISES BELOW

  def showPowerfulCars() = {
    val carsDS = readCars()

    val powerfulCarsDS = carsDS.filter(_.Horsepower.getOrElse(0L) > 140)

    powerfulCarsDS.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  def averageHP() = {
    val carsDS = readCars()

    carsDS.select(avg(col("Horsepower"))).writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def countCarsByOrigin() = {
    val carsDS = readCars()

    val carsDataframeAPI = carsDS.groupBy(col("Origin")).count()
    val carsDatasetAPI = carsDS.groupByKey(car => car.Origin).count() // option 1 with Dataset API

    carsDatasetAPI.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    // showCarNames()
    // showPowerfulCars()
    // averageHP()
    countCarsByOrigin()

  }

}
