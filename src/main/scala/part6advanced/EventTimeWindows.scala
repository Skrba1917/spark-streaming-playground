package part6advanced

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.functions._

object EventTimeWindows {

  val spark = SparkSession.builder()
    .appName("EventTimeWindows")
    .master("local[2]")
    .getOrCreate()

  val onlinePurchaseSchema = StructType(
    Array(
      StructField("id", StringType),
      StructField("time", TimestampType),
      StructField("item", StringType),
      StructField("quantity", IntegerType)
    )
  )

  def readPurchasesFromFile() = spark.read
    .schema(onlinePurchaseSchema)
    .json("src/main/resources/data/purchases")

  def readPurchasesFromSocket() = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 12345)
    .load()
    .select(from_json(col("value"), onlinePurchaseSchema).as("purchase"))
    .selectExpr("purchase.*")

  def aggregatePurchasesByTumblingWindow() = {
    val purchasesDF = readPurchasesFromSocket()

    val windowByDay = purchasesDF
      .groupBy(window(col("time"), "1 day").as("time")) // tumbling window: sliding duration == window duration
      .agg(sum("quantity").as("totalQuantity"))
      .select(
        col("time").getField("start").as("start"),
        col("time").getField("end").as("end"),
        col("totalQuantity")
      )

    windowByDay.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }


  def aggregatePurchasesBySlidingWindow() = {
    val purchasesDF = readPurchasesFromSocket()

    val windowByDay = purchasesDF
      .groupBy(window(col("time"), "1 day", "1 hour").as("time"))
      .agg(sum("quantity").as("totalQuantity"))
      .select(
        col("time").getField("start").as("start"),
        col("time").getField("end").as("end"),
        col("totalQuantity")
      )

    windowByDay.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  // EXERCISES
  // 1) show best selling product of every day + quantity
  // 2) show the best selling product of every 24 hours, updated every hour

  // 1)
  def showBestSellingProduct() = {
      val purchasesDF = readPurchasesFromFile()

      val windowByDay = purchasesDF
        .groupBy(col("item"), window(col("time"), "1 day").as("day"))
        .agg(sum("quantity").as("totalQuantity"))
        .select(
          col("day").getField("start").as("start"),
          col("day").getField("end").as("end"),
          col("totalQuantity"),
          col("item"),
        )
        .orderBy(col("day"), col("totalQuantity").desc)

      windowByDay.writeStream
        .format("console")
        .outputMode("complete")
        .start()
        .awaitTermination()
  }

  // 2)
  def bestSellingUpdated() = {
    val purchasesDF = readPurchasesFromFile()

    val windowByDay = purchasesDF
      .groupBy(col("item"), window(col("time"), "1 day", "1 hour").as("time"))
      .agg(sum("quantity").as("totalQuantity"))
      .select(
        col("time").getField("start").as("start"),
        col("time").getField("end").as("end"),
        col("totalQuantity"),
        col("item"),
      )
      .orderBy(col("time"), col("totalQuantity").desc)

    windowByDay.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    // aggregatePurchasesBySlidingWindow()
    // showBestSellingProduct()
    bestSellingUpdated()
  }

}
