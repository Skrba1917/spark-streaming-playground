package part2structuredstreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object StreamingAggregations {

  val spark = SparkSession.builder()
    .appName("StreamingAggregations")
    .config("spark.master", "local[2]")
    .getOrCreate()

  def streamingCount() = {
    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    val lineCount: DataFrame = lines.selectExpr("count(*) as lineCount")

    // aggs with distinct are not supported otherwise Spark will need to keep track of EVERYTHING

    lineCount.writeStream
      .format("console")
      .outputMode("complete") // append and update not supported on aggregations without watermark
      .start()
      .awaitTermination()
  }

  def numericalAggregations(aggFunction: Column => Column) = {
    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    // aggregate here
    val numbers = lines.select(col("value").cast("integer").as("number"))
    val aggregationDF = numbers.select(aggFunction(col("number")).as("agg_so_far"))

    aggregationDF.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def groupNames() = {
    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    val names = lines
      .select(col("value").as("name"))
      .groupBy(col("name")) // RelationalGroupedSet
      .count()

    names.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    // streamingCount()
    // numericalAggregations(stddev)
    groupNames()
  }


}
