package part6advanced

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout}

object ArbitraryStatefulComputation {

  val spark = SparkSession.builder()
    .appName("ASC")
    .master("local[2]")
    .getOrCreate()

  import spark.implicits._

  case class SocialPostRecord(postType: String, count: Int, storageUsed: Int)
  case class SocialPostBulk(postType: String, count: Int, totalStorageUsed: Int)
  case class AveragePostStorage(postType: String, averageStorage: Double)

  def readSocialUpdates() =
    spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()
      .as[String]
      .map {line =>
        val tokens = line.split(",")
        SocialPostRecord(tokens(0), tokens(1).toInt, tokens(2).toInt)
      }

  def updateAverageStorage(
                          postType: String, // the key for grouping
                          group: Iterator[SocialPostRecord], // batch of data associated to the key
                          state: GroupState[SocialPostBulk] // like an "option", needs to be managed manually
                          ) : AveragePostStorage = {
    /*
      - extract the state to start with
      - for all the items in the group
        - aggregate data:
          - summing up the total count
          - summing up the total storage
      - update the state wih new aggregated data
      - return a single value of type AveragePostStorage
     */

    val previousBulk =
      if (state.exists) state.get
      else SocialPostBulk(postType, 0, 0)

    val totalAggregatedData = group.foldLeft((0, 0)) { (currentData, record) =>
      val (currentCount, currentStorage) = currentData
      (currentCount + record.count, currentStorage + record.storageUsed)
    }

    val (totalCount, totalStorage) = totalAggregatedData
    val newPostBulk = SocialPostBulk(postType, previousBulk.count + totalCount, previousBulk.totalStorageUsed + totalStorage)

    state.update(newPostBulk)

    AveragePostStorage(postType, newPostBulk.totalStorageUsed * 1.0 / newPostBulk.count)
  }

  def getAveragePostStorage() = {
    val socialStream = readSocialUpdates()

    val regularSql = socialStream
      .groupByKey(_.postType)
      .agg(sum(col("count")).as("totalCount").as[Int], sum(col("count")).as("totalCount").as[Int])
      .selectExpr("postType", "totalStorage/totalCount as avgStorage")

    val averageByPostType = socialStream
      .groupByKey(_.postType)
      .mapGroupsWithState(GroupStateTimeout.NoTimeout())(updateAverageStorage)

    averageByPostType.writeStream
      .outputMode("update")
      .foreachBatch { (batch: Dataset[AveragePostStorage], _: Long) =>
        batch.show()
      }
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    getAveragePostStorage()
  }

}
