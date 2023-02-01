package part4integrations

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.util

object IntegratingKafkaDStreams {

  val spark = SparkSession.builder()
    .appName("IntegratingKafka")
    .master("local[2]")
    .getOrCreate()

  val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

  val kafkaParams: Map[String, Object] = Map(
    "bootstrap.servers" -> "localhost:9092",
    "key.serializer" -> classOf[StringSerializer], // send data to kafka
    "value.serializer" -> classOf[StringSerializer],
    "key.deserializer" -> classOf[StringDeserializer], // receive data from kafka
    "value.deserializer" -> classOf[StringDeserializer],
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> false.asInstanceOf[Object]
  )

  val kafkaTopic = "rockthejvm"

  def readFromKafka() = {
    val topics = Array(kafkaTopic)
    val kafkaDStream = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent, // distributes the partitions evenly across the Spark Cluster
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams + ("group.id" -> "group1"))
    )

    val processedStream = kafkaDStream.map(record => (record.key(), record.value()))
    processedStream.print()

    ssc.start()
    ssc.awaitTermination()
  }

  def writeToKafka() = {
    val inputData = ssc.socketTextStream("localhost", 12345)

    // transformation
    val processedData = inputData.map(_.toUpperCase())

    processedData.foreachRDD { rdd =>
      rdd.foreachPartition {partition =>
        // the code is run by a single executor inside this lambda
        val kafkaHashMap = new java.util.HashMap[String, Object]()
        kafkaParams.foreach {pair =>
          kafkaHashMap.put(pair._1, pair._2)
        }

        // producer can insert records into kafka topics
        // available on this executor
        val producer = new KafkaProducer[String, String](kafkaHashMap)

        partition.foreach { value =>
          val message = new ProducerRecord[String, String](kafkaTopic, null, value)
          // feed the message into the Kafka topic
          producer.send(message)
        }

        producer.close()
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    // readFromKafka()
    writeToKafka()

  }

}
