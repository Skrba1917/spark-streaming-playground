package part5twitterapp

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import twitter4j.{StallWarning, Status, StatusDeletionNotice, StatusListener, TwitterStream, TwitterStreamFactory}

import scala.concurrent.Promise

class TwitterReceiver extends Receiver[Status](StorageLevel.MEMORY_ONLY) {

  import scala.concurrent.ExecutionContext.Implicits.global

  val twitterStreamPromise = Promise[TwitterStream]
  val twitterStreamFuture = twitterStreamPromise.future

  private def simpleStatusListener = new StatusListener {
    override def onStatus(status: Status): Unit = store(status)
    override def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice): Unit = ()
    override def onTrackLimitationNotice(numberOfLimitedStatuses: Int): Unit = ()
    override def onScrubGeo(userId: Long, upToStatusId: Long): Unit = ()
    override def onStallWarning(warning: StallWarning): Unit = ()
    override def onException(ex: Exception): Unit = ex.printStackTrace()
  }

  override def onStart(): Unit = {
    val twitterStream = new TwitterStreamFactory("src/main/resources/twitter4j.properties")
      .getInstance()
      .addListener(simpleStatusListener)
      .sample("en") // call the Twitter sample endpoint for English tweets

    twitterStreamPromise.success(twitterStream)
  }

  override def onStop(): Unit = twitterStreamFuture.foreach {twitterStream =>
    twitterStream.cleanUp()
    twitterStream.shutdown()
  }
}
