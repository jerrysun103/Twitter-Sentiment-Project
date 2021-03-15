package SparkStreaming

import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._

object SparkStreamingDriver {
  // Access token: 1368376671733178372-inXzPbhkwXNnS56wx5NihMDFc7FM5D
  // Access token secret: 1wWxyYyASGwSmrzFr3BRxI4QiVKJ4SAttnTiNIE2391OW

  // API key: ENGYp3Uh5L9xxDuE8TjIl1hzZ
  // API key secret: WSLLdjomzKnLOlCszmI4pTY6noC0630BlM2l1rhuU1fJoULPbo

  /** Makes sure only ERROR messages get logged to avoid log spam. */
  def setupLogging(): Unit = {
    import org.apache.log4j.{Level, Logger}
    val rootLogger = Logger.getRootLogger
    rootLogger.setLevel(Level.ERROR)
  }

  /** Configures Twitter service credentials using twitter.txt in the main workspace directory */
  def setupTwitter(): Unit = {
    import scala.io.Source

    val lines = Source.fromFile("data/twitter.txt")
    for (line <- lines.getLines) {
      val fields = line.split(" ")
      if (fields.length == 2) {
        System.setProperty("twitter4j.oauth." + fields(0), fields(1))
      }
    }
    lines.close()
  }

  /** Our main function where the action happens */
  def runSparkStreaming() {

    // Configure Twitter credentials using twitter.txt
    setupTwitter()
    println("Set up Twitter")

    // Set up a Spark streaming context named "PopularHashtags" that runs locally using
    // all CPU cores and one-second batches of data
    val ssc = new StreamingContext("local[*]", "PopularHashtags", Seconds(1))

    // Get rid of log spam (should be called after the context is set up)
    setupLogging()

    println("Before Created DStream")
    // Create a DStream from Twitter using our streaming context
    val tweets = TwitterUtils.createStream(ssc, None)
    println("Created a DSream")

    // Now extract the text of each status update into DStreams using map()
    val statuses = tweets.map(status => status.getText)
    println("Got Text")

    // Blow out each word into a new DStream
    val tweetwords = statuses.flatMap(tweetText => tweetText.split(" "))
    println("Blow out each word into a new DStream")

    // Now eliminate anything that's not a hashtag
    val hashtags = tweetwords.filter(word => word.startsWith("#"))
    println("eliminate anything that is not a hashtag")

    // Map each hashtag to a key/value pair of (hashtag, 1) so we can count them up by adding up the values
    val hashtagKeyValues = hashtags.map(hashtag => (hashtag, 1))
    println("Map each hashtag to a key/value pair of (hashtag, 1) so we can count them up by adding up the values")

    // Now count them up over a 5 minute window sliding every one second
    val hashtagCounts = hashtagKeyValues.reduceByKeyAndWindow( (x,y) => x + y, (x,y) => x - y, Seconds(300), Seconds(1))
    //  You will often see this written in the following shorthand:
    //val hashtagCounts = hashtagKeyValues.reduceByKeyAndWindow( _ + _, _ -_, Seconds(300), Seconds(1))
    println("Now count them up over a 5 minute window sliding every one second")

    // Sort the results by the count values
    val sortedResults = hashtagCounts.transform(rdd => rdd.sortBy(x => x._2, ascending = false))
    println("Sort the results by the count values")

    // Print the top 10
    sortedResults.print
    println("Print the top 10")

    // Set a checkpoint directory, and kick it all off
    // I could watch this all day!
    ssc.checkpoint("C:/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }
}
