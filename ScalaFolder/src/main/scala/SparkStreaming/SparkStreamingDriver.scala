package SparkStreaming

import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import MongoDB.MongoDBDriver.{connectCollection}
import org.apache.spark.sql.{DataFrame, SparkSession}
import SparkNLP.SparkNLPDriver._

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

  /** Rank the real time popular hashtags */
  def runPopularHashTags() {

    // Configure Twitter credentials using twitter.txt
    setupTwitter()

    // Set up a Spark streaming context named "PopularHashtags" that runs locally using
    // all CPU cores and 10-second batches of data
    val ssc = new StreamingContext("local[*]", "PopularHashtags", Seconds(10))

    // Get rid of log spam (should be called after the context is set up)
    setupLogging()


    // Create a DStream from Twitter using our streaming context
    val tweets = TwitterUtils.createStream(ssc, None)


    // Now extract the text of each status update into DStreams using map()
    val statuses = tweets.map(status => status.getText)


    // Blow out each word into a new DStream
    val tweetwords = statuses.flatMap(tweetText => tweetText.split(" "))

    
    // Now eliminate anything that's not a hashtag
    val hashtags = tweetwords.filter(word => word.startsWith("#"))


    // Map each hashtag to a key/value pair of (hashtag, 1) so we can count them up by adding up the values
    val hashtagKeyValues = hashtags.map(hashtag => (hashtag, 1))


    // Now count them up over a 5 minute window sliding every 30 second
    val hashtagCounts = hashtagKeyValues.reduceByKeyAndWindow( (x,y) => x + y, (x,y) => x - y, Seconds(300), Seconds(30))
    //  shorthand:
    //val hashtagCounts = hashtagKeyValues.reduceByKeyAndWindow( _ + _, _ -_, Seconds(300), Seconds(60))


    // Sort the results by the count values
    val sortedResults = hashtagCounts.transform(rdd => rdd.sortBy(x => x._2, ascending = false))


    // Print the top 10
    sortedResults.print


    // Set a checkpoint directory, and kick it all off
    ssc.checkpoint("checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }

  /** For given hashtag, find the sentiment polarity for streaming data*/
  def runStreamingSentiment(keyword: String){
    // Configure Twitter credentials using twitter.txt
    setupTwitter()

    // Set up a Spark streaming context named "PopularHashtags" that runs locally using
    // all CPU cores and 10-second batches of data
    val ssc = new StreamingContext("local[*]", "StreamingSentiment", Seconds(10))

    // Get rid of log spam (should be called after the context is set up)
    setupLogging()

    // Create a DStream from Twitter using our streaming context
    val tweets = TwitterUtils.createStream(ssc, None)


    // Now extract the text of each status update into DStreams using map()
    val statuses = tweets.map(status => status.getText)

    //only contain the text with target hashtag
    val targetTexts = statuses.filter(tweetText => tweetText.contains(keyword))

    // Now kick them off over a 10 minute window sliding every 30 second
    val targetTextStreaming = targetTexts.window(Seconds(600), Seconds(30))

    // transform to rdd
    // Use Spark-NLP to get sentiment
    // Write into MongoDB


    targetTextStreaming.foreachRDD { rdd =>

      // Get the singleton instance of SparkSession
      val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
      import spark.implicits._

      // Convert RDD[String] to DataFrame
      val StreamingDataFrame = rdd.toDF("text")

      // add sentiment polarity for each row
      val streamingDataFrameWithSentiment = addSentiment(StreamingDataFrame)

      // Create a temporary view
      streamingDataFrameWithSentiment.createOrReplaceTempView("textWithSentiment")

      streamingDataFrameWithSentiment.show()
    }

//    targetTextStreaming.print

    // Set a checkpoint directory, and kick it all off
    ssc.checkpoint("checkpoint/")
    ssc.start()
    ssc.awaitTermination()

  }
}
