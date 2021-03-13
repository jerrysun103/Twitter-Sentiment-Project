package SparkNLP

import com.johnsnowlabs.nlp.pretrained.PretrainedPipeline
import com.johnsnowlabs.nlp.SparkNLP
import org.apache.spark.sql._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._


import scala.math.BigDecimal.RoundingMode

object SparkNLPDriver {
  //initialize JohnSnowLabs Spark NLP library pretrained sentiment analysis pipeline
  def setUpPipeline(df: DataFrame): DataFrame = {
    val pipeline =
      PretrainedPipeline("analyze_sentimentdl_use_twitter", lang = "en")

    val results = pipeline.annotate(df, "text")

    results
  }

  /** check twitter text fields to determine
   * if tweet is positive or negative sentiment.
   * Returns dataframe with counts of each sentiment.
   */
  def analyzeSentiment(spark: SparkSession, df: DataFrame): DataFrame = {
    import spark.implicits._
    val sentimentResults = df
      .select($"sentiment" ("result").as("Sentiment Results"))
      .groupBy($"Sentiment Results")
      .count()
      .orderBy($"count".desc)
      .cache()

//    sentimentResults.show()
    sentimentResults
  }

  //selects and returns count of only positive sentiments
  def positiveSentiments(spark: SparkSession, df: DataFrame): Int = {
    import spark.implicits._
    val positiveSentiment = df
      .where(array_contains(($"Sentiment Results"), "positive"))
      .collect()

    // println(positiveSentiment(0)(1))
    positiveSentiment(0)(1).toString.toInt
  }

  //selects and returns count of only negative sentiments
  def negativeSentiments(spark: SparkSession, df: DataFrame): Int = {
    import spark.implicits._
    val negativeSentiment = df
      .where(array_contains(($"Sentiment Results"), "negative"))
      .collect()

//    println(negativeSentiment(0)(1))
    negativeSentiment(0)(1).toString.toInt
  }

  //Returns percentage of positive to negative tweets with two decimal precision
  def positiveRatio(posSentiment: Int, negSentiment: Int): Double = {
    // ratio = # of positive / (# of positive + # of negative)
    val posRatio =
      posSentiment.toDouble / (posSentiment.toDouble + negSentiment.toDouble)

    val posPercent =
      BigDecimal(posRatio).setScale(2, RoundingMode.HALF_UP).toDouble
    // println(posPercent)
    posPercent * 100
  }

  //converts final results into a dataframe
  def createResultsDf(
                       spark: SparkSession,
                       negResults: Int,
                       posResults: Int,
                       finalResults: Int
                     ): DataFrame = {
    import spark.implicits._
    val resultsDF = spark
      .createDataFrame(
        Seq(
          ("Number_Negative_Sentiments", negResults),
          ("Number_positive_Sentiments", posResults),
          ("Percentage of Positive Sentiments", finalResults)
        )
      )
      .toDF()

    resultsDF
  }


  def tweetPositiveNegative(spark: SparkSession, df: DataFrame): DataFrame = {
    // use pretrained pipeline to fit dataframe(add sentiment structs)
    val dfWithSentiment = setUpPipeline(df)
    println("pipeline done")

    // groupby sentiment, then count
    val sentiments = analyzeSentiment(spark, dfWithSentiment)
    println("sentiment done")

    // compute the negtive sentiment count
    val negSent = negativeSentiments(spark, sentiments)
    println("negative count done")

    // compute the positive sentiment count
    val posSent = positiveSentiments(spark, sentiments)
    println("positive count done")

    // compute the ratio
    val finalPosRatio = positiveRatio(posSent, negSent).toInt
    println("ratio done")

    createResultsDf(spark, negSent, posSent, finalPosRatio)
  }

  //runs all methods above
  def getSentimentSummary(df: DataFrame): DataFrame = {
    /* Create the SparkSession.
    */
    val spark = SparkSession.builder()
      .master("local")
      .appName("MongoSparkConnectorIntro")
      .getOrCreate()

    val sentimentSummaryDF = tweetPositiveNegative(spark, df)

    sentimentSummaryDF
  }

  // test the accuracy of sparkNLP sentiment prediction
  def computeSparkNLPAccuracy(df: DataFrame): Double = {
    /* Create the SparkSession.
    */
    val spark = SparkSession.builder()
      .master("local")
      .appName("SparkNlPAccuracyTest")
      .getOrCreate()

    // use pretrained pipeline to fit dataframe(add sentiment structs)
    val dfWithSentiment = setUpPipeline(df)
    println("pipeline done")


    // only keep text, polarity and sentiment result
    val sentiment_DF =  dfWithSentiment.select(dfWithSentiment.col("text"),
                        dfWithSentiment.col("polarity"),
                        dfWithSentiment.col("sentiment.result")(0).as("SentimentResult"))
    println("only keep text, polarity and sentiment result")
//    sentiment_DF.show()

    //select positve and negative only
    val selected_sentiment_DF = sentiment_DF.filter(sentiment_DF("SentimentResult") =!= "neutral")
    println("select positve and negative only")


    //add test column to compare label and sentiment predict
    val plus = "positive"
    val minus = "negative"

    val sentimentWithAccuracy = selected_sentiment_DF.withColumn("test result", when(col("polarity") === 0.0 && col("SentimentResult") === lit(minus),0)
                                                     .when(col("polarity") === 4 && col("SentimentResult") === lit(plus), 0.0)
                                                     .otherwise(1.0))
    println("do the predict test")

    val accuracy = 1 - (sentimentWithAccuracy.agg(sum("test result")).first.getDouble(0) / selected_sentiment_DF.count().toDouble)


    sentimentWithAccuracy.write.format("csv").save("test_accuracy.csv")
    accuracy
  }
}


//    println(sentiment_DF.count())
//    println(selected_sentiment_DF.count())
//    sentiment_DF.write.format("csv").save("test_sentiment.csv")
//    selected_sentiment_DF.write.format("csv").save("test_selected_sentiment.csv")

//    for (s <- sentiment_DF.select("SentimentResult").collect().map(_(0)).toList){
//      println(s)
//    }