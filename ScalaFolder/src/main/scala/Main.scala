import MongoDB.MongoDBDriver.connectSentiment140
import SparkNLP.SparkNLPDriver.getSentimentSummary

object Main {
  def main(args: Array[String]): Unit = {
    println("Apache Spark Application Started ...")

    // get the uri from MongoDB Compass
    val uri: String = "mongodb://admin:Z7aDPBAx9GjWJw@cluster0-shard-00-00.k9dyo.mongodb.net:27017," +
      "cluster0-shard-00-01.k9dyo.mongodb.net:27017,cluster0-shard-00-02.k9dyo.mongodb.net:27017/LearnMongoDB?ssl=" +
      "true&replicaSet=atlas-1ek15t-shard-0&authSource=admin&retryWrites=true&w=majority"

    // get the sentiment140 DataFrame
    val sentiment140_DF = connectSentiment140(uri)

    // get the sentiment summary for sentiment140 dataset
    val sentiment_summary = getSentimentSummary(sentiment140_DF)

    println("Apache Spark Application Completed.")
  }
}
