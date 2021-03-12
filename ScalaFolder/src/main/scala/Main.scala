import MongoDB.MongoDBDriver.{connectCollection}
import SparkNLP.SparkNLPDriver.getSentimentSummary

object Main {
  def main(args: Array[String]): Unit = {
    println("Apache Spark Application Started ...")

    // get the uri from MongoDB Compass
    val uri: String = "mongodb://admin:Z7aDPBAx9GjWJw@cluster0-shard-00-00.k9dyo.mongodb.net:27017," +
      "cluster0-shard-00-01.k9dyo.mongodb.net:27017,cluster0-shard-00-02.k9dyo.mongodb.net:27017/LearnMongoDB?ssl=" +
      "true&replicaSet=atlas-1ek15t-shard-0&authSource=admin&retryWrites=true&w=majority"

    // collection name
    val colletionName = "TwitterSentimentProject"
//    val colletionName = "TestCollection"

    // get the sentiment140 DataFrame
    val sentiment_DF = connectCollection(uri,colletionName)

    // get the sentiment summary for sentiment140 dataset
    val sentiment_summary = getSentimentSummary(sentiment_DF)

    sentiment_summary.show()

    println("Apache Spark Application Completed.")
  }
}
