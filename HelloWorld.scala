package test

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.mongodb.scala.{MongoClient, MongoCollection, MongoDatabase}
import com.mongodb.spark.config._
import com.mongodb.spark._

object HelloWorld {
//  case class MongoClient(uri: String) {
//    def getDatabase(str: String): MongoDatabase = ???
//  }

  def main(args: Array[String]): Unit = {
    println("Apache Spark Application Started ...")

    val uri: String = "mongodb://admin:Z7aDPBAx9GjWJw@cluster0-shard-00-00.k9dyo.mongodb.net:27017," +
      "cluster0-shard-00-01.k9dyo.mongodb.net:27017,cluster0-shard-00-02.k9dyo.mongodb.net:27017/LearnMongoDB?ssl=" +
      "true&replicaSet=atlas-1ek15t-shard-0&authSource=admin&retryWrites=true&w=majority"

    /* Create the SparkSession.
    */
    val spark = SparkSession.builder()
      .master("local")
      .appName("MongoSparkConnectorIntro")
      .config("spark.mongodb.input.uri", uri)
      .config("spark.mongodb.output.uri", uri)
      .getOrCreate()

    val sc = spark.sparkContext
    val loadConfig = ReadConfig(Map("uri"->uri, "collection" -> "TwitterSentimentProject", "database" -> "LearnMongoDB"))
    val rdd = MongoSpark.load(sc, loadConfig)
    rdd.take(10).foreach(println)
    spark.stop()
    println("Apache Spark Application Completed.")

  }
}

//    //configuration
//    val mongodb_host_name = "localhost"
//    val mongodb_port_no = "27017"
//    val mongodb_user_name = "admin"
//    val mongodb_password = "Z7aDPBAx9@GjWJw"
//    val mongodb_database_name = "LearnMongoDB"
//    val mongodb_collection_name = "TwitterSentimentProject"
//
//    val spark_mongodb_output_uri = "mongodb://" + mongodb_user_name + ":" +
//      mongodb_password + "@" + mongodb_host_name + ":" + mongodb_port_no +
//      "/" + mongodb_database_name + "." + mongodb_collection_name
//
//    val df = spark.read
//      .format("mongo")
//      .option("uri", spark_mongodb_output_uri)
//      .option("database", mongodb_database_name)
//      .option("collection", mongodb_collection_name)
//      .load()

//    val client: MongoClient = MongoClient(uri)
//    val db: MongoDatabase = client.getDatabase("LearnMongoDB")
////    val mycollection: MongoCollection = db.getCollection("TwitterSentimentProject")
//    db.listCollectionNames()
// System.setProperty("org.mongodb.async.type", "netty")

//Create Spark Context
//val sc = new SparkContext("local[*]", "MongoDB")
//sc.loadFromMongoDB(ReadConfig(Map("uri" -> uri))) // Uses the ReadConfig
