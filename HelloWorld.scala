package test

import org.apache.spark.SparkContext
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.mongodb.scala.{MongoClient, MongoCollection, MongoDatabase}
import com.mongodb.spark.config._
import com.mongodb.spark._
import com.johnsnowlabs.nlp.pretrained.PretrainedPipeline
import com.johnsnowlabs.nlp.SparkNLP
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{StructType, StructField}
//import org.apache.spark.sql.functions.{array_contains, col}
//import scala.math.BigDecimal.RoundingMode

object HelloWorld {

//  case class Tweet(date:String, ids:Double, text:String, user:String, _id: String, polarity:Int)

  //initialize JohnSnowLabs Spark NLP library pretrained sentiment analysis pipeline
  def setUpPipeline(df: DataFrame): DataFrame = {
    val pipeline = PretrainedPipeline("analyze_sentimentdl_use_twitter", lang = "en")
    val selectColumns = df.columns.toSeq
    //select ugly name text
    val name = selectColumns(2)
    val results = pipeline.annotate(df, name)

////    extract sentiment result
////    We start by declaring an "anonymous function" in Scala
//    val lookupSentiment : StructType => String = (sentiment:StructType)=>{
////        sentiment("result").asInstanceOf[String]
//        sentiment.apply("result").toString()
//    }
//
//    // Then wrap it with a udf
//    val lookupSentimentUDF = udf(lookupSentiment)
//
//    // Add a sentiment column using our new udf
//    val dfWithSentiment = results.withColumn("SentimentResult", lookupSentimentUDF(col("sentiment")))
//
////    val dfWithSentiment = results.select(($"sentiment.result").as("SentimentResult"))
    val dfWithSentiment = results.select(results.col("sentiment.result").as("SentimentResult"))
    dfWithSentiment
  }

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
    import spark.implicits._
    val rdd = MongoSpark.load(sc, loadConfig).toDF()



    val df_withSentiment = setUpPipeline(rdd)
//    df_withSentiment.printSchema()
    df_withSentiment.select("SentimentResult").show()
//    df.take(5).foreach(println)

//    println(selectColumns)
//    println(name)
//    println(rdd.getClass)
//    rdd.printSchema()
//    rdd.select(name).show()

////    val rdd_df = rdd.toDF()
//    val dfWithSchema = spark.createDataFrame(rdd).toDF("date", "ids", "text", "user", "_id", "polarity")
//    dfWithSchema.select("text").show()

//    rdd_df.printSchema
//    rdd_df.take(5).foreach(println)

//    df.printSchema()


    println("Apache Spark Application Completed.")

  }
}

