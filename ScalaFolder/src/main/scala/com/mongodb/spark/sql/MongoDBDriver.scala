package com.mongodb.spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.mongodb.scala.{MongoClient, MongoCollection, MongoDatabase}
import com.mongodb.spark.config._
import com.mongodb.spark._
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{StructField, StructType}
import com.mongodb.spark.sql.MapFunctions.rowToDocument
import org.mongodb.scala.bson.BsonDocument
import org.bson.{BsonDocument}
import org.apache.spark.rdd.RDD


object MongoDBDriver {
  def connectCollection(uri: String, collectionName: String): DataFrame = {
    /* Create the SparkSession.
    */
    val spark = SparkSession.builder()
      .master("local")
      .appName("MongoSparkConnectorIntro")
      .config("spark.mongodb.input.uri", uri)
      .config("spark.mongodb.output.uri", uri)
      .getOrCreate()

    val sc = spark.sparkContext
    val loadConfig = ReadConfig(Map("uri"->uri, "collection" -> collectionName, "database" -> "LearnMongoDB"))
    import spark.implicits._
    val rdd = MongoSpark.load(sc, loadConfig).toDF()
    rdd
  }

  def convertToBson(df:DataFrame): RDD[BsonDocument] = {
    val documentRdd: RDD[BsonDocument] = df.rdd.map(row => rowToDocument(row))
    documentRdd
  }
}
