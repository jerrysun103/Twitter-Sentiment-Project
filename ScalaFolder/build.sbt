name := "ScalaFolder"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.7",
  "org.apache.spark" %% "spark-sql" % "2.4.7",
  "org.apache.spark" %% "spark-mllib" % "2.4.7",

  "org.mongodb.spark" %% "mongo-spark-connector" % "2.4.3",
  "org.mongodb.scala" %% "mongo-scala-driver" % "2.9.0",

  "com.johnsnowlabs.nlp" %% "spark-nlp" % "2.7.5",

  "org.twitter4j" % "twitter4j-core" % "4.0.2",
  "org.twitter4j" % "twitter4j-stream" % "4.0.2",
  "org.apache.spark" %% "spark-streaming" % "1.6.",
  "org.apache.bahir" %% "spark-streaming-twitter" % "1.6.0"
//  "org.spark-project" %% "dstream-twitter" % "0.1.0"
)