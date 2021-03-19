# Twitter-Sentiment-Project
This project aims to do the sentiment of the tweets polarity prediction using Scala, Apache Spark, and MongoDB. And there are 3 main components: MongoDB, Spark NLP, and Spark Streaming.

# MongoDB
Read data from and push data into MongoDB by leveraging Spark MongoDB connector.

# Spark NLP
By using a pre-trained pipeline from Spark NLP, I can compute the sentiment score for any given text. Before directly apply the Spark NLP into streaming tweeter data, I use a famous NLP dataset called Sentiment140 for the accuracy test. In detail, the Sentiment140 dataset contains 1.6 million tweets with sentiment labels collected by Stanford and the accuracy of Spark NLP text sentiment prediction is around 84% which is good enough for a common purpose.

# Spark Streaming
For a user-defined keyword, I extract related real-time streaming tweets and compute corresponding sentiment polarity result for each one by using Twitter Developer API and Spark Streaming library.
