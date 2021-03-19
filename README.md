# Twitter-Sentiment-Project

This project aims to do the tweets sentiment polarity prediction using Scala, Apache Spark, and MongoDB. And there are 3 main component: MongoDB, Spark NLP, and Spark Streaming.

# MongoDB
Read from and push into MongoDB by leveraging Spark mongodb connector.

# Spark NLP
By using pretrained pipeline from Spark NLP, I am able to compute the sentiment score for any given text. Before directly apply the Spark NLP into streaming tweeter data, I use a famous NLP dataset called Sentiment140 for accuracy test. In details, the Sentiment140 dataset contains 1.6 million tweets with sentiment label collected by Stanford and the accuracy of Spark NLP text sentiment prediction is around 84% which is good enough for common purpose.

# Spark Streaming
For a user-defined keyword, I extract related real-time streaming tweets and compute corresponding sentiment polarity result for each one by using Twitter Developer API and Spark Streaming libirary.
