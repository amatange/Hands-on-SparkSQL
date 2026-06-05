from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg, count, round

# Initialize Spark Session
spark = SparkSession.builder.appName("SentimentEngagement").getOrCreate()

# Load posts data
posts_df = spark.read.option("header", True).option("inferSchema", True).csv("input/posts.csv")

# 1. Bucketing: Classify SentimentScore into Positive, Neutral, or Negative segments
# SentimentScore scales from -1.0 to 1.0
categorized_df = posts_df.withColumn(
    "SentimentCategory",
    when(col("SentimentScore") > 0.2, "Positive")
    .when(col("SentimentScore") < -0.2, "Negative")
    .otherwise("Neutral")
)

# 2. Group by category and assess average visibility performance
sentiment_engagement = categorized_df.groupBy("SentimentCategory").agg(count("PostID").alias("TotalPosts"), round(avg("Likes"), 2).alias("AvgLikes"), round(avg("Retweets"), 2).alias("AvgRetweets")).orderBy(col("AvgLikes").desc())

# Save result
sentiment_engagement.coalesce(1).write.mode("overwrite").csv("outputs/sentiment_engagement.csv", header=True)