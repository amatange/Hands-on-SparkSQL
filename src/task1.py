from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col, count

# Initialize Spark Session
spark = SparkSession.builder.appName("HashtagTrends").getOrCreate()

# Load posts data
posts_df = spark.read.option("header", True).csv("input/posts.csv")

# TODO: Split the Hashtags column into individual hashtags and count the frequency of each hashtag and sort descending
hashtag_df = posts_df.withColumn("Hashtag", explode(split(col("Hashtags"), ",")))
hashtag_counts = hashtag_df.groupBy("Hashtag").agg(count("PostID").alias("Count")).orderBy(col("Count").desc())

# Save result
hashtag_counts.coalesce(1).write.mode("overwrite").csv("outputs/hashtag_trends.csv", header=True)
