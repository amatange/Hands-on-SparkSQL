from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum

# Initialize Spark Session
spark = SparkSession.builder.appName("EngagementByAge").getOrCreate()

# Load data inputs
posts_df = spark.read.option("header", True).option("inferSchema", True).csv("input/posts.csv")
users_df = spark.read.option("header", True).option("inferSchema", True).csv("input/users.csv")

# 1. Join datasets on UserID
joined_df = posts_df.join(users_df, on="UserID", how="inner")

# 2. Group by AgeGroup and aggregate metrics
engagement_by_age = joined_df.groupBy("AgeGroup").agg(_sum("Likes").alias("TotalLikes"), _sum("Retweets").alias("TotalRetweets")).orderBy(col("TotalLikes").desc())

# Save result
engagement_by_age.coalesce(1).write.mode("overwrite").csv("outputs/engagement_by_age.csv", header=True)