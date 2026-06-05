from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum

# Initialize Spark Session
spark = SparkSession.builder.appName("TopVerifiedUsers").getOrCreate()

# Load data inputs
posts_df = spark.read.option("header", True).option("inferSchema", True).csv("input/posts.csv")
users_df = spark.read.option("header", True).option("inferSchema", True).csv("input/users.csv")

# 1. Isolate verified user base 
verified_users = users_df.filter(col("Verified") == True)

# 2. Join with posts matrix
verified_posts = posts_df.join(verified_users, on="UserID", how="inner")

# 3. Aggregate performance statistics by verified profile
top_verified = verified_posts.groupBy("UserID", "Username", "Country").agg(_sum("Likes").alias("CumulativeLikes"), _sum("Retweets").alias("CumulativeRetweets")).orderBy(col("CumulativeLikes").desc())

# Save result
top_verified.coalesce(1).write.mode("overwrite").csv("outputs/top_verified_users.csv", header=True)