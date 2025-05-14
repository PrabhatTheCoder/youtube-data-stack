from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("Read Partitioned CSV") \
    .getOrCreate()

input_path = "s3://youtube-raw-data-apsouth1-s3/youtube/raw_statistics/"

df = spark.read.option("header", "true") \
    .option("inferSchema", "true") \
    .csv(input_path)

filtered_df = df.filter(col("region").isin("ca", "gb", "us"))

selected_df = filtered_df.select(
    col("video_id").cast("string"),
    col("trending_date").cast("string"),
    col("title").cast("string"),
    col("channel_title").cast("string"),
    col("category_id").cast("bigint"),
    col("publish_time").cast("string"),
    col("tags").cast("string"),
    col("views").cast("bigint"),
    col("likes").cast("bigint"),
    col("dislikes").cast("bigint"),
    col("comment_count").cast("bigint"),
    col("thumbnail_link").cast("string"),
    col("comments_disabled").cast("boolean"),
    col("ratings_disabled").cast("boolean"),
    col("video_error_or_removed").cast("boolean"),
    col("description").cast("string"),
    col("region").cast("string")
)

output_base = "s3://yt-cleaned-useast1-dev/youtube/raw_statistics/"

regions = [row["region"] for row in selected_df.select("region").distinct().collect()]

for region in regions:
    region_df = selected_df.filter(col("region") == region).drop("region")
    
    # Coalesce to 1 file and write to the appropriate path
    region_df.coalesce(1).write.mode("overwrite").parquet(f"{output_base}region={region}/")

spark.stop()

