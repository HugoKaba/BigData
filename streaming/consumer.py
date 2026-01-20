from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

APP_NAME = "EcommerceStreaming"
KAFKA_BOOTSTRAP_SERVERS = "kafka:29092" 
KAFKA_TOPIC = "ecommerce_events"

def get_spark_session():
    return SparkSession.builder \
        .appName(APP_NAME) \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()

def run_consumer():
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()
        
    schema = StructType([
        StructField("timestamp", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("product", StringType(), True),
        StructField("price", DoubleType(), True)
    ])
    
    json_df = df.select(F.from_json(F.col("value").cast("string"), schema).alias("data")).select("data.*")
    
    processed_df = json_df.withColumn("timestamp", F.to_timestamp(F.col("timestamp")))
    
    high_value = processed_df.filter((F.col("event_type") == "purchase") & (F.col("price") > 1500))
    
    query_high_value = high_value.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .queryName("HighValueAlerts") \
        .start()
        
    windowed_counts = processed_df.groupBy(
        F.window(F.col("timestamp"), "10 seconds"),
        F.col("event_type")
    ).count().filter("count > 5") 
    
    query_counts = windowed_counts.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", "false") \
        .queryName("TrafficStats") \
        .start()
        
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    run_consumer()
