"""
Streaming Ingestion Pipeline
Real-time data streaming using Azure Event Hubs and Spark Structured Streaming
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, to_json, struct, 
    window, watermark, avg, count, sum as spark_sum,
    current_timestamp, expr
)
from pyspark.sql.types import (
    StructType, StructField, StringType, 
    DoubleType, IntegerType, TimestampType, BooleanType
import os

# Configuration
EVENT_HUBS_CONNECTION_STRING = os.getenv("EVENT_HUBS_CONNECTION_STRING", "")
EVENT_HUBS_NAMESPACE = os.getenv("EVENT_HUBS_NAMESPACE", "chronos-events")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
CHECKPOINT_LOCATION = os.getenv("CHECKPOINT_LOCATION", "abfss://checkpoints@storage.dfs.core.windows.net/streaming")

# Schema definitions
def get_transaction_schema():
    return StructType([
        StructField("transaction_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("amount", DoubleType(), False),
        StructField("currency", StringType(), True),
        StructField("merchant_id", StringType(), True),
        StructField("merchant_category", StringType(), True),
        StructField("timestamp", TimestampType(), False),
        StructField("location", StringType(), True),
        StructField("is_fraud", BooleanType(), True),
    ])

def get_iot_telemetry_schema():
    return StructType([
        StructField("device_id", StringType(), False),
        StructField("sensor_type", StringType(), False),
        StructField("value", DoubleType(), False),
        StructField("unit", StringType(), True),
        StructField("timestamp", TimestampType(), False),
        StructField("location", StringType(), True),
        StructField("battery_level", DoubleType(), True),
    ])

def get_user_event_schema():
    return StructType([
        StructField("event_id", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("page_url", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("timestamp", TimestampType(), False),
        StructField("metadata", StringType(), True),
    ])


class StreamingIngestionPipeline:
    """Main streaming ingestion pipeline"""
    
    def __init__(self, app_name: str):
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.streaming.stopGracefullyOnShutdown", "true") \
            .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_LOCATION) \
            .config("spark.eventhubs.connectionString", EVENT_HUBS_CONNECTION_STRING) \
            .config("spark.eventhubs.consumerGroup", "$Default") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
    
    def create_eventhubs_source(self, event_hub_name: str):
        """Create Event Hubs source for streaming"""
        return self.spark.readStream \
            .format("eventhubs") \
            .option("eventHubName", event_hub_name) \
            .load()
    
    def create_kafka_source(self, topic: str):
        """Create Kafka source for streaming"""
        return self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
            .option("subscribe", topic) \
            .option("startingOffsets", "earliest") \
            .load()
    
    def process_transactions(self, raw_df):
        """Process financial transactions in real-time"""
        schema = get_transaction_schema()
        
        parsed_df = raw_df.select(
            from_json(col("body").cast("string"), schema).alias("data")
        ).select("data.*")
        
        # Windowed aggregations for fraud detection
        windowed_df = parsed_df \
            .withWatermark("timestamp", "5 minutes") \
            .groupBy(
                window(col("timestamp"), "1 minute", "30 seconds"),
                col("customer_id")
            ) \
            .agg(
                count("transaction_id").alias("transaction_count"),
                spark_sum("amount").alias("total_amount"),
                avg("amount").alias("avg_amount")
            )
        
        # Write to Delta Lake
        query = windowed_df \
            .writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("checkpointLocation", f"{CHECKPOINT_LOCATION}/transactions") \
            .start("abfss://datalake@storage.dfs.core.windows.net/silver/transactions")
        
        return query
    
    def process_iot_telemetry(self, raw_df):
        """Process IoT device telemetry in real-time"""
        schema = get_iot_telemetry_schema()
        
        parsed_df = raw_df.select(
            from_json(col("body").cast("string"), schema).alias("data")
        ).select("data.*")
        
        # Aggregate by device and time window
        windowed_df = parsed_df \
            .withWatermark("timestamp", "10 minutes") \
            .groupBy(
                window(col("timestamp"), "5 minutes"),
                col("device_id"),
                col("sensor_type")
            ) \
            .agg(
                avg("value").alias("avg_value"),
                count("value").alias("reading_count"),
                spark_sum("value").alias("total_value")
            )
        
        # Write to Delta Lake
        query = windowed_df \
            .writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("checkpointLocation", f"{CHECKPOINT_LOCATION}/iot") \
            .start("abfss://datalake@storage.dfs.core.windows.net/silver/iot_telemetry")
        
        return query
    
    def process_user_events(self, raw_df):
        """Process user web/app events for analytics"""
        schema = get_user_event_schema()
        
        parsed_df = raw_df.select(
            from_json(col("body").cast("string"), schema).alias("data")
        ).select("data.*")
        
        # Session-based aggregations
        session_df = parsed_df \
            .withWatermark("timestamp", "30 minutes") \
            .groupBy(
                window(col("timestamp"), "15 minutes"),
                col("session_id")
            ) \
            .agg(
                count("event_id").alias("event_count"),
                count("user_id").alias("user_count")
            )
        
        # Write to Delta Lake
        query = session_df \
            .writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("checkpointLocation", f"{CHECKPOINT_LOCATION}/events") \
            .start("abfss://datalake@storage.dfs.core.windows.net/silver/user_events")
        
        return query
    
    def run(self):
        """Run the streaming pipeline"""
        # Subscribe to multiple Event Hubs
        transactions_raw = self.create_eventhubs_source("transactions")
        iot_raw = self.create_eventhubs_source("iot-telemetry")
        events_raw = self.create_eventhubs_source("user-events")
        
        # Start processing streams
        t_query = self.process_transactions(transactions_raw)
        iot_query = self.process_iot_telemetry(iot_raw)
        events_query = self.process_user_events(events_raw)
        
        # Wait for all queries to terminate
        self.spark.streams.awaitAnyTermination()


def main():
    """Main entry point"""
    pipeline = StreamingIngestionPipeline("chronos-streaming")
    pipeline.run()


if __name__ == "__main__":
    main()
