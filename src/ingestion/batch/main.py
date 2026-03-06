"""
Batch Data Ingestion Pipeline
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import col, to_timestamp, lit, current_timestamp
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BatchIngestion:
    """Batch data ingestion from various sources"""
    
    def __init__(self, config: dict):
        self.config = config
        self.spark = None
    
    def create_spark_session(self) -> SparkSession:
        """Create and configure Spark session"""
        self.spark = SparkSession.builder \
            .appName("Chronos-BatchIngestion") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.AzureLogStore") \
            .config("spark.hadoop.fs.azure.account.key", self.config.get("storage_key")) \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        return self.spark
    
    def ingest_from_blob(self, source_path: str, format: str = "csv") -> "DataFrame":
        """Ingest data from Azure Blob Storage"""
        logger.info(f"Ingesting from {source_path}")
        
        df = self.spark.read \
            .format(format) \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load(source_path)
        
        logger.info(f"Ingested {df.count()} records")
        return df
    
    def ingest_from_sql(self, jdbc_url: str, table: str, properties: dict) -> "DataFrame":
        """Ingest data from SQL database"""
        logger.info(f"Ingesting from {table}")
        
        df = self.spark.read \
            .jdbc(url=jdbc_url, table=table, properties=properties)
        
        logger.info(f"Ingested {df.count()} records")
        return df
    
    def write_to_bronze(self, df, table_name: str, partition_by: str = "date"):
        """Write data to bronze (raw) layer"""
        output_path = f"{self.config['bronze_path']}/{table_name}"
        
        df.write \
            .format("delta") \
            .mode("append") \
            .partitionBy(partition_by) \
            .save(output_path)
        
        logger.info(f"Wrote {df.count()} records to bronze/{table_name}")
    
    def run(self):
        """Run batch ingestion pipeline"""
        logger.info("Starting batch ingestion pipeline...")
        
        # Create Spark session
        self.create_spark_session()
        
        # Ingest from various sources
        for source in self.config.get("sources", []):
            source_type = source.get("type")
            
            if source_type == "blob":
                df = self.ingest_from_blob(
                    source.get("path"),
                    source.get("format", "csv")
                )
            elif source_type == "sql":
                df = self.ingest_from_sql(
                    self.config.get("jdbc_url"),
                    source.get("table"),
                    self.config.get("db_properties", {})
                )
            elif source_type == "api":
                # API ingestion would be implemented here
                logger.warning("API ingestion not yet implemented")
                continue
            else:
                logger.warning(f"Unknown source type: {source_type}")
                continue
            
            # Add metadata columns
            df = df.withColumn("ingestion_timestamp", current_timestamp())
            df = df.withColumn("source_system", lit(source.get("name")))
            
            # Write to bronze layer
            self.write_to_bronze(df, source.get("table_name", "unknown"))
        
        logger.info("Batch ingestion pipeline completed")
        
        # Stop Spark session
        if self.spark:
            self.spark.stop()


if __name__ == "__main__":
    config = {
        "storage_key": "your-storage-key",
        "bronze_path": "abfs://data@storageaccount.dfs.core.windows.net/bronze",
        "jdbc_url": "jdbc:sqlserver://server.database.windows.net:1433;database=database",
        "db_properties": {
            "user": "username",
            "password": "password",
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        },
        "sources": [
            {
                "type": "blob",
                "name": "sales",
                "path": "abfs://raw@storageaccount.dfs.core.windows.net/sales/*.csv",
                "format": "csv",
                "table_name": "sales"
            }
        ]
    }
    
    ingestion = BatchIngestion(config)
    ingestion.run()
