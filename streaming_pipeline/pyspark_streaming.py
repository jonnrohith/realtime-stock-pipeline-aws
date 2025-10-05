"""
PySpark Structured Streaming for Real-time Financial Data Processing
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_json, current_timestamp, window, 
    avg, max, min, count, sum, first, last, 
    when, isnan, isnull, coalesce, lit, regexp_replace,
    split, explode, struct, to_timestamp, date_format
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    IntegerType, TimestampType, BooleanType, ArrayType
)
from pyspark.sql.streaming import StreamingQuery
import structlog
from typing import Dict

logger = structlog.get_logger()


class FinancialDataStreamProcessor:
    """PySpark Structured Streaming processor for financial data."""
    
    def __init__(self, 
                 kafka_bootstrap_servers: str = "localhost:9092",
                 checkpoint_location: str = "/tmp/spark-checkpoint",
                 output_path: str = "s3a://your-bucket/financial-data"):
        """
        Initialize the stream processor.
        
        Args:
            kafka_bootstrap_servers: Kafka bootstrap servers
            checkpoint_location: Spark checkpoint location
            output_path: Output path for processed data
        """
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.checkpoint_location = checkpoint_location
        self.output_path = output_path
        
        # Initialize Spark session
        self.spark = SparkSession.builder \
            .appName("FinancialDataStreamProcessor") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.streaming.checkpointLocation", checkpoint_location) \
            .getOrCreate()
        
        # Set log level
        self.spark.sparkContext.setLogLevel("WARN")
        
        # Define schemas
        self.schemas = self._define_schemas()
        
        logger.info("Stream processor initialized", 
                   kafka_servers=kafka_bootstrap_servers,
                   checkpoint_location=checkpoint_location)
    
    def _define_schemas(self) -> Dict[str, StructType]:
        """Define schemas for different data types."""
        # Base message schema
        base_schema = StructType([
            StructField("timestamp", StringType(), True),
            StructField("data_type", StringType(), True),
            StructField("metadata", StructType([
                StructField("job_id", StringType(), True),
                StructField("source", StringType(), True)
            ]), True)
        ])
        
        # Stock quote schema
        stock_quote_schema = StructType([
            StructField("symbol", StringType(), True),
            StructField("name", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("previous_close", DoubleType(), True),
            StructField("open", DoubleType(), True),
            StructField("high", DoubleType(), True),
            StructField("low", DoubleType(), True),
            StructField("volume", IntegerType(), True),
            StructField("market_cap", DoubleType(), True),
            StructField("pe_ratio", DoubleType(), True),
            StructField("dividend_yield", DoubleType(), True),
            StructField("change", DoubleType(), True),
            StructField("change_percent", DoubleType(), True),
            StructField("currency", StringType(), True),
            StructField("exchange", StringType(), True),
            StructField("quote_type", StringType(), True),
            StructField("timestamp", StringType(), True)
        ])
        
        # Market screener schema
        market_screener_schema = StructType([
            StructField("symbol", StringType(), True),
            StructField("name", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("change", DoubleType(), True),
            StructField("change_percent", DoubleType(), True),
            StructField("volume", IntegerType(), True),
            StructField("market_cap", DoubleType(), True),
            StructField("screener_type", StringType(), True),
            StructField("rank", IntegerType(), True)
        ])
        
        # Stock news schema
        stock_news_schema = StructType([
            StructField("symbol", StringType(), True),
            StructField("title", StringType(), True),
            StructField("url", StringType(), True),
            StructField("text", StringType(), True),
            StructField("source", StringType(), True),
            StructField("news_type", StringType(), True),
            StructField("published_time", StringType(), True),
            StructField("image_url", StringType(), True)
        ])
        
        return {
            "stock_quotes": StructType(base_schema.fields + [
                StructField("data", stock_quote_schema, True)
            ]),
            "market_screeners": StructType(base_schema.fields + [
                StructField("data", market_screener_schema, True)
            ]),
            "stock_news": StructType(base_schema.fields + [
                StructField("data", stock_news_schema, True)
            ])
        }
    
    def create_stock_quotes_stream(self) -> StreamingQuery:
        """Create streaming query for stock quotes."""
        logger.info("Creating stock quotes stream")
        
        # Read from Kafka
        kafka_df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
            .option("subscribe", "yahoo-stock-quotes") \
            .option("startingOffsets", "latest") \
            .load()
        
        # Parse JSON and apply schema
        quotes_df = kafka_df \
            .select(
                from_json(col("value").cast("string"), self.schemas["stock_quotes"]).alias("data")
            ) \
            .select("data.*") \
            .select(
                col("timestamp").cast(TimestampType()).alias("message_timestamp"),
                col("data_type"),
                col("data.symbol"),
                col("data.name"),
                col("data.price"),
                col("data.previous_close"),
                col("data.open"),
                col("data.high"),
                col("data.low"),
                col("data.volume"),
                col("data.market_cap"),
                col("data.pe_ratio"),
                col("data.dividend_yield"),
                col("data.change"),
                col("data.change_percent"),
                col("data.currency"),
                col("data.exchange"),
                col("data.quote_type"),
                col("data.timestamp").cast(TimestampType()).alias("quote_timestamp"),
                col("metadata.job_id"),
                col("metadata.source")
            )
        
        # Data quality and cleaning
        cleaned_quotes = quotes_df \
            .withColumn("price", when(isnan(col("price")) | isnull(col("price")), lit(0.0)).otherwise(col("price"))) \
            .withColumn("volume", when(isnan(col("volume")) | isnull(col("volume")), lit(0)).otherwise(col("volume"))) \
            .withColumn("change_percent", when(isnan(col("change_percent")) | isnull(col("change_percent")), lit(0.0)).otherwise(col("change_percent"))) \
            .withColumn("market_cap_category", 
                       when(col("market_cap") >= 200_000_000_000, "Mega Cap")
                       .when(col("market_cap") >= 10_000_000_000, "Large Cap")
                       .when(col("market_cap") >= 2_000_000_000, "Mid Cap")
                       .when(col("market_cap") >= 300_000_000, "Small Cap")
                       .otherwise("Micro Cap")) \
            .withColumn("price_trend",
                       when(col("change_percent") > 2, "Strong Up")
                       .when(col("change_percent") > 0, "Up")
                       .when(col("change_percent") > -2, "Sideways")
                       .when(col("change_percent") > -5, "Down")
                       .otherwise("Strong Down"))
        
        # Write to data lake
        query = cleaned_quotes \
            .writeStream \
            .format("parquet") \
            .option("path", f"{self.output_path}/stock_quotes") \
            .option("checkpointLocation", f"{self.checkpoint_location}/stock_quotes") \
            .trigger(processingTime="30 seconds") \
            .start()
        
        return query
    
    def create_market_screeners_stream(self) -> StreamingQuery:
        """Create streaming query for market screeners."""
        logger.info("Creating market screeners stream")
        
        # Read from Kafka
        kafka_df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
            .option("subscribe", "yahoo-market-screeners") \
            .option("startingOffsets", "latest") \
            .load()
        
        # Parse JSON and apply schema
        screeners_df = kafka_df \
            .select(
                from_json(col("value").cast("string"), self.schemas["market_screeners"]).alias("data")
            ) \
            .select("data.*") \
            .select(
                col("timestamp").cast(TimestampType()).alias("message_timestamp"),
                col("data_type"),
                col("data.symbol"),
                col("data.name"),
                col("data.price"),
                col("data.change"),
                col("data.change_percent"),
                col("data.volume"),
                col("data.market_cap"),
                col("data.screener_type"),
                col("data.rank"),
                col("metadata.job_id"),
                col("metadata.source")
            )
        
        # Data quality and cleaning
        cleaned_screeners = screeners_df \
            .withColumn("price", when(isnan(col("price")) | isnull(col("price")), lit(0.0)).otherwise(col("price"))) \
            .withColumn("change_percent", when(isnan(col("change_percent")) | isnull(col("change_percent")), lit(0.0)).otherwise(col("change_percent"))) \
            .withColumn("volume", when(isnan(col("volume")) | isnull(col("volume")), lit(0)).otherwise(col("volume"))) \
            .withColumn("performance_category",
                       when(col("change_percent") > 10, "Outstanding")
                       .when(col("change_percent") > 5, "Excellent")
                       .when(col("change_percent") > 0, "Good")
                       .when(col("change_percent") > -5, "Poor")
                       .otherwise("Terrible"))
        
        # Write to data lake
        query = cleaned_screeners \
            .writeStream \
            .format("parquet") \
            .option("path", f"{self.output_path}/market_screeners") \
            .option("checkpointLocation", f"{self.checkpoint_location}/market_screeners") \
            .trigger(processingTime="60 seconds") \
            .start()
        
        return query
    
    def create_stock_news_stream(self) -> StreamingQuery:
        """Create streaming query for stock news."""
        logger.info("Creating stock news stream")
        
        # Read from Kafka
        kafka_df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
            .option("subscribe", "yahoo-stock-news") \
            .option("startingOffsets", "latest") \
            .load()
        
        # Parse JSON and apply schema
        news_df = kafka_df \
            .select(
                from_json(col("value").cast("string"), self.schemas["stock_news"]).alias("data")
            ) \
            .select("data.*") \
            .select(
                col("timestamp").cast(TimestampType()).alias("message_timestamp"),
                col("data_type"),
                col("data.symbol"),
                col("data.title"),
                col("data.url"),
                col("data.text"),
                col("data.source"),
                col("data.news_type"),
                col("data.published_time").cast(TimestampType()).alias("published_timestamp"),
                col("data.image_url"),
                col("metadata.job_id"),
                col("metadata.source")
            )
        
        # Data quality and cleaning
        cleaned_news = news_df \
            .withColumn("title", regexp_replace(col("title"), r'[^\w\s]', '')) \
            .withColumn("text", regexp_replace(col("text"), r'[^\w\s]', '')) \
            .withColumn("word_count", 
                       size(split(col("title"), " ")) + 
                       size(split(col("text"), " "))) \
            .withColumn("news_category",
                       when(col("title").rlike("(?i)(earnings|revenue|profit)"), "Financial")
                       .when(col("title").rlike("(?i)(merger|acquisition|deal)"), "M&A")
                       .when(col("title").rlike("(?i)(product|launch|release)"), "Product")
                       .when(col("title").rlike("(?i)(ceo|executive|management)"), "Leadership")
                       .otherwise("General"))
        
        # Write to data lake
        query = cleaned_news \
            .writeStream \
            .format("parquet") \
            .option("path", f"{self.output_path}/stock_news") \
            .option("checkpointLocation", f"{self.checkpoint_location}/stock_news") \
            .trigger(processingTime="60 seconds") \
            .start()
        
        return query
    
    def create_aggregated_quotes_stream(self) -> StreamingQuery:
        """Create aggregated quotes stream with windowing."""
        logger.info("Creating aggregated quotes stream")
        
        # Read from Kafka
        kafka_df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
            .option("subscribe", "yahoo-stock-quotes") \
            .option("startingOffsets", "latest") \
            .load()
        
        # Parse and clean data
        quotes_df = kafka_df \
            .select(
                from_json(col("value").cast("string"), self.schemas["stock_quotes"]).alias("data")
            ) \
            .select("data.*") \
            .select(
                col("timestamp").cast(TimestampType()).alias("message_timestamp"),
                col("data.symbol"),
                col("data.price"),
                col("data.volume"),
                col("data.change_percent"),
                col("data.market_cap")
            ) \
            .filter(col("price").isNotNull() & (col("price") > 0))
        
        # Create windowed aggregations
        windowed_quotes = quotes_df \
            .withWatermark("message_timestamp", "1 minute") \
            .groupBy(
                window(col("message_timestamp"), "5 minutes"),
                col("symbol")
            ) \
            .agg(
                avg("price").alias("avg_price"),
                max("price").alias("max_price"),
                min("price").alias("min_price"),
                first("price").alias("open_price"),
                last("price").alias("close_price"),
                sum("volume").alias("total_volume"),
                avg("change_percent").alias("avg_change_percent"),
                avg("market_cap").alias("avg_market_cap"),
                count("*").alias("record_count")
            ) \
            .withColumn("price_volatility", 
                       (col("max_price") - col("min_price")) / col("avg_price") * 100) \
            .withColumn("window_start", col("window.start")) \
            .withColumn("window_end", col("window.end"))
        
        # Write aggregated data
        query = windowed_quotes \
            .writeStream \
            .format("parquet") \
            .option("path", f"{self.output_path}/aggregated_quotes") \
            .option("checkpointLocation", f"{self.checkpoint_location}/aggregated_quotes") \
            .trigger(processingTime="5 minutes") \
            .start()
        
        return query
    
    def create_market_summary_stream(self) -> StreamingQuery:
        """Create market summary stream."""
        logger.info("Creating market summary stream")
        
        # Read from Kafka
        kafka_df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
            .option("subscribe", "yahoo-stock-quotes") \
            .option("startingOffsets", "latest") \
            .load()
        
        # Parse and clean data
        quotes_df = kafka_df \
            .select(
                from_json(col("value").cast("string"), self.schemas["stock_quotes"]).alias("data")
            ) \
            .select("data.*") \
            .select(
                col("timestamp").cast(TimestampType()).alias("message_timestamp"),
                col("data.symbol"),
                col("data.price"),
                col("data.change_percent"),
                col("data.volume"),
                col("data.market_cap")
            ) \
            .filter(col("price").isNotNull() & (col("price") > 0))
        
        # Create market summary
        market_summary = quotes_df \
            .withWatermark("message_timestamp", "1 minute") \
            .groupBy(window(col("message_timestamp"), "10 minutes")) \
            .agg(
                count("symbol").alias("total_stocks"),
                avg("price").alias("avg_price"),
                avg("change_percent").alias("avg_change_percent"),
                sum("volume").alias("total_volume"),
                sum("market_cap").alias("total_market_cap"),
                count(when(col("change_percent") > 0, 1)).alias("gainers"),
                count(when(col("change_percent") < 0, 1)).alias("losers"),
                count(when(col("change_percent") == 0, 1)).alias("unchanged")
            ) \
            .withColumn("market_sentiment",
                       when(col("gainers") > col("losers"), "Bullish")
                       .when(col("losers") > col("gainers"), "Bearish")
                       .otherwise("Neutral")) \
            .withColumn("window_start", col("window.start")) \
            .withColumn("window_end", col("window.end"))
        
        # Write market summary
        query = market_summary \
            .writeStream \
            .format("parquet") \
            .option("path", f"{self.output_path}/market_summary") \
            .option("checkpointLocation", f"{self.checkpoint_location}/market_summary") \
            .trigger(processingTime="10 minutes") \
            .start()
        
        return query
    
    def start_all_streams(self):
        """Start all streaming queries."""
        logger.info("Starting all streaming queries")
        
        queries = []
        
        try:
            # Start individual streams
            queries.append(("stock_quotes", self.create_stock_quotes_stream()))
            queries.append(("market_screeners", self.create_market_screeners_stream()))
            queries.append(("stock_news", self.create_stock_news_stream()))
            queries.append(("aggregated_quotes", self.create_aggregated_quotes_stream()))
            queries.append(("market_summary", self.create_market_summary_stream()))
            
            logger.info("All streaming queries started", count=len(queries))
            
            # Wait for termination
            for name, query in queries:
                query.awaitTermination()
                
        except Exception as e:
            logger.error("Error in streaming queries", error=str(e))
            # Stop all queries
            for name, query in queries:
                query.stop()
            raise
    
    def stop_all_streams(self):
        """Stop all streaming queries."""
        logger.info("Stopping all streaming queries")
        # This would need to be implemented to track and stop queries
        pass


def main():
    """Main function for running the stream processor."""
    processor = FinancialDataStreamProcessor(
        kafka_bootstrap_servers="localhost:9092",
        checkpoint_location="/tmp/spark-checkpoint",
        output_path="s3a://your-bucket/financial-data"
    )
    
    try:
        print("🚀 Starting PySpark Structured Streaming")
        print("📊 Processing real-time financial data")
        print("⏹️  Press Ctrl+C to stop")
        
        processor.start_all_streams()
        
    except KeyboardInterrupt:
        print("\n⏹️  Stopping stream processor...")
    finally:
        processor.stop_all_streams()
        print("✅ Stream processor stopped")


if __name__ == "__main__":
    main()
