#!/usr/bin/env python3
"""
Local Spark Runner for Financial Data Pipeline
"""
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
import time
from datetime import datetime

def create_spark_session():
    """Create Spark session with proper configuration."""
    return SparkSession.builder \
        .appName("FinancialDataPipeline") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoint") \
        .getOrCreate()

def process_financial_data():
    """Process financial data with Spark."""
    print("🚀 Starting Spark Financial Data Processing")
    
    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    print("✅ Spark session created")
    
    # Load sample data (in real scenario, this would come from Kafka or files)
    print("📊 Loading financial data...")
    
    # Create sample data
    sample_data = [
        {"symbol": "AAPL", "price": 150.25, "volume": 50000000, "change_percent": 1.5, "timestamp": "2024-01-01 10:00:00"},
        {"symbol": "MSFT", "price": 300.50, "volume": 30000000, "change_percent": -0.5, "timestamp": "2024-01-01 10:00:00"},
        {"symbol": "GOOGL", "price": 250.75, "volume": 20000000, "change_percent": 2.1, "timestamp": "2024-01-01 10:00:00"},
        {"symbol": "AAPL", "price": 151.00, "volume": 45000000, "change_percent": 0.5, "timestamp": "2024-01-01 11:00:00"},
        {"symbol": "MSFT", "price": 301.25, "volume": 35000000, "change_percent": 0.25, "timestamp": "2024-01-01 11:00:00"},
        {"symbol": "GOOGL", "price": 252.00, "volume": 25000000, "change_percent": 0.5, "timestamp": "2024-01-01 11:00:00"},
    ]
    
    # Create DataFrame
    df = spark.createDataFrame(sample_data)
    df = df.withColumn("timestamp", col("timestamp").cast(TimestampType()))
    
    print(f"✅ Loaded {df.count()} records")
    
    # Data transformations
    print("🔄 Processing data transformations...")
    
    # 1. Add calculated fields
    df_processed = df.withColumn("dollar_volume", col("price") * col("volume")) \
                    .withColumn("price_trend", 
                               when(col("change_percent") > 2, "Strong Up")
                               .when(col("change_percent") > 0, "Up")
                               .when(col("change_percent") > -2, "Sideways")
                               .otherwise("Down")) \
                    .withColumn("hour", hour(col("timestamp"))) \
                    .withColumn("date", date_format(col("timestamp"), "yyyy-MM-dd"))
    
    # 2. Aggregations by symbol
    print("📈 Calculating aggregations...")
    
    daily_aggregations = df_processed.groupBy("symbol", "date") \
        .agg(
            avg("price").alias("avg_price"),
            max("price").alias("max_price"),
            min("price").alias("min_price"),
            sum("volume").alias("total_volume"),
            avg("change_percent").alias("avg_change_percent"),
            count("*").alias("record_count")
        ) \
        .withColumn("price_volatility", 
                   (col("max_price") - col("min_price")) / col("avg_price") * 100)
    
    # 3. Market summary
    market_summary = df_processed.groupBy("date") \
        .agg(
            count("symbol").alias("total_stocks"),
            avg("price").alias("avg_price"),
            avg("change_percent").alias("avg_change_percent"),
            sum("volume").alias("total_volume"),
            count(when(col("change_percent") > 0, 1)).alias("gainers"),
            count(when(col("change_percent") < 0, 1)).alias("losers")
        ) \
        .withColumn("market_sentiment",
                   when(col("gainers") > col("losers"), "Bullish")
                   .when(col("losers") > col("gainers"), "Bearish")
                   .otherwise("Neutral"))
    
    # 4. Top performers
    top_performers = df_processed.groupBy("symbol") \
        .agg(
            avg("change_percent").alias("avg_change_percent"),
            sum("volume").alias("total_volume"),
            avg("price").alias("avg_price")
        ) \
        .orderBy(desc("avg_change_percent")) \
        .limit(10)
    
    # Show results
    print("\n📊 DAILY AGGREGATIONS BY SYMBOL:")
    print("=" * 50)
    daily_aggregations.show(20, False)
    
    print("\n📈 MARKET SUMMARY:")
    print("=" * 50)
    market_summary.show(20, False)
    
    print("\n🏆 TOP PERFORMERS:")
    print("=" * 50)
    top_performers.show(20, False)
    
    # Save results
    print("\n💾 Saving results...")
    
    # Create output directory
    os.makedirs("spark_output", exist_ok=True)
    
    # Save as Parquet
    daily_aggregations.write.mode("overwrite").parquet("spark_output/daily_aggregations.parquet")
    market_summary.write.mode("overwrite").parquet("spark_output/market_summary.parquet")
    top_performers.write.mode("overwrite").parquet("spark_output/top_performers.parquet")
    
    # Save as JSON for easy viewing
    daily_aggregations.write.mode("overwrite").json("spark_output/daily_aggregations.json")
    market_summary.write.mode("overwrite").json("spark_output/market_summary.json")
    top_performers.write.mode("overwrite").json("spark_output/top_performers.json")
    
    print("✅ Results saved to spark_output/ directory")
    
    # Show Spark UI info
    print(f"\n🌐 Spark UI available at: http://localhost:4040")
    print(f"📊 Spark Master: {spark.sparkContext.master}")
    print(f"🔧 Spark Version: {spark.version}")
    
    # Keep Spark running for UI access
    print("\n⏳ Keeping Spark session alive for 60 seconds...")
    print("   Visit http://localhost:4040 to see the Spark UI")
    print("   Press Ctrl+C to stop early")
    
    try:
        time.sleep(60)
    except KeyboardInterrupt:
        print("\n⏹️  Stopping Spark session...")
    
    spark.stop()
    print("✅ Spark session stopped")

def run_streaming_example():
    """Run a simple streaming example."""
    print("\n🌊 Running Spark Streaming Example...")
    
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # Create a simple streaming DataFrame
    # In real scenario, this would read from Kafka
    streaming_df = spark \
        .readStream \
        .format("rate") \
        .option("rowsPerSecond", 1) \
        .load()
    
    # Add some processing
    processed_df = streaming_df \
        .withColumn("timestamp", current_timestamp()) \
        .withColumn("random_price", (rand() * 100) + 50) \
        .withColumn("random_volume", (rand() * 1000000) + 100000)
    
    # Write to console (for demo)
    query = processed_df \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .start()
    
    print("✅ Streaming started - will run for 10 seconds...")
    time.sleep(10)
    
    query.stop()
    spark.stop()
    print("✅ Streaming stopped")

if __name__ == "__main__":
    print("🚀 Financial Data Pipeline - Spark Edition")
    print("=" * 50)
    
    try:
        # Run batch processing
        process_financial_data()
        
        # Run streaming example
        run_streaming_example()
        
        print("\n🎉 Spark processing completed successfully!")
        print("\n📁 Check the following directories:")
        print("   - spark_output/ : Processed data files")
        print("   - data/raw/     : Raw financial data")
        print("   - data/processed/: Cleaned financial data")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        sys.exit(1)
