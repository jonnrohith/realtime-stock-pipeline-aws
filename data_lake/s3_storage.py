"""
S3/MinIO Data Lake Storage Integration
"""
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import structlog
from pathlib import Path
import json

logger = structlog.get_logger()


class S3DataLakeStorage:
    """S3/MinIO data lake storage manager."""
    
    def __init__(self, 
                 endpoint_url: str = None,
                 access_key: str = None,
                 secret_key: str = None,
                 bucket_name: str = "financial-data",
                 region: str = "us-east-1"):
        """
        Initialize S3/MinIO storage.
        
        Args:
            endpoint_url: MinIO endpoint URL (None for AWS S3)
            access_key: Access key
            secret_key: Secret key
            bucket_name: S3 bucket name
            region: AWS region
        """
        self.endpoint_url = endpoint_url
        self.bucket_name = bucket_name
        self.region = region
        
        # Initialize S3 client
        session = boto3.Session(
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region
        )
        
        if endpoint_url:
            # MinIO configuration
            self.s3_client = session.client(
                's3',
                endpoint_url=endpoint_url
            )
        else:
            # AWS S3 configuration
            self.s3_client = session.client('s3')
        
        # Create bucket if it doesn't exist
        self._create_bucket_if_not_exists()
        
        logger.info("S3 data lake storage initialized", 
                   bucket=bucket_name, 
                   endpoint=endpoint_url or "AWS S3")
    
    def _create_bucket_if_not_exists(self):
        """Create bucket if it doesn't exist."""
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            logger.info("Bucket already exists", bucket=self.bucket_name)
        except:
            try:
                if self.endpoint_url:
                    # MinIO
                    self.s3_client.create_bucket(Bucket=self.bucket_name)
                else:
                    # AWS S3
                    self.s3_client.create_bucket(
                        Bucket=self.bucket_name,
                        CreateBucketConfiguration={'LocationConstraint': self.region}
                    )
                logger.info("Bucket created", bucket=self.bucket_name)
            except Exception as e:
                logger.error("Failed to create bucket", error=str(e))
                raise
    
    def upload_parquet_data(self, 
                           data: List[Dict[str, Any]], 
                           table_name: str,
                           partition_columns: List[str] = None,
                           compression: str = "snappy") -> str:
        """
        Upload data as Parquet to S3.
        
        Args:
            data: List of dictionaries containing data
            table_name: Name of the table/dataset
            partition_columns: Columns to partition by
            compression: Compression algorithm
            
        Returns:
            S3 path where data was uploaded
        """
        if not data:
            logger.warning("No data to upload", table_name=table_name)
            return None
        
        # Create DataFrame
        df = pd.DataFrame(data)
        
        # Generate S3 path with partitioning
        s3_path = self._generate_s3_path(table_name, partition_columns)
        
        try:
            # Convert to PyArrow Table
            table = pa.Table.from_pandas(df)
            
            # Write to S3 as Parquet
            pq.write_to_dataset(
                table,
                root_path=f"s3://{self.bucket_name}/{s3_path}",
                partition_cols=partition_columns,
                compression=compression,
                filesystem=pa.fs.S3FileSystem(
                    endpoint_override=self.endpoint_url,
                    access_key=self.s3_client._client_config.access_key,
                    secret_key=self.s3_client._client_config.secret_key,
                    region=self.region
                )
            )
            
            logger.info("Data uploaded to S3", 
                       table_name=table_name,
                       records=len(data),
                       s3_path=s3_path)
            
            return s3_path
            
        except Exception as e:
            logger.error("Failed to upload data to S3", 
                        table_name=table_name, error=str(e))
            raise
    
    def upload_json_data(self, 
                        data: List[Dict[str, Any]], 
                        table_name: str,
                        partition_by_date: bool = True) -> str:
        """
        Upload data as JSON to S3.
        
        Args:
            data: List of dictionaries containing data
            table_name: Name of the table/dataset
            partition_by_date: Whether to partition by date
            
        Returns:
            S3 path where data was uploaded
        """
        if not data:
            logger.warning("No data to upload", table_name=table_name)
            return None
        
        # Generate S3 path
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        if partition_by_date:
            date_partition = datetime.now().strftime("%Y/%m/%d")
            s3_path = f"{table_name}/date={date_partition}/{table_name}_{timestamp}.json"
        else:
            s3_path = f"{table_name}/{table_name}_{timestamp}.json"
        
        try:
            # Upload JSON data
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_path,
                Body=json.dumps(data, default=str, indent=2),
                ContentType='application/json'
            )
            
            logger.info("JSON data uploaded to S3", 
                       table_name=table_name,
                       records=len(data),
                       s3_path=s3_path)
            
            return s3_path
            
        except Exception as e:
            logger.error("Failed to upload JSON data to S3", 
                        table_name=table_name, error=str(e))
            raise
    
    def read_parquet_data(self, 
                         table_name: str,
                         start_date: str = None,
                         end_date: str = None,
                         filters: List[tuple] = None) -> pd.DataFrame:
        """
        Read Parquet data from S3.
        
        Args:
            table_name: Name of the table/dataset
            start_date: Start date filter (YYYY-MM-DD)
            end_date: End date filter (YYYY-MM-DD)
            filters: Additional filters
            
        Returns:
            DataFrame with the data
        """
        try:
            # Build S3 path
            s3_path = f"s3://{self.bucket_name}/{table_name}/"
            
            # Read Parquet data
            df = pd.read_parquet(
                s3_path,
                engine='pyarrow',
                filesystem=pa.fs.S3FileSystem(
                    endpoint_override=self.endpoint_url,
                    access_key=self.s3_client._client_config.access_key,
                    secret_key=self.s3_client._client_config.secret_key,
                    region=self.region
                )
            )
            
            # Apply date filters if provided
            if start_date or end_date:
                if 'date' in df.columns:
                    df['date'] = pd.to_datetime(df['date'])
                    if start_date:
                        df = df[df['date'] >= start_date]
                    if end_date:
                        df = df[df['date'] <= end_date]
            
            # Apply additional filters
            if filters:
                for column, operator, value in filters:
                    if operator == '==':
                        df = df[df[column] == value]
                    elif operator == '>':
                        df = df[df[column] > value]
                    elif operator == '<':
                        df = df[df[column] < value]
                    elif operator == '>=':
                        df = df[df[column] >= value]
                    elif operator == '<=':
                        df = df[df[column] <= value]
                    elif operator == 'in':
                        df = df[df[column].isin(value)]
            
            logger.info("Data read from S3", 
                       table_name=table_name,
                       records=len(df),
                       s3_path=s3_path)
            
            return df
            
        except Exception as e:
            logger.error("Failed to read data from S3", 
                        table_name=table_name, error=str(e))
            raise
    
    def list_tables(self) -> List[str]:
        """List all tables in the data lake."""
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Delimiter='/'
            )
            
            tables = []
            if 'CommonPrefixes' in response:
                for prefix in response['CommonPrefixes']:
                    table_name = prefix['Prefix'].rstrip('/')
                    tables.append(table_name)
            
            logger.info("Tables listed", count=len(tables))
            return tables
            
        except Exception as e:
            logger.error("Failed to list tables", error=str(e))
            return []
    
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """Get information about a table."""
        try:
            # List objects in the table
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=f"{table_name}/"
            )
            
            if 'Contents' not in response:
                return {"error": "Table not found"}
            
            objects = response['Contents']
            total_size = sum(obj['Size'] for obj in objects)
            file_count = len(objects)
            
            # Get date range if partitioned by date
            dates = []
            for obj in objects:
                key_parts = obj['Key'].split('/')
                for i, part in enumerate(key_parts):
                    if part.startswith('date='):
                        dates.append(part.split('=')[1])
                        break
            
            info = {
                "table_name": table_name,
                "file_count": file_count,
                "total_size_bytes": total_size,
                "total_size_mb": round(total_size / (1024 * 1024), 2),
                "date_range": {
                    "min": min(dates) if dates else None,
                    "max": max(dates) if dates else None
                },
                "last_modified": max(obj['LastModified'] for obj in objects).isoformat()
            }
            
            logger.info("Table info retrieved", table_name=table_name)
            return info
            
        except Exception as e:
            logger.error("Failed to get table info", 
                        table_name=table_name, error=str(e))
            return {"error": str(e)}
    
    def delete_old_data(self, 
                       table_name: str, 
                       days_to_keep: int = 30) -> int:
        """
        Delete old data from a table.
        
        Args:
            table_name: Name of the table
            days_to_keep: Number of days to keep
            
        Returns:
            Number of objects deleted
        """
        try:
            cutoff_date = datetime.now() - timedelta(days=days_to_keep)
            
            # List objects in the table
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=f"{table_name}/"
            )
            
            if 'Contents' not in response:
                return 0
            
            objects_to_delete = []
            for obj in response['Contents']:
                if obj['LastModified'].replace(tzinfo=None) < cutoff_date:
                    objects_to_delete.append({'Key': obj['Key']})
            
            if not objects_to_delete:
                logger.info("No old data to delete", table_name=table_name)
                return 0
            
            # Delete objects in batches
            deleted_count = 0
            batch_size = 1000
            
            for i in range(0, len(objects_to_delete), batch_size):
                batch = objects_to_delete[i:i + batch_size]
                
                delete_response = self.s3_client.delete_objects(
                    Bucket=self.bucket_name,
                    Delete={
                        'Objects': batch,
                        'Quiet': True
                    }
                )
                
                deleted_count += len(batch)
            
            logger.info("Old data deleted", 
                       table_name=table_name,
                       deleted_count=deleted_count,
                       days_kept=days_to_keep)
            
            return deleted_count
            
        except Exception as e:
            logger.error("Failed to delete old data", 
                        table_name=table_name, error=str(e))
            return 0
    
    def _generate_s3_path(self, 
                         table_name: str, 
                         partition_columns: List[str] = None) -> str:
        """Generate S3 path with partitioning."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        if partition_columns and 'date' in partition_columns:
            date_partition = datetime.now().strftime("%Y/%m/%d")
            return f"{table_name}/date={date_partition}/"
        else:
            return f"{table_name}/"
    
    def create_data_lake_structure(self):
        """Create standard data lake structure."""
        structure = {
            "raw": [
                "stock_quotes",
                "market_screeners", 
                "stock_news",
                "stock_history"
            ],
            "processed": [
                "cleaned_quotes",
                "enriched_quotes",
                "aggregated_quotes",
                "market_summary"
            ],
            "analytics": [
                "daily_summary",
                "sector_performance",
                "top_performers",
                "news_sentiment"
            ]
        }
        
        for layer, tables in structure.items():
            for table in tables:
                # Create empty structure (just metadata)
                metadata = {
                    "layer": layer,
                    "table_name": table,
                    "created_at": datetime.utcnow().isoformat(),
                    "schema_version": "1.0"
                }
                
                self.s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=f"{layer}/{table}/_metadata.json",
                    Body=json.dumps(metadata, indent=2),
                    ContentType='application/json'
                )
        
        logger.info("Data lake structure created", layers=list(structure.keys()))


def main():
    """Main function for testing S3 storage."""
    # Example usage
    storage = S3DataLakeStorage(
        endpoint_url="http://localhost:9000",  # MinIO
        access_key="minioadmin",
        secret_key="minioadmin",
        bucket_name="financial-data"
    )
    
    # Create data lake structure
    storage.create_data_lake_structure()
    
    # Example data
    sample_data = [
        {
            "symbol": "AAPL",
            "price": 150.25,
            "volume": 50000000,
            "timestamp": datetime.utcnow().isoformat()
        },
        {
            "symbol": "MSFT", 
            "price": 300.50,
            "volume": 30000000,
            "timestamp": datetime.utcnow().isoformat()
        }
    ]
    
    # Upload data
    s3_path = storage.upload_parquet_data(
        data=sample_data,
        table_name="stock_quotes",
        partition_columns=["date"]
    )
    
    print(f"Data uploaded to: {s3_path}")
    
    # List tables
    tables = storage.list_tables()
    print(f"Tables: {tables}")
    
    # Get table info
    for table in tables:
        info = storage.get_table_info(table)
        print(f"Table {table}: {info}")


if __name__ == "__main__":
    main()
