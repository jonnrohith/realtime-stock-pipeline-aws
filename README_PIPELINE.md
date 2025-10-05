# Yahoo Finance Data Engineering Pipeline

A comprehensive data engineering pipeline for extracting, transforming, and processing financial data from Yahoo Finance API.

## 🏗️ Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Data Sources  │    │   Data Pipeline │    │   Data Storage  │
│                 │    │                 │    │                 │
│ • Market Tickers│───▶│ • Extractors    │───▶│ • Raw Data      │
│ • Stock Quotes  │    │ • Transformers  │    │ • Processed Data│
│ • Stock History │    │ • Validators    │    │ • Output Files  │
│ • Market Screen │    │ • Scheduler     │    │                 │
│ • Stock News    │    │ • Monitor       │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## 🚀 Features

### Data Extraction
- **Market Tickers**: Get lists of stocks, ETFs, crypto
- **Stock Quotes**: Real-time and historical price data
- **Stock History**: Daily, hourly, minute-level data
- **Market Screeners**: Gainers, losers, most active stocks
- **Stock News**: Latest news and market updates

### Data Processing
- **Data Cleaning**: Remove nulls, standardize formats
- **Data Enrichment**: Add categories, quality scores
- **Data Validation**: Quality checks and alerts
- **Data Aggregation**: Market summaries and statistics

### Pipeline Management
- **Scheduling**: Cron-based job scheduling
- **Monitoring**: Real-time alerts and metrics
- **Error Handling**: Retry logic and failure recovery
- **Data Retention**: Automatic cleanup of old data

## 📦 Installation

1. **Install Dependencies**:
```bash
pip install -r requirements.txt
```

2. **Set Environment Variables**:
```bash
cp env.example .env
# Edit .env with your RapidAPI key
```

3. **Create Data Directories**:
```bash
mkdir -p data/raw data/processed output
```

## 🔧 Configuration

### Pipeline Configuration
```python
from data_pipeline.config import pipeline_config

# Modify settings
pipeline_config.batch_size = 100
pipeline_config.requests_per_minute = 60
pipeline_config.enabled_sources = ["market_tickers", "stock_quotes"]
```

### Data Source Configuration
```python
from data_pipeline.config import data_source_config

# Configure symbols to track
data_source_config.quote_symbols = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]
data_source_config.history_symbols = ["AAPL", "MSFT", "GOOGL"]
data_source_config.news_symbols = ["AAPL", "MSFT", "GOOGL"]
```

## 🎯 Usage

### Basic Usage

```python
from data_pipeline.pipeline import DataPipeline

# Initialize pipeline
pipeline = DataPipeline()

# Run specific data source
result = pipeline.run_data_source("stock_quotes", symbols=["AAPL", "MSFT"])
print(f"Extracted {result['processed_items']} quotes")

# Run full pipeline
result = pipeline.run_full_pipeline()
print(f"Processed {result['data_sources_processed']} data sources")
```

### Scheduled Execution

```python
# Start scheduler
pipeline.start()

# The pipeline will now run on schedule:
# - Market Tickers: Daily at 6 AM
# - Stock Quotes: Every 5 minutes (9 AM - 4 PM, Mon-Fri)
# - Stock History: Daily at 7 PM
# - Market Screeners: Every 15 minutes (9 AM - 4 PM, Mon-Fri)
# - Stock News: Every 30 minutes

# Stop scheduler
pipeline.stop()
```

### Monitoring and Alerts

```python
# Get pipeline status
status = pipeline.get_status()
print(f"Recent jobs: {len(status['recent_jobs'])}")
print(f"Alerts: {status['alert_summary']['total_alerts']}")

# List recent jobs
jobs = pipeline.list_jobs()
for job in jobs:
    print(f"{job['job_id']}: {job['status']}")

# Get specific job status
job_status = pipeline.get_job_status("job_123")
```

## 📊 Data Models

### Stock
```python
{
    "symbol": "AAPL",
    "name": "Apple Inc.",
    "exchange": "NASDAQ",
    "sector": "Technology",
    "industry": "Consumer Electronics",
    "market_cap": 3000000000000,
    "currency": "USD"
}
```

### Stock Quote
```python
{
    "symbol": "AAPL",
    "price": 150.25,
    "change": 2.50,
    "change_percent": 1.69,
    "volume": 50000000,
    "market_cap": 3000000000000,
    "timestamp": "2024-01-01T16:00:00Z"
}
```

### Stock History
```python
{
    "symbol": "AAPL",
    "date": "2024-01-01",
    "open": 148.50,
    "high": 151.00,
    "low": 147.25,
    "close": 150.25,
    "volume": 50000000,
    "interval": "1d"
}
```

## 🔍 Data Quality

The pipeline includes comprehensive data quality checks:

### Completeness Checks
- Required fields validation
- Missing data detection
- Data coverage analysis

### Validity Checks
- Price validation (positive values)
- Volume validation (non-negative)
- Date format validation
- Symbol format validation

### Consistency Checks
- Duplicate detection
- Cross-field validation
- Data type consistency

## 📈 Monitoring

### Metrics Collected
- **Processing Metrics**: Records processed, processing time
- **Quality Metrics**: Data quality scores, validation results
- **System Metrics**: Memory usage, CPU usage, disk space
- **API Metrics**: Request count, error rates, response times

### Alerts Generated
- **Job Failures**: When extraction jobs fail
- **Data Quality Issues**: When data quality drops below thresholds
- **System Resource Issues**: High memory/CPU usage
- **API Issues**: High error rates or slow responses

## 🗂️ File Structure

```
data_pipeline/
├── __init__.py
├── config.py              # Configuration settings
├── models.py              # Data models
├── extractors.py          # Data extraction logic
├── transformers.py        # Data transformation logic
├── scheduler.py           # Job scheduling
├── monitoring.py          # Monitoring and alerting
└── pipeline.py            # Main orchestrator

data/
├── raw/                   # Raw extracted data
└── processed/             # Cleaned and transformed data

output/                    # Final output files
```

## 🚨 Error Handling

The pipeline includes robust error handling:

- **Retry Logic**: Automatic retries for failed API calls
- **Rate Limiting**: Respects API rate limits
- **Graceful Degradation**: Continues processing even if some sources fail
- **Error Logging**: Comprehensive error logging and alerting
- **Data Recovery**: Saves partial results on failures

## 🔧 Customization

### Adding New Data Sources

1. **Create Extractor**:
```python
class NewDataExtractor(BaseExtractor):
    def extract(self, **kwargs):
        # Implementation
        pass
```

2. **Add to Pipeline**:
```python
# In pipeline.py
self.extractors[DataSourceType.NEW_SOURCE] = NewDataExtractor(rapidapi_key)
```

3. **Update Configuration**:
```python
# In config.py
enabled_sources = ["market_tickers", "new_source"]
```

### Custom Transformations

```python
class CustomTransformer(BaseTransformer):
    def transform(self, data, transformation_type):
        # Custom transformation logic
        return transformed_data
```

## 📋 Examples

See `pipeline_example.py` for comprehensive examples including:
- Basic data extraction
- Scheduled execution
- Monitoring and alerting
- Error handling
- Data quality checks

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License.

## 🆘 Support

For questions or issues:
1. Check the documentation
2. Review the examples
3. Check the logs in `logs/` directory
4. Open an issue on GitHub

## 🔄 Version History

- **v1.0.0**: Initial release with basic extraction
- **v1.1.0**: Added scheduling and monitoring
- **v1.2.0**: Added data quality checks and alerting
- **v1.3.0**: Added comprehensive transformations and aggregations
