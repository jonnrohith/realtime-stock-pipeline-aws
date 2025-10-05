# Complete Financial Data Engineering Pipeline

A comprehensive, production-ready data engineering pipeline for real-time financial data processing using modern technologies.

## 🏗️ Architecture Overview

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Data Sources  │    │   Streaming     │    │   Processing    │    │   Storage       │
│                 │    │                 │    │                 │    │                 │
│ • Yahoo Finance │───▶│ • Kafka         │───▶│ • PySpark       │───▶│ • S3/MinIO      │
│ • Real-time API │    │ • Real-time     │    │ • Structured    │    │ • Data Lake     │
│ • Historical    │    │ • Streaming     │    │   Streaming     │    │ • Parquet       │
└─────────────────┘    └─────────────────┘    └─────────────────┘    └─────────────────┘
                                │                        │                        │
                                ▼                        ▼                        ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Orchestration │    │   Analytics     │    │   Monitoring    │    │   Visualization │
│                 │    │                 │    │                 │    │                 │
│ • Airflow       │    │ • dbt           │    │ • Grafana       │    │ • Streamlit     │
│ • DAGs          │    │ • Transformations│   │ • Prometheus    │    │ • Dashboards    │
│ • Scheduling    │    │ • Dimensional   │    │ • Alerts        │    │ • Real-time     │
└─────────────────┘    └─────────────────┘    └─────────────────┘    └─────────────────┘
```

## 🚀 Technology Stack

### Data Ingestion
- **Yahoo Finance API**: Real-time and historical financial data
- **Python**: Data extraction and API integration
- **RapidAPI**: API management and rate limiting

### Streaming
- **Apache Kafka**: Real-time data streaming
- **Kafka Producers**: Python-based data producers
- **Kafka Topics**: Organized data streams

### Processing
- **Apache Spark**: Distributed data processing
- **PySpark Structured Streaming**: Real-time stream processing
- **Data Transformations**: Cleaning, enrichment, aggregation

### Storage
- **S3/MinIO**: Data lake storage
- **Parquet**: Columnar storage format
- **PostgreSQL**: Relational database for analytics

### Analytics
- **dbt**: Data transformation and modeling
- **Dimensional Models**: Star schema design
- **Data Quality**: Validation and testing

### Orchestration
- **Apache Airflow**: Workflow orchestration
- **DAGs**: Directed Acyclic Graphs
- **Scheduling**: Automated pipeline execution

### Monitoring
- **Grafana**: Metrics visualization
- **Prometheus**: Metrics collection
- **Alerting**: Real-time notifications

### Visualization
- **Streamlit**: Interactive dashboards
- **Plotly**: Interactive charts
- **Real-time Updates**: Live data visualization

## 📁 Project Structure

```
financial-data-pipeline/
├── data_pipeline/                 # Core data pipeline components
│   ├── __init__.py
│   ├── config.py                  # Configuration management
│   ├── models.py                  # Data models
│   ├── extractors.py              # Data extraction
│   ├── transformers.py            # Data transformation
│   ├── scheduler.py               # Job scheduling
│   ├── monitoring.py              # Monitoring and alerting
│   └── pipeline.py                # Main orchestrator
├── streaming_pipeline/            # Real-time streaming
│   ├── kafka_producer.py          # Kafka data producer
│   └── pyspark_streaming.py       # PySpark streaming processing
├── data_lake/                     # Data lake storage
│   └── s3_storage.py              # S3/MinIO integration
├── dbt_models/                    # dbt transformations
│   ├── models/
│   │   ├── staging/               # Staging models
│   │   ├── marts/                 # Mart models
│   │   └── schema.yml             # Data schema
│   ├── dbt_project.yml            # dbt configuration
│   └── profiles.yml               # Database profiles
├── visualization/                 # Dashboards and visualization
│   └── dashboard.py               # Streamlit dashboard
├── orchestration/                 # Workflow orchestration
│   └── airflow_dag.py             # Airflow DAGs
├── monitoring/                    # Monitoring configuration
│   ├── grafana/                   # Grafana dashboards
│   └── prometheus/                # Prometheus config
├── docker-compose.yml             # Container orchestration
├── requirements.txt               # Python dependencies
└── README_COMPLETE_PIPELINE.md    # This file
```

## 🛠️ Setup and Installation

### Prerequisites
- Docker and Docker Compose
- Python 3.9+
- Git

### 1. Clone the Repository
```bash
git clone <repository-url>
cd financial-data-pipeline
```

### 2. Environment Setup
```bash
# Create environment file
cp .env.example .env

# Edit environment variables
nano .env
```

### 3. Start the Infrastructure
```bash
# Start all services
docker-compose up -d

# Check service status
docker-compose ps
```

### 4. Initialize Data Pipeline
```bash
# Install Python dependencies
pip install -r requirements.txt

# Run initial data extraction
python simple_pipeline_example.py
```

## 🔧 Configuration

### Environment Variables
```bash
# API Configuration
RAPIDAPI_KEY=your_rapidapi_key
YAHOO_API_BASE_URL=https://yahoo-finance15.p.rapidapi.com

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=localhost:9092

# Database Configuration
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=financial_data
POSTGRES_USER=postgres
POSTGRES_PASSWORD=password

# S3/MinIO Configuration
S3_ENDPOINT_URL=http://localhost:9000
S3_ACCESS_KEY=minioadmin
S3_SECRET_KEY=minioadmin
S3_BUCKET=financial-data

# Airflow Configuration
AIRFLOW_HOME=/opt/airflow
```

### Service URLs
- **Airflow**: http://localhost:8080 (admin/admin)
- **Streamlit Dashboard**: http://localhost:8501
- **Grafana**: http://localhost:3000 (admin/admin)
- **Jupyter**: http://localhost:8888
- **MinIO Console**: http://localhost:9001 (minioadmin/minioadmin)
- **Kafka UI**: http://localhost:8080 (via Airflow)

## 📊 Data Flow

### 1. Data Ingestion
```python
# Extract data from Yahoo Finance API
extractor = StockQuotesExtractor(rapidapi_key)
job = extractor.extract(symbols=["AAPL", "MSFT", "GOOGL"])
```

### 2. Real-time Streaming
```python
# Stream data to Kafka
producer = YahooFinanceKafkaProducer(rapidapi_key)
producer.produce_stock_quotes(symbols, interval=30)
```

### 3. Stream Processing
```python
# Process streams with PySpark
processor = FinancialDataStreamProcessor()
processor.start_all_streams()
```

### 4. Data Storage
```python
# Store in data lake
storage = S3DataLakeStorage()
storage.upload_parquet_data(data, "stock_quotes")
```

### 5. Analytics
```bash
# Run dbt transformations
dbt run --profiles-dir .
dbt test --profiles-dir .
```

## 🎯 Usage Examples

### Basic Data Extraction
```python
from data_pipeline.pipeline import DataPipeline

# Initialize pipeline
pipeline = DataPipeline()

# Extract specific data
result = pipeline.run_data_source("stock_quotes", symbols=["AAPL", "MSFT"])
print(f"Extracted {result['processed_items']} quotes")
```

### Real-time Streaming
```python
from streaming_pipeline.kafka_producer import YahooFinanceKafkaProducer

# Start streaming
producer = YahooFinanceKafkaProducer()
producer.produce_all_data(symbols=["AAPL", "MSFT", "GOOGL"])
```

### Data Transformation
```python
from data_pipeline.transformers import DataTransformer

# Transform data
transformer = DataTransformer()
result = transformer.transform_data(data, "stock_quotes")
```

### Dashboard Access
```bash
# Start Streamlit dashboard
streamlit run visualization/dashboard.py
```

## 📈 Monitoring and Alerting

### Metrics Collected
- **Data Quality**: Completeness, validity, consistency
- **Processing Performance**: Throughput, latency, error rates
- **System Resources**: CPU, memory, disk usage
- **API Usage**: Request counts, rate limits, errors

### Alerts Configured
- **Data Quality Issues**: Missing data, invalid values
- **Processing Failures**: Job failures, timeouts
- **System Issues**: High resource usage, disk space
- **API Issues**: Rate limiting, authentication errors

### Monitoring Dashboards
- **Pipeline Health**: Overall system status
- **Data Quality**: Quality metrics and trends
- **Performance**: Processing times and throughput
- **Business Metrics**: Market data insights

## 🔍 Data Quality

### Validation Rules
- **Completeness**: Required fields present
- **Validity**: Data types and ranges correct
- **Consistency**: Cross-field validation
- **Accuracy**: Business rule validation

### Quality Checks
- **Automated Testing**: dbt tests
- **Data Profiling**: Statistical analysis
- **Anomaly Detection**: Outlier identification
- **Trend Analysis**: Quality over time

## 🚀 Deployment

### Development
```bash
# Start development environment
docker-compose -f docker-compose.yml up -d

# Run tests
pytest tests/

# Run data quality checks
dbt test
```

### Production
```bash
# Deploy to production
docker-compose -f docker-compose.prod.yml up -d

# Monitor deployment
kubectl get pods -n financial-data
```

### Scaling
- **Horizontal Scaling**: Add more Kafka partitions
- **Vertical Scaling**: Increase container resources
- **Auto-scaling**: Kubernetes HPA configuration

## 🔧 Troubleshooting

### Common Issues
1. **API Rate Limiting**: Check RapidAPI quotas
2. **Kafka Connection**: Verify Kafka is running
3. **Database Connection**: Check PostgreSQL status
4. **S3 Access**: Verify MinIO credentials

### Debug Commands
```bash
# Check service logs
docker-compose logs kafka
docker-compose logs postgres
docker-compose logs airflow-webserver

# Test API connection
python -c "from data_pipeline.extractors import StockQuotesExtractor; print('API OK')"

# Test Kafka connection
python -c "from kafka import KafkaProducer; print('Kafka OK')"
```

## 📚 Documentation

### API Documentation
- **Yahoo Finance API**: [RapidAPI Documentation](https://rapidapi.com/hub)
- **Kafka API**: [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- **dbt Documentation**: [dbt Documentation](https://docs.getdbt.com/)

### Architecture Decisions
- **Why Kafka?**: High-throughput, fault-tolerant streaming
- **Why PySpark?**: Distributed processing, real-time capabilities
- **Why S3?**: Scalable, cost-effective storage
- **Why dbt?**: Data transformation, testing, documentation

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
1. Check the troubleshooting section
2. Review the logs
3. Open an issue on GitHub
4. Contact the data engineering team

## 🔄 Version History

- **v1.0.0**: Initial release with basic pipeline
- **v1.1.0**: Added real-time streaming
- **v1.2.0**: Added monitoring and alerting
- **v1.3.0**: Added dbt transformations
- **v1.4.0**: Added visualization dashboard
- **v1.5.0**: Added container orchestration

---

**Built with ❤️ by the Data Engineering Team**

