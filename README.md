# Walmart API Data Sourcing System

A comprehensive Python-based data engineering solution for sourcing product data from Walmart using the RapidAPI platform. This system provides robust data extraction, transformation, and storage capabilities with built-in error handling, rate limiting, and job monitoring.

## Features

- **API Integration**: Seamless integration with Walmart RapidAPI
- **Data Models**: Structured data models using Pydantic for type safety
- **Rate Limiting**: Built-in rate limiting to respect API limits
- **Error Handling**: Comprehensive error handling with retry logic
- **Job Monitoring**: Track extraction jobs with detailed status reporting
- **Multiple Output Formats**: Support for JSON, CSV, and Parquet output formats
- **Async Support**: Asynchronous API calls for better performance
- **Logging**: Structured logging with configurable levels
- **Data Validation**: Automatic data validation and parsing

## Installation

1. Clone or download this repository
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Set up environment variables:
   ```bash
   cp env.example .env
   ```
   
4. Edit `.env` file with your RapidAPI credentials:
   ```env
   WALMART_API_BASE_URL=https://walmart-data.p.rapidapi.com
   RAPIDAPI_KEY=your_rapidapi_key_here
   RAPIDAPI_HOST=walmart-data.p.rapidapi.com
   ```

## Quick Start

### Basic Usage

```python
from walmart_client import WalmartAPIClient

# Initialize client
client = WalmartAPIClient("your_rapidapi_key")

# Test connection
if client.test_connection():
    print("API connection successful!")

# Get category data
category_url = "https://www.walmart.com/browse/cell-phones/phone-cases/1105910_133161_1997952"
response = client.get_category_data(category_url)

if response.success:
    print(f"Fetched {len(response.data.get('products', []))} products")
```

### Data Extraction

```python
from data_extractor import WalmartDataExtractor

# Initialize extractor
extractor = WalmartDataExtractor("your_rapidapi_key")

# Extract from single category
job = extractor.extract_category_data(
    category_url="https://www.walmart.com/browse/cell-phones/phone-cases/1105910_133161_1997952",
    job_id="phone_cases_extraction",
    save_to_file=True,
    output_format="csv"
)

print(f"Job status: {job.status}")
print(f"Total items: {job.total_items}")
```

### Multiple Categories

```python
# Extract from multiple categories
category_urls = [
    "https://www.walmart.com/browse/cell-phones/phone-cases/1105910_133161_1997952",
    "https://www.walmart.com/browse/electronics/laptops/3944_3951_1089430",
    "https://www.walmart.com/browse/home/home-decor/4044_1076885"
]

job = extractor.extract_multiple_categories(
    category_urls=category_urls,
    job_id="multi_category_extraction",
    save_to_file=True,
    output_format="json",
    delay_between_requests=2.0
)
```

## API Reference

### WalmartAPIClient

Main client for interacting with the Walmart RapidAPI.

#### Methods

- `test_connection()`: Test API connectivity
- `get_category_data(category_url)`: Get products from a category
- `search_products(query, **kwargs)`: Search for products
- `get_product_details(product_url)`: Get detailed product information

### WalmartDataExtractor

High-level data extraction and transformation.

#### Methods

- `extract_category_data(category_url, job_id, save_to_file, output_format)`: Extract from single category
- `extract_search_results(query, job_id, save_to_file, output_format, **search_params)`: Extract search results
- `extract_multiple_categories(category_urls, job_id, save_to_file, output_format, delay_between_requests)`: Extract from multiple categories
- `get_job_status(job_id)`: Get job status
- `list_jobs()`: List all jobs

### Data Models

#### WalmartProduct
Structured model for product data including:
- Basic info (name, brand, model, SKU)
- Pricing (price, original_price, currency)
- Availability (in_stock, stock_quantity)
- Images and specifications
- Ratings and reviews

#### WalmartCategory
Model for category data including:
- Category information
- Subcategories
- Products in the category

#### DataExtractionJob
Model for tracking extraction jobs including:
- Job status and progress
- Error handling
- Timing information

## Configuration

The system uses environment variables for configuration. See `env.example` for all available options:

- `WALMART_API_BASE_URL`: Walmart API base URL
- `RAPIDAPI_KEY`: Your RapidAPI key
- `RAPIDAPI_HOST`: RapidAPI host
- `RATE_LIMIT_REQUESTS_PER_MINUTE`: Rate limiting (default: 60)
- `MAX_RETRIES`: Maximum retry attempts (default: 3)
- `LOG_LEVEL`: Logging level (default: INFO)

## Output Formats

The system supports multiple output formats:

- **JSON**: Human-readable JSON format
- **CSV**: Comma-separated values for spreadsheet applications
- **Parquet**: Efficient columnar format for data analysis

Output files are saved in the `output/` directory with timestamps.

## Error Handling

The system includes comprehensive error handling:

- **Rate Limiting**: Automatic handling of rate limit errors
- **Retry Logic**: Exponential backoff for failed requests
- **Validation**: Data validation using Pydantic models
- **Logging**: Detailed logging for debugging

## Examples

Run the example script to see the system in action:

```bash
python example_usage.py
```

This will demonstrate:
- Basic API usage
- Single category extraction
- Multiple category extraction
- Search functionality
- Job monitoring

## Rate Limiting

The system respects API rate limits by:
- Tracking request timestamps
- Implementing delays between requests
- Handling 429 (Too Many Requests) responses
- Configurable rate limits

## Logging

Structured logging is used throughout the system:

```python
import structlog
logger = structlog.get_logger()
logger.info("Operation completed", status="success", items_processed=100)
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

This project is licensed under the MIT License.

## Support

For issues and questions:
1. Check the logs for error details
2. Verify your RapidAPI key and configuration
3. Check API rate limits and quotas
4. Review the example usage patterns

## Cloud Deployment

See `AWS_DEPLOYMENT_GUIDE.md` for detailed steps.

Quick start with Terraform (showcase-ready):

```bash
./deploy-to-aws.sh  # set AUTO_APPLY=true to auto-apply
```

CI: GitHub Actions runs Terraform plan via `.github/workflows/terraform.yml` using AWS OIDC (set secret `AWS_TERRAFORM_ROLE_ARN`).

## Changelog

### v1.0.0
- Initial release
- Basic API client implementation
- Data extraction and transformation
- Multiple output format support
- Job monitoring and tracking
- Comprehensive error handling
