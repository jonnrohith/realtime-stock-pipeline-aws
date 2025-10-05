"""
Data extraction and transformation logic for Walmart product data.
"""
import asyncio
import json
import time
from datetime import datetime
from typing import List, Dict, Any, Optional, Union
from pathlib import Path

import pandas as pd
import structlog

from walmart_client import WalmartAPIClient, WalmartDataParser
from models import WalmartProduct, WalmartCategory, WalmartSearchResult, DataExtractionJob
from config import api_config

logger = structlog.get_logger()


class WalmartDataExtractor:
    """Main class for extracting and transforming Walmart data."""
    
    def __init__(self, rapidapi_key: str):
        self.client = WalmartAPIClient(rapidapi_key)
        self.parser = WalmartDataParser()
        self.jobs: Dict[str, DataExtractionJob] = {}
    
    def extract_category_data(
        self, 
        category_url: str, 
        job_id: Optional[str] = None,
        save_to_file: bool = True,
        output_format: str = "json"
    ) -> DataExtractionJob:
        """
        Extract all product data from a Walmart category.
        
        Args:
            category_url: Walmart category URL
            job_id: Optional job ID for tracking
            save_to_file: Whether to save results to file
            output_format: Output format ('json', 'csv', 'parquet')
        
        Returns:
            DataExtractionJob with extraction results
        """
        if job_id is None:
            job_id = f"category_{int(time.time())}"
        
        job = DataExtractionJob(
            job_id=job_id,
            job_type="category",
            status="running",
            parameters={"category_url": category_url, "output_format": output_format},
            started_at=datetime.utcnow()
        )
        self.jobs[job_id] = job
        
        try:
            logger.info("Starting category data extraction", job_id=job_id, category_url=category_url)
            
            # Fetch category data from API
            api_response = self.client.get_category_data(category_url)
            
            if not api_response.success:
                job.status = "failed"
                job.error_message = api_response.error
                job.completed_at = datetime.utcnow()
                logger.error("Category extraction failed", job_id=job_id, error=api_response.error)
                return job
            
            # Parse the response
            category_data = self.parser.parse_category_response(api_response.data)
            job.total_items = len(category_data.products)
            job.processed_items = len(category_data.products)
            
            # Save to file if requested
            if save_to_file:
                self._save_category_data(category_data, job_id, output_format)
            
            job.status = "completed"
            job.completed_at = datetime.utcnow()
            job.duration_seconds = (job.completed_at - job.started_at).total_seconds()
            
            logger.info("Category extraction completed", 
                       job_id=job_id, 
                       total_items=job.total_items,
                       duration=job.duration_seconds)
            
            return job
            
        except Exception as e:
            job.status = "failed"
            job.error_message = str(e)
            job.completed_at = datetime.utcnow()
            job.duration_seconds = (job.completed_at - job.started_at).total_seconds()
            
            logger.error("Category extraction failed with exception", 
                        job_id=job_id, 
                        error=str(e))
            return job
    
    def extract_search_results(
        self,
        query: str,
        job_id: Optional[str] = None,
        save_to_file: bool = True,
        output_format: str = "json",
        **search_params
    ) -> DataExtractionJob:
        """
        Extract product data from search results.
        
        Args:
            query: Search query
            job_id: Optional job ID for tracking
            save_to_file: Whether to save results to file
            output_format: Output format ('json', 'csv', 'parquet')
            **search_params: Additional search parameters
        
        Returns:
            DataExtractionJob with extraction results
        """
        if job_id is None:
            job_id = f"search_{int(time.time())}"
        
        job = DataExtractionJob(
            job_id=job_id,
            job_type="search",
            status="running",
            parameters={"query": query, "output_format": output_format, **search_params},
            started_at=datetime.utcnow()
        )
        self.jobs[job_id] = job
        
        try:
            logger.info("Starting search data extraction", job_id=job_id, query=query)
            
            # Fetch search results from API
            api_response = self.client.search_products(query, **search_params)
            
            if not api_response.success:
                job.status = "failed"
                job.error_message = api_response.error
                job.completed_at = datetime.utcnow()
                logger.error("Search extraction failed", job_id=job_id, error=api_response.error)
                return job
            
            # Parse the response
            search_results = self.parser.parse_search_response(api_response.data, query)
            job.total_items = len(search_results.products)
            job.processed_items = len(search_results.products)
            
            # Save to file if requested
            if save_to_file:
                self._save_search_results(search_results, job_id, output_format)
            
            job.status = "completed"
            job.completed_at = datetime.utcnow()
            job.duration_seconds = (job.completed_at - job.started_at).total_seconds()
            
            logger.info("Search extraction completed", 
                       job_id=job_id, 
                       total_items=job.total_items,
                       duration=job.duration_seconds)
            
            return job
            
        except Exception as e:
            job.status = "failed"
            job.error_message = str(e)
            job.completed_at = datetime.utcnow()
            job.duration_seconds = (job.completed_at - job.started_at).total_seconds()
            
            logger.error("Search extraction failed with exception", 
                        job_id=job_id, 
                        error=str(e))
            return job
    
    def extract_multiple_categories(
        self,
        category_urls: List[str],
        job_id: Optional[str] = None,
        save_to_file: bool = True,
        output_format: str = "json",
        delay_between_requests: float = 1.0
    ) -> DataExtractionJob:
        """
        Extract data from multiple categories sequentially.
        
        Args:
            category_urls: List of Walmart category URLs
            job_id: Optional job ID for tracking
            save_to_file: Whether to save results to file
            output_format: Output format ('json', 'csv', 'parquet')
            delay_between_requests: Delay between requests in seconds
        
        Returns:
            DataExtractionJob with extraction results
        """
        if job_id is None:
            job_id = f"multi_category_{int(time.time())}"
        
        job = DataExtractionJob(
            job_id=job_id,
            job_type="multi_category",
            status="running",
            parameters={
                "category_urls": category_urls, 
                "output_format": output_format,
                "delay_between_requests": delay_between_requests
            },
            started_at=datetime.utcnow()
        )
        self.jobs[job_id] = job
        
        all_products = []
        all_categories = []
        
        try:
            logger.info("Starting multi-category extraction", 
                       job_id=job_id, 
                       category_count=len(category_urls))
            
            for i, category_url in enumerate(category_urls):
                logger.info("Processing category", 
                           job_id=job_id, 
                           category_index=i+1, 
                           total_categories=len(category_urls),
                           category_url=category_url)
                
                # Extract single category
                category_job = self.extract_category_data(
                    category_url=category_url,
                    job_id=f"{job_id}_category_{i}",
                    save_to_file=False  # We'll save all together
                )
                
                if category_job.status == "completed":
                    # Get the parsed data from the API response
                    api_response = self.client.get_category_data(category_url)
                    if api_response.success:
                        category_data = self.parser.parse_category_response(api_response.data)
                        all_categories.append(category_data)
                        all_products.extend(category_data.products)
                        job.processed_items += len(category_data.products)
                else:
                    job.failed_items += 1
                    logger.warning("Category extraction failed", 
                                 job_id=job_id, 
                                 category_url=category_url,
                                 error=category_job.error_message)
                
                # Add delay between requests
                if i < len(category_urls) - 1:
                    time.sleep(delay_between_requests)
            
            job.total_items = len(all_products)
            
            # Save combined results
            if save_to_file and all_products:
                self._save_combined_data(all_products, all_categories, job_id, output_format)
            
            job.status = "completed"
            job.completed_at = datetime.utcnow()
            job.duration_seconds = (job.completed_at - job.started_at).total_seconds()
            
            logger.info("Multi-category extraction completed", 
                       job_id=job_id, 
                       total_items=job.total_items,
                       processed_items=job.processed_items,
                       failed_items=job.failed_items,
                       duration=job.duration_seconds)
            
            return job
            
        except Exception as e:
            job.status = "failed"
            job.error_message = str(e)
            job.completed_at = datetime.utcnow()
            job.duration_seconds = (job.completed_at - job.started_at).total_seconds()
            
            logger.error("Multi-category extraction failed with exception", 
                        job_id=job_id, 
                        error=str(e))
            return job
    
    def _save_category_data(self, category_data: WalmartCategory, job_id: str, output_format: str):
        """Save category data to file."""
        output_dir = Path("output")
        output_dir.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        if output_format == "json":
            file_path = output_dir / f"category_{job_id}_{timestamp}.json"
            with open(file_path, 'w') as f:
                json.dump(category_data.dict(), f, indent=2, default=str)
        
        elif output_format == "csv":
            file_path = output_dir / f"category_{job_id}_{timestamp}.csv"
            df = pd.DataFrame([product.dict() for product in category_data.products])
            df.to_csv(file_path, index=False)
        
        elif output_format == "parquet":
            file_path = output_dir / f"category_{job_id}_{timestamp}.parquet"
            df = pd.DataFrame([product.dict() for product in category_data.products])
            df.to_parquet(file_path, index=False)
        
        logger.info("Category data saved", job_id=job_id, file_path=str(file_path))
    
    def _save_search_results(self, search_results: WalmartSearchResult, job_id: str, output_format: str):
        """Save search results to file."""
        output_dir = Path("output")
        output_dir.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        if output_format == "json":
            file_path = output_dir / f"search_{job_id}_{timestamp}.json"
            with open(file_path, 'w') as f:
                json.dump(search_results.dict(), f, indent=2, default=str)
        
        elif output_format == "csv":
            file_path = output_dir / f"search_{job_id}_{timestamp}.csv"
            df = pd.DataFrame([product.dict() for product in search_results.products])
            df.to_csv(file_path, index=False)
        
        elif output_format == "parquet":
            file_path = output_dir / f"search_{job_id}_{timestamp}.parquet"
            df = pd.DataFrame([product.dict() for product in search_results.products])
            df.to_parquet(file_path, index=False)
        
        logger.info("Search results saved", job_id=job_id, file_path=str(file_path))
    
    def _save_combined_data(self, products: List[WalmartProduct], categories: List[WalmartCategory], job_id: str, output_format: str):
        """Save combined data from multiple categories."""
        output_dir = Path("output")
        output_dir.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        if output_format == "json":
            file_path = output_dir / f"combined_{job_id}_{timestamp}.json"
            data = {
                "categories": [cat.dict() for cat in categories],
                "products": [product.dict() for product in products],
                "summary": {
                    "total_categories": len(categories),
                    "total_products": len(products),
                    "extraction_timestamp": timestamp
                }
            }
            with open(file_path, 'w') as f:
                json.dump(data, f, indent=2, default=str)
        
        elif output_format == "csv":
            file_path = output_dir / f"combined_{job_id}_{timestamp}.csv"
            df = pd.DataFrame([product.dict() for product in products])
            df.to_csv(file_path, index=False)
        
        elif output_format == "parquet":
            file_path = output_dir / f"combined_{job_id}_{timestamp}.parquet"
            df = pd.DataFrame([product.dict() for product in products])
            df.to_parquet(file_path, index=False)
        
        logger.info("Combined data saved", job_id=job_id, file_path=str(file_path))
    
    def get_job_status(self, job_id: str) -> Optional[DataExtractionJob]:
        """Get the status of an extraction job."""
        return self.jobs.get(job_id)
    
    def list_jobs(self) -> List[DataExtractionJob]:
        """List all extraction jobs."""
        return list(self.jobs.values())
