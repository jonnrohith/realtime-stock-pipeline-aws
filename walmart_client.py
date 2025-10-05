"""
Walmart RapidAPI client for data sourcing.
"""
import asyncio
import time
from typing import List, Optional, Dict, Any, Union
from urllib.parse import urlparse, parse_qs

import requests
import structlog
from pydantic import ValidationError

from api_client import BaseAPIClient, APIError, RateLimitError, AuthenticationError
from models import WalmartProduct, WalmartCategory, WalmartSearchResult, APIResponse
from config import api_config

logger = structlog.get_logger()


class WalmartAPIClient(BaseAPIClient):
    """Walmart RapidAPI client for product data extraction."""
    
    def __init__(self, rapidapi_key: str):
        super().__init__(
            base_url=api_config.walmart_api_base_url,
            api_key=rapidapi_key,
            api_secret=None
        )
        self.rapidapi_host = api_config.rapidapi_host
    
    def _get_headers(self) -> Dict[str, str]:
        """Get RapidAPI specific headers."""
        return {
            'x-rapidapi-key': self.api_key,
            'x-rapidapi-host': self.rapidapi_host,
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'User-Agent': 'WalmartDataSourcingClient/1.0'
        }
    
    def test_connection(self) -> bool:
        """Test API connection with a simple request."""
        try:
            # Test with a simple category request
            response = self.get_category_data("https://www.walmart.com/browse/electronics")
            return response.success
        except Exception as e:
            logger.error("Connection test failed", error=str(e))
            return False
    
    def get_category_data(self, category_url: str) -> APIResponse:
        """
        Get product data from a Walmart category URL.
        
        Args:
            category_url: Walmart category URL (e.g., "https://www.walmart.com/browse/cell-phones/phone-cases/1105910_133161_1997952")
        
        Returns:
            APIResponse containing category and product data
        """
        start_time = time.time()
        
        try:
            logger.info("Fetching category data", category_url=category_url)
            
            response = self.get(
                endpoint="/walmart-serp.php",
                params={"url": category_url}
            )
            
            response_time = time.time() - start_time
            
            if response.status_code == 200:
                data = response.json()
                logger.info("Category data fetched successfully", 
                           category_url=category_url, 
                           response_time=response_time)
                
                return APIResponse(
                    success=True,
                    data=data,
                    status_code=response.status_code,
                    response_time=response_time,
                    endpoint="/walmart-serp.php",
                    method="GET"
                )
            else:
                logger.error("Failed to fetch category data", 
                           status_code=response.status_code,
                           category_url=category_url)
                return APIResponse(
                    success=False,
                    error=f"HTTP {response.status_code}: {response.text}",
                    status_code=response.status_code,
                    response_time=response_time,
                    endpoint="/walmart-serp.php",
                    method="GET"
                )
                
        except Exception as e:
            response_time = time.time() - start_time
            logger.error("Error fetching category data", 
                        error=str(e), 
                        category_url=category_url)
            return APIResponse(
                success=False,
                error=str(e),
                response_time=response_time,
                endpoint="/walmart-serp.php",
                method="GET"
            )
    
    def search_products(self, query: str, **kwargs) -> APIResponse:
        """
        Search for products using Walmart search API.
        
        Args:
            query: Search query string
            **kwargs: Additional search parameters
        
        Returns:
            APIResponse containing search results
        """
        start_time = time.time()
        
        try:
            logger.info("Searching products", query=query)
            
            # Use the search URL format for the API
            search_url = f"https://www.walmart.com/search?q={query.replace(' ', '+')}"
            response = self.get(
                endpoint="/walmart-serp.php",
                params={"url": search_url}
            )
            
            response_time = time.time() - start_time
            
            if response.status_code == 200:
                data = response.json()
                logger.info("Product search completed", 
                           query=query, 
                           response_time=response_time)
                
                return APIResponse(
                    success=True,
                    data=data,
                    status_code=response.status_code,
                    response_time=response_time,
                    endpoint="/walmart-serp.php",
                    method="GET"
                )
            else:
                logger.error("Product search failed", 
                           status_code=response.status_code,
                           query=query)
                return APIResponse(
                    success=False,
                    error=f"HTTP {response.status_code}: {response.text}",
                    status_code=response.status_code,
                    response_time=response_time,
                    endpoint="/walmart-serp.php",
                    method="GET"
                )
                
        except Exception as e:
            response_time = time.time() - start_time
            logger.error("Error searching products", 
                        error=str(e), 
                        query=query)
            return APIResponse(
                success=False,
                error=str(e),
                response_time=response_time,
                endpoint="/walmart-serp.php",
                method="GET"
            )
    
    def get_product_details(self, product_url: str) -> APIResponse:
        """
        Get detailed product information.
        
        Args:
            product_url: Walmart product URL
        
        Returns:
            APIResponse containing product details
        """
        start_time = time.time()
        
        try:
            logger.info("Fetching product details", product_url=product_url)
            
            response = self.get(
                endpoint="/walmart-product.php",
                params={"url": product_url}
            )
            
            response_time = time.time() - start_time
            
            if response.status_code == 200:
                data = response.json()
                logger.info("Product details fetched successfully", 
                           product_url=product_url, 
                           response_time=response_time)
                
                return APIResponse(
                    success=True,
                    data=data,
                    status_code=response.status_code,
                    response_time=response_time,
                    endpoint="/walmart-product.php",
                    method="GET"
                )
            else:
                logger.error("Failed to fetch product details", 
                           status_code=response.status_code,
                           product_url=product_url)
                return APIResponse(
                    success=False,
                    error=f"HTTP {response.status_code}: {response.text}",
                    status_code=response.status_code,
                    response_time=response_time,
                    endpoint="/walmart-product.php",
                    method="GET"
                )
                
        except Exception as e:
            response_time = time.time() - start_time
            logger.error("Error fetching product details", 
                        error=str(e), 
                        product_url=product_url)
            return APIResponse(
                success=False,
                error=str(e),
                response_time=response_time,
                endpoint="/walmart-product.php",
                method="GET"
            )


class WalmartDataParser:
    """Parser for Walmart API responses."""
    
    @staticmethod
    def parse_category_response(response_data: Dict[str, Any]) -> WalmartCategory:
        """Parse category API response into WalmartCategory model."""
        try:
            # Extract category information
            category_data = {
                "name": response_data.get("category_name"),
                "url": response_data.get("category_url"),
                "product_count": response_data.get("total_products", 0)
            }
            
            # Parse products
            products = []
            if "products" in response_data:
                for product_data in response_data["products"]:
                    try:
                        product = WalmartDataParser.parse_product_data(product_data)
                        products.append(product)
                    except ValidationError as e:
                        logger.warning("Failed to parse product", error=str(e), product_data=product_data)
                        continue
            
            category = WalmartCategory(
                **category_data,
                products=products
            )
            
            return category
            
        except Exception as e:
            logger.error("Failed to parse category response", error=str(e), response_data=response_data)
            raise
    
    @staticmethod
    def parse_product_data(product_data: Dict[str, Any]) -> WalmartProduct:
        """Parse product data into WalmartProduct model."""
        try:
            # Map API response fields to our model
            parsed_data = {
                "product_id": product_data.get("id"),
                "name": product_data.get("name") or product_data.get("title"),
                "brand": product_data.get("brand"),
                "model": product_data.get("model"),
                "sku": product_data.get("sku"),
                "price": WalmartDataParser._parse_price(product_data.get("price")),
                "original_price": WalmartDataParser._parse_price(product_data.get("original_price")),
                "description": product_data.get("description"),
                "short_description": product_data.get("short_description"),
                "category": product_data.get("category"),
                "subcategory": product_data.get("subcategory"),
                "in_stock": product_data.get("in_stock"),
                "stock_quantity": product_data.get("stock_quantity"),
                "availability_status": product_data.get("availability_status"),
                "image_url": product_data.get("image_url") or product_data.get("image"),
                "image_urls": product_data.get("image_urls", []),
                "rating": product_data.get("rating"),
                "review_count": product_data.get("review_count"),
                "specifications": product_data.get("specifications", {}),
                "features": product_data.get("features", []),
                "product_url": product_data.get("product_url") or product_data.get("url"),
                "buy_url": product_data.get("buy_url")
            }
            
            # Remove None values
            parsed_data = {k: v for k, v in parsed_data.items() if v is not None}
            
            return WalmartProduct(**parsed_data)
            
        except Exception as e:
            logger.error("Failed to parse product data", error=str(e), product_data=product_data)
            raise
    
    @staticmethod
    def _parse_price(price_str: Union[str, float, int, None]) -> Optional[float]:
        """Parse price string to float."""
        if price_str is None:
            return None
        
        try:
            if isinstance(price_str, (int, float)):
                return float(price_str)
            
            # Remove currency symbols and clean the string
            price_str = str(price_str).replace("$", "").replace(",", "").strip()
            
            if not price_str:
                return None
                
            return float(price_str)
        except (ValueError, TypeError):
            logger.warning("Failed to parse price", price_str=price_str)
            return None
    
    @staticmethod
    def parse_search_response(response_data: Dict[str, Any], query: str) -> WalmartSearchResult:
        """Parse search API response into WalmartSearchResult model."""
        try:
            products = []
            if "products" in response_data:
                for product_data in response_data["products"]:
                    try:
                        product = WalmartDataParser.parse_product_data(product_data)
                        products.append(product)
                    except ValidationError as e:
                        logger.warning("Failed to parse search product", error=str(e), product_data=product_data)
                        continue
            
            search_result = WalmartSearchResult(
                query=query,
                total_results=response_data.get("total_results", len(products)),
                page=response_data.get("page", 1),
                page_size=response_data.get("page_size", len(products)),
                products=products,
                search_time=response_data.get("search_time")
            )
            
            return search_result
            
        except Exception as e:
            logger.error("Failed to parse search response", error=str(e), response_data=response_data)
            raise
