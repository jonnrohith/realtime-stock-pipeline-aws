"""
Base API client for data sourcing with retry logic, rate limiting, and error handling.
"""
import asyncio
import time
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union
from urllib.parse import urljoin

import aiohttp
import requests
import structlog
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from config import api_config

logger = structlog.get_logger()


class APIError(Exception):
    """Base exception for API-related errors."""
    pass


class RateLimitError(APIError):
    """Exception raised when rate limit is exceeded."""
    pass


class AuthenticationError(APIError):
    """Exception raised when authentication fails."""
    pass


class BaseAPIClient(ABC):
    """Base class for API clients with common functionality."""
    
    def __init__(self, base_url: str, api_key: str, api_secret: Optional[str] = None):
        self.base_url = base_url.rstrip('/')
        self.api_key = api_key
        self.api_secret = api_secret
        self.session = None
        self.rate_limiter = RateLimiter(
            requests_per_minute=api_config.rate_limit_requests_per_minute,
            burst=api_config.rate_limit_burst
        )
        
    def _get_headers(self) -> Dict[str, str]:
        """Get default headers for API requests."""
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'User-Agent': 'DataSourcingClient/1.0'
        }
        
        if self.api_key:
            headers['Authorization'] = f'Bearer {self.api_key}'
            
        if self.api_secret:
            headers['X-API-Secret'] = self.api_secret
            
        return headers
    
    def _build_url(self, endpoint: str) -> str:
        """Build full URL from endpoint."""
        return urljoin(self.base_url + '/', endpoint.lstrip('/'))
    
    @retry(
        stop=stop_after_attempt(api_config.max_retries),
        wait=wait_exponential(multiplier=api_config.retry_delay),
        retry=retry_if_exception_type((requests.exceptions.RequestException, RateLimitError))
    )
    def _make_request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None
    ) -> requests.Response:
        """Make HTTP request with retry logic and rate limiting."""
        url = self._build_url(endpoint)
        request_headers = {**self._get_headers(), **(headers or {})}
        
        # Rate limiting
        self.rate_limiter.wait_if_needed()
        
        logger.info("Making API request", method=method, url=url, params=params)
        
        try:
            response = requests.request(
                method=method,
                url=url,
                params=params,
                json=data,
                headers=request_headers,
                timeout=30
            )
            
            # Handle rate limiting
            if response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', 60))
                logger.warning("Rate limit exceeded, waiting", retry_after=retry_after)
                time.sleep(retry_after)
                raise RateLimitError("Rate limit exceeded")
            
            # Handle authentication errors
            if response.status_code == 401:
                raise AuthenticationError("Authentication failed")
            
            # Handle other HTTP errors
            response.raise_for_status()
            
            logger.info("API request successful", status_code=response.status_code)
            return response
            
        except requests.exceptions.RequestException as e:
            logger.error("API request failed", error=str(e), url=url)
            raise APIError(f"Request failed: {e}") from e
    
    async def _make_async_request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None
    ) -> aiohttp.ClientResponse:
        """Make async HTTP request with retry logic and rate limiting."""
        url = self._build_url(endpoint)
        request_headers = {**self._get_headers(), **(headers or {})}
        
        # Rate limiting
        await self.rate_limiter.async_wait_if_needed()
        
        logger.info("Making async API request", method=method, url=url, params=params)
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.request(
                    method=method,
                    url=url,
                    params=params,
                    json=data,
                    headers=request_headers,
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as response:
                    # Handle rate limiting
                    if response.status == 429:
                        retry_after = int(response.headers.get('Retry-After', 60))
                        logger.warning("Rate limit exceeded, waiting", retry_after=retry_after)
                        await asyncio.sleep(retry_after)
                        raise RateLimitError("Rate limit exceeded")
                    
                    # Handle authentication errors
                    if response.status == 401:
                        raise AuthenticationError("Authentication failed")
                    
                    # Handle other HTTP errors
                    if response.status >= 400:
                        raise aiohttp.ClientResponseError(
                            request_info=response.request_info,
                            history=response.history,
                            status=response.status,
                            message=f"HTTP {response.status}"
                        )
                    
                    logger.info("Async API request successful", status_code=response.status)
                    return response
                    
        except aiohttp.ClientError as e:
            logger.error("Async API request failed", error=str(e), url=url)
            raise APIError(f"Async request failed: {e}") from e
    
    def get(self, endpoint: str, params: Optional[Dict[str, Any]] = None, **kwargs) -> requests.Response:
        """Make GET request."""
        return self._make_request('GET', endpoint, params=params, **kwargs)
    
    def post(self, endpoint: str, data: Optional[Dict[str, Any]] = None, **kwargs) -> requests.Response:
        """Make POST request."""
        return self._make_request('POST', endpoint, data=data, **kwargs)
    
    def put(self, endpoint: str, data: Optional[Dict[str, Any]] = None, **kwargs) -> requests.Response:
        """Make PUT request."""
        return self._make_request('PUT', endpoint, data=data, **kwargs)
    
    def delete(self, endpoint: str, **kwargs) -> requests.Response:
        """Make DELETE request."""
        return self._make_request('DELETE', endpoint, **kwargs)
    
    async def async_get(self, endpoint: str, params: Optional[Dict[str, Any]] = None, **kwargs) -> aiohttp.ClientResponse:
        """Make async GET request."""
        return await self._make_async_request('GET', endpoint, params=params, **kwargs)
    
    async def async_post(self, endpoint: str, data: Optional[Dict[str, Any]] = None, **kwargs) -> aiohttp.ClientResponse:
        """Make async POST request."""
        return await self._make_async_request('POST', endpoint, data=data, **kwargs)
    
    @abstractmethod
    def test_connection(self) -> bool:
        """Test API connection."""
        pass


class RateLimiter:
    """Rate limiter to control API request frequency."""
    
    def __init__(self, requests_per_minute: int, burst: int = 10):
        self.requests_per_minute = requests_per_minute
        self.burst = burst
        self.requests = []
        self.lock = asyncio.Lock()
    
    def wait_if_needed(self):
        """Wait if necessary to respect rate limits."""
        now = time.time()
        minute_ago = now - 60
        
        # Remove old requests
        self.requests = [req_time for req_time in self.requests if req_time > minute_ago]
        
        # Check if we need to wait
        if len(self.requests) >= self.requests_per_minute:
            sleep_time = 60 - (now - self.requests[0])
            if sleep_time > 0:
                logger.info("Rate limiting: waiting", sleep_time=sleep_time)
                time.sleep(sleep_time)
                # Remove the oldest request after waiting
                self.requests.pop(0)
        
        # Add current request
        self.requests.append(now)
    
    async def async_wait_if_needed(self):
        """Async version of wait_if_needed."""
        async with self.lock:
            now = time.time()
            minute_ago = now - 60
            
            # Remove old requests
            self.requests = [req_time for req_time in self.requests if req_time > minute_ago]
            
            # Check if we need to wait
            if len(self.requests) >= self.requests_per_minute:
                sleep_time = 60 - (now - self.requests[0])
                if sleep_time > 0:
                    logger.info("Rate limiting: waiting", sleep_time=sleep_time)
                    await asyncio.sleep(sleep_time)
                    # Remove the oldest request after waiting
                    self.requests.pop(0)
            
            # Add current request
            self.requests.append(now)
