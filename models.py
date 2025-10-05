"""
Data models for Walmart product data.
"""
from datetime import datetime
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field


class WalmartProduct(BaseModel):
    """Model for Walmart product data."""
    
    # Basic product information
    product_id: Optional[str] = Field(None, alias="id")
    name: Optional[str] = None
    brand: Optional[str] = None
    model: Optional[str] = None
    sku: Optional[str] = None
    
    # Pricing
    price: Optional[float] = None
    original_price: Optional[float] = None
    currency: str = "USD"
    
    # Product details
    description: Optional[str] = None
    short_description: Optional[str] = None
    category: Optional[str] = None
    subcategory: Optional[str] = None
    
    # Availability
    in_stock: Optional[bool] = None
    stock_quantity: Optional[int] = None
    availability_status: Optional[str] = None
    
    # Images
    image_url: Optional[str] = None
    image_urls: List[str] = Field(default_factory=list)
    
    # Ratings and reviews
    rating: Optional[float] = None
    review_count: Optional[int] = None
    
    # Product specifications
    specifications: Dict[str, Any] = Field(default_factory=dict)
    features: List[str] = Field(default_factory=list)
    
    # URLs
    product_url: Optional[str] = None
    buy_url: Optional[str] = None
    
    # Metadata
    scraped_at: datetime = Field(default_factory=datetime.utcnow)
    source: str = "walmart_rapidapi"
    
    class Config:
        validate_by_name = True


class WalmartCategory(BaseModel):
    """Model for Walmart category data."""
    
    category_id: Optional[str] = None
    name: Optional[str] = None
    url: Optional[str] = None
    parent_category: Optional[str] = None
    level: Optional[int] = None
    product_count: Optional[int] = None
    
    # Subcategories
    subcategories: List['WalmartCategory'] = Field(default_factory=list)
    
    # Products in this category
    products: List[WalmartProduct] = Field(default_factory=list)
    
    # Metadata
    scraped_at: datetime = Field(default_factory=datetime.utcnow)
    source: str = "walmart_rapidapi"


class WalmartSearchResult(BaseModel):
    """Model for Walmart search results."""
    
    query: str
    total_results: Optional[int] = None
    page: Optional[int] = None
    page_size: Optional[int] = None
    products: List[WalmartProduct] = Field(default_factory=list)
    
    # Search metadata
    search_time: Optional[float] = None
    filters_applied: Dict[str, Any] = Field(default_factory=dict)
    
    # Metadata
    scraped_at: datetime = Field(default_factory=datetime.utcnow)
    source: str = "walmart_rapidapi"


class APIResponse(BaseModel):
    """Generic API response wrapper."""
    
    success: bool
    data: Optional[Any] = None
    error: Optional[str] = None
    status_code: Optional[int] = None
    response_time: Optional[float] = None
    
    # Request metadata
    endpoint: Optional[str] = None
    method: Optional[str] = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class DataExtractionJob(BaseModel):
    """Model for tracking data extraction jobs."""
    
    job_id: str
    job_type: str  # 'category', 'search', 'product'
    status: str  # 'pending', 'running', 'completed', 'failed'
    
    # Job parameters
    parameters: Dict[str, Any] = Field(default_factory=dict)
    
    # Results
    total_items: Optional[int] = None
    processed_items: int = 0
    failed_items: int = 0
    
    # Timing
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    duration_seconds: Optional[float] = None
    
    # Error handling
    error_message: Optional[str] = None
    retry_count: int = 0
    
    # Metadata
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


# Update forward references
WalmartCategory.model_rebuild()
