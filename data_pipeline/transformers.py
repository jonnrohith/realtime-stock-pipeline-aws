"""
Data Transformers for cleaning and enriching financial data
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Union
from pathlib import Path
import json
import structlog

from .models import (
    DataTransformation, DataQualityCheck, DataQualityStatus,
    Stock, StockQuote, StockHistory, MarketScreener, StockNews
)

logger = structlog.get_logger()


class BaseTransformer:
    """Base class for all data transformers."""
    
    def __init__(self):
        pass
    
    def transform(self, data: List[Dict[str, Any]], transformation_type: str) -> List[Dict[str, Any]]:
        """Apply transformation to data."""
        raise NotImplementedError


class DataCleaner(BaseTransformer):
    """Data cleaning transformer."""
    
    def clean_stock_data(self, stocks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Clean stock data."""
        cleaned_data = []
        
        for stock in stocks:
            cleaned_stock = self._clean_stock_record(stock)
            if cleaned_stock:
                cleaned_data.append(cleaned_stock)
        
        logger.info("Stock data cleaned", 
                   original_count=len(stocks), 
                   cleaned_count=len(cleaned_data))
        
        return cleaned_data
    
    def _clean_stock_record(self, stock: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Clean a single stock record."""
        try:
            # Remove None values
            cleaned = {k: v for k, v in stock.items() if v is not None}
            
            # Clean symbol
            if "symbol" in cleaned:
                cleaned["symbol"] = str(cleaned["symbol"]).strip().upper()
            
            # Clean name
            if "name" in cleaned:
                cleaned["name"] = str(cleaned["name"]).strip()
            
            # Clean market cap
            if "market_cap" in cleaned and isinstance(cleaned["market_cap"], str):
                cleaned["market_cap"] = self._parse_market_cap(cleaned["market_cap"])
            
            # Clean currency
            if "currency" not in cleaned:
                cleaned["currency"] = "USD"
            
            return cleaned
            
        except Exception as e:
            logger.warning("Failed to clean stock record", error=str(e), stock=stock)
            return None
    
    def _parse_market_cap(self, market_cap_str: str) -> Optional[float]:
        """Parse market cap string to float."""
        try:
            # Remove commas and convert to float
            return float(market_cap_str.replace(",", ""))
        except (ValueError, TypeError):
            return None
    
    def clean_quote_data(self, quotes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Clean quote data."""
        cleaned_data = []
        
        for quote in quotes:
            cleaned_quote = self._clean_quote_record(quote)
            if cleaned_quote:
                cleaned_data.append(cleaned_quote)
        
        logger.info("Quote data cleaned", 
                   original_count=len(quotes), 
                   cleaned_count=len(cleaned_data))
        
        return cleaned_data
    
    def _clean_quote_record(self, quote: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Clean a single quote record."""
        try:
            # Remove None values
            cleaned = {k: v for k, v in quote.items() if v is not None}
            
            # Clean symbol
            if "symbol" in cleaned:
                cleaned["symbol"] = str(cleaned["symbol"]).strip().upper()
            
            # Clean numeric fields
            numeric_fields = ["price", "previous_close", "open", "high", "low", 
                            "volume", "market_cap", "pe_ratio", "dividend_yield",
                            "change", "change_percent"]
            
            for field in numeric_fields:
                if field in cleaned:
                    cleaned[field] = self._parse_numeric(cleaned[field])
            
            # Clean currency
            if "currency" not in cleaned:
                cleaned["currency"] = "USD"
            
            return cleaned
            
        except Exception as e:
            logger.warning("Failed to clean quote record", error=str(e), quote=quote)
            return None
    
    def _parse_numeric(self, value: Any) -> Optional[float]:
        """Parse numeric value."""
        if value is None:
            return None
        
        try:
            if isinstance(value, (int, float)):
                return float(value)
            
            if isinstance(value, str):
                # Remove common formatting
                cleaned = value.replace(",", "").replace("$", "").replace("%", "").strip()
                return float(cleaned)
            
            return None
        except (ValueError, TypeError):
            return None


class DataEnricher(BaseTransformer):
    """Data enrichment transformer."""
    
    def enrich_stock_data(self, stocks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Enrich stock data with additional fields."""
        enriched_data = []
        
        for stock in stocks:
            enriched_stock = self._enrich_stock_record(stock)
            enriched_data.append(enriched_stock)
        
        logger.info("Stock data enriched", count=len(enriched_data))
        return enriched_data
    
    def _enrich_stock_record(self, stock: Dict[str, Any]) -> Dict[str, Any]:
        """Enrich a single stock record."""
        enriched = stock.copy()
        
        # Add market cap category
        if "market_cap" in enriched and enriched["market_cap"]:
            enriched["market_cap_category"] = self._categorize_market_cap(enriched["market_cap"])
        
        # Add sector category
        if "sector" in enriched and enriched["sector"]:
            enriched["sector_category"] = self._categorize_sector(enriched["sector"])
        
        # Add data quality score
        enriched["data_quality_score"] = self._calculate_data_quality_score(stock)
        
        # Add enrichment timestamp
        enriched["enriched_at"] = datetime.utcnow().isoformat()
        
        return enriched
    
    def _categorize_market_cap(self, market_cap: float) -> str:
        """Categorize market cap."""
        if market_cap >= 200_000_000_000:  # 200B+
            return "Mega Cap"
        elif market_cap >= 10_000_000_000:  # 10B+
            return "Large Cap"
        elif market_cap >= 2_000_000_000:   # 2B+
            return "Mid Cap"
        elif market_cap >= 300_000_000:     # 300M+
            return "Small Cap"
        else:
            return "Micro Cap"
    
    def _categorize_sector(self, sector: str) -> str:
        """Categorize sector."""
        sector_mapping = {
            "Technology": "Growth",
            "Healthcare": "Defensive",
            "Financial Services": "Cyclical",
            "Consumer Discretionary": "Cyclical",
            "Consumer Staples": "Defensive",
            "Energy": "Cyclical",
            "Industrials": "Cyclical",
            "Materials": "Cyclical",
            "Real Estate": "Defensive",
            "Utilities": "Defensive",
            "Communication Services": "Growth"
        }
        return sector_mapping.get(sector, "Other")
    
    def _calculate_data_quality_score(self, record: Dict[str, Any]) -> float:
        """Calculate data quality score (0-1)."""
        total_fields = len(record)
        non_null_fields = sum(1 for v in record.values() if v is not None and v != "")
        return non_null_fields / total_fields if total_fields > 0 else 0.0


class DataAggregator(BaseTransformer):
    """Data aggregation transformer."""
    
    def aggregate_quotes_by_sector(self, quotes: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Aggregate quotes by sector."""
        df = pd.DataFrame(quotes)
        
        if "sector" not in df.columns:
            return {}
        
        sector_stats = df.groupby("sector").agg({
            "price": ["mean", "median", "std", "count"],
            "change_percent": ["mean", "median", "std"],
            "volume": ["sum", "mean"],
            "market_cap": ["sum", "mean"]
        }).round(2)
        
        return sector_stats.to_dict()
    
    def calculate_market_summary(self, quotes: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Calculate market summary statistics."""
        if not quotes:
            return {}
        
        df = pd.DataFrame(quotes)
        
        summary = {
            "total_stocks": len(quotes),
            "avg_price": df["price"].mean() if "price" in df.columns else None,
            "avg_change_percent": df["change_percent"].mean() if "change_percent" in df.columns else None,
            "total_volume": df["volume"].sum() if "volume" in df.columns else None,
            "total_market_cap": df["market_cap"].sum() if "market_cap" in df.columns else None,
            "gainers": len(df[df["change_percent"] > 0]) if "change_percent" in df.columns else 0,
            "losers": len(df[df["change_percent"] < 0]) if "change_percent" in df.columns else 0,
            "unchanged": len(df[df["change_percent"] == 0]) if "change_percent" in df.columns else 0
        }
        
        return {k: v for k, v in summary.items() if v is not None}


class DataValidator(BaseTransformer):
    """Data validation transformer."""
    
    def validate_data_quality(self, data: List[Dict[str, Any]], data_type: str) -> List[DataQualityCheck]:
        """Validate data quality."""
        checks = []
        
        # Completeness check
        completeness_check = self._check_completeness(data, data_type)
        checks.append(completeness_check)
        
        # Validity check
        validity_check = self._check_validity(data, data_type)
        checks.append(validity_check)
        
        # Consistency check
        consistency_check = self._check_consistency(data, data_type)
        checks.append(consistency_check)
        
        return checks
    
    def _check_completeness(self, data: List[Dict[str, Any]], data_type: str) -> DataQualityCheck:
        """Check data completeness."""
        if not data:
            return DataQualityCheck(
                check_id=f"completeness_{data_type}_{int(datetime.utcnow().timestamp())}",
                data_source=data_type,
                check_type="completeness",
                status=DataQualityStatus.ERROR,
                message="No data found",
                details={"record_count": 0}
            )
        
        # Check for required fields based on data type
        required_fields = self._get_required_fields(data_type)
        missing_fields = []
        
        for record in data:
            for field in required_fields:
                if field not in record or record[field] is None:
                    missing_fields.append(field)
        
        missing_percentage = len(missing_fields) / (len(data) * len(required_fields)) * 100
        
        if missing_percentage > 20:
            status = DataQualityStatus.ERROR
        elif missing_percentage > 10:
            status = DataQualityStatus.WARNING
        else:
            status = DataQualityStatus.VALID
        
        return DataQualityCheck(
            check_id=f"completeness_{data_type}_{int(datetime.utcnow().timestamp())}",
            data_source=data_type,
            check_type="completeness",
            status=status,
            message=f"Missing {missing_percentage:.1f}% of required fields",
            details={
                "record_count": len(data),
                "missing_fields": missing_fields,
                "missing_percentage": missing_percentage
            }
        )
    
    def _check_validity(self, data: List[Dict[str, Any]], data_type: str) -> DataQualityCheck:
        """Check data validity."""
        invalid_records = []
        
        for i, record in enumerate(data):
            if data_type == "stock_quotes":
                if "price" in record and (record["price"] is None or record["price"] <= 0):
                    invalid_records.append(f"Record {i}: Invalid price")
                if "volume" in record and record["volume"] is not None and record["volume"] < 0:
                    invalid_records.append(f"Record {i}: Negative volume")
        
        invalid_percentage = len(invalid_records) / len(data) * 100 if data else 0
        
        if invalid_percentage > 10:
            status = DataQualityStatus.ERROR
        elif invalid_percentage > 5:
            status = DataQualityStatus.WARNING
        else:
            status = DataQualityStatus.VALID
        
        return DataQualityCheck(
            check_id=f"validity_{data_type}_{int(datetime.utcnow().timestamp())}",
            data_source=data_type,
            check_type="validity",
            status=status,
            message=f"{invalid_percentage:.1f}% of records have invalid values",
            details={
                "record_count": len(data),
                "invalid_records": invalid_records,
                "invalid_percentage": invalid_percentage
            }
        )
    
    def _check_consistency(self, data: List[Dict[str, Any]], data_type: str) -> DataQualityCheck:
        """Check data consistency."""
        # Check for duplicate symbols
        symbols = [record.get("symbol") for record in data if record.get("symbol")]
        duplicate_symbols = len(symbols) - len(set(symbols))
        
        if duplicate_symbols > 0:
            status = DataQualityStatus.WARNING
            message = f"Found {duplicate_symbols} duplicate symbols"
        else:
            status = DataQualityStatus.VALID
            message = "No duplicate symbols found"
        
        return DataQualityCheck(
            check_id=f"consistency_{data_type}_{int(datetime.utcnow().timestamp())}",
            data_source=data_type,
            check_type="consistency",
            status=status,
            message=message,
            details={
                "record_count": len(data),
                "duplicate_symbols": duplicate_symbols
            }
        )
    
    def _get_required_fields(self, data_type: str) -> List[str]:
        """Get required fields for data type."""
        field_mapping = {
            "market_tickers": ["symbol", "name"],
            "stock_quotes": ["symbol", "price"],
            "stock_history": ["symbol", "date", "close"],
            "market_screeners": ["symbol", "screener_type"],
            "stock_news": ["symbol", "title", "url"]
        }
        return field_mapping.get(data_type, ["symbol"])


class DataTransformer:
    """Main data transformer orchestrator."""
    
    def __init__(self):
        self.cleaner = DataCleaner()
        self.enricher = DataEnricher()
        self.aggregator = DataAggregator()
        self.validator = DataValidator()
    
    def transform_data(self, data: List[Dict[str, Any]], data_type: str) -> Dict[str, Any]:
        """Transform data through the complete pipeline."""
        logger.info("Starting data transformation", data_type=data_type, count=len(data))
        
        # Clean data
        cleaned_data = self.cleaner.clean_stock_data(data)
        
        # Enrich data
        enriched_data = self.enricher.enrich_stock_data(cleaned_data)
        
        # Validate data quality
        quality_checks = self.validator.validate_data_quality(enriched_data, data_type)
        
        # Generate aggregations
        aggregations = {}
        if data_type == "stock_quotes":
            aggregations = {
                "sector_aggregation": self.aggregator.aggregate_quotes_by_sector(enriched_data),
                "market_summary": self.aggregator.calculate_market_summary(enriched_data)
            }
        
        result = {
            "transformed_data": enriched_data,
            "quality_checks": [check.dict() for check in quality_checks],
            "aggregations": aggregations,
            "transformation_metadata": {
                "original_count": len(data),
                "cleaned_count": len(cleaned_data),
                "enriched_count": len(enriched_data),
                "transformed_at": datetime.utcnow().isoformat()
            }
        }
        
        logger.info("Data transformation completed", 
                   data_type=data_type, 
                   original_count=len(data),
                   final_count=len(enriched_data))
        
        return result
