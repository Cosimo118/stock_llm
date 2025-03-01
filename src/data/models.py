"""
Base data models for the quantitative trading system.
"""
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List, Dict, Any
import pandas as pd

@dataclass
class MarketData:
    """Base class for market data."""
    symbol: str
    market: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int
    amount: float
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'MarketData':
        """Create a MarketData instance from a dictionary."""
        return cls(**data)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert the instance to a dictionary."""
        return {
            'symbol': self.symbol,
            'market': self.market,
            'timestamp': self.timestamp,
            'open': self.open,
            'high': self.high,
            'low': self.low,
            'close': self.close,
            'volume': self.volume,
            'amount': self.amount
        }

@dataclass
class StockInfo:
    """Stock basic information."""
    symbol: str
    name: str
    market: str
    industry: Optional[str] = None
    list_date: Optional[datetime] = None
    is_active: bool = True

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'StockInfo':
        """Create a StockInfo instance from a dictionary."""
        return cls(**data)

class DataValidator:
    """Data validation utilities."""
    
    @staticmethod
    def validate_market_data(data: pd.DataFrame) -> bool:
        """
        Validate market data DataFrame.
        
        Args:
            data: DataFrame containing market data
            
        Returns:
            bool: True if data is valid, False otherwise
        """
        required_columns = ['symbol', 'market', 'timestamp', 'open', 'high', 'low', 'close', 'volume', 'amount']
        
        # Check required columns
        if not all(col in data.columns for col in required_columns):
            return False
        
        # Check data types
        numeric_columns = ['open', 'high', 'low', 'close', 'volume', 'amount']
        for col in numeric_columns:
            if not pd.api.types.is_numeric_dtype(data[col]):
                return False
        
        # Check for missing values
        if data[required_columns].isnull().any().any():
            return False
        
        # Check value ranges
        if (data[['open', 'high', 'low', 'close']] < 0).any().any():
            return False
        
        if (data[['volume', 'amount']] < 0).any().any():
            return False
        
        return True
