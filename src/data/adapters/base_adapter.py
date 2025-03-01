"""
Base adapter interface for market data sources.
"""
from abc import ABC, abstractmethod
from datetime import datetime
from typing import List, Dict, Any, Optional
import pandas as pd

from ..models import MarketData, StockInfo
from ...utils.logger import log


class MarketDataAdapter(ABC):
    """Base class for market data adapters."""
    
    def __init__(self, market: str):
        """Initialize the adapter.
        
        Args:
            market: Market identifier (e.g., 'CN' for China A-shares)
        """
        self.market = market
        self._validate_market()
    
    def _validate_market(self) -> None:
        """Validate market identifier."""
        valid_markets = ['CN', 'HK']
        if self.market not in valid_markets:
            raise ValueError(f"Invalid market: {self.market}. Must be one of {valid_markets}")
    
    @abstractmethod
    async def get_stock_list(self) -> List[StockInfo]:
        """Get list of all available stocks.
        
        Returns:
            List of StockInfo objects
        """
        pass
    
    @abstractmethod
    async def get_stock_info(self, symbol: str) -> StockInfo:
        """Get detailed information for a specific stock.
        
        Args:
            symbol: Stock symbol
            
        Returns:
            StockInfo object
        """
        pass
    
    @abstractmethod
    async def get_daily_data(
        self,
        symbol: str,
        start_date: datetime,
        end_date: datetime,
        adjust_type: Optional[str] = None
    ) -> pd.DataFrame:
        """Get daily historical data for a stock.
        
        Args:
            symbol: Stock symbol
            start_date: Start date for historical data
            end_date: End date for historical data
            adjust_type: Price adjustment type ('qfq' for forward adjustment,
                        'hfq' for backward adjustment, None for no adjustment)
            
        Returns:
            DataFrame with daily data
        """
        pass
    
    @abstractmethod
    async def get_real_time_quotes(self, symbols: List[str]) -> Dict[str, MarketData]:
        """Get real-time quotes for multiple stocks.
        
        Args:
            symbols: List of stock symbols
            
        Returns:
            Dictionary mapping symbols to MarketData objects
        """
        pass
    
    def _validate_symbol(self, symbol: str) -> None:
        """Validate stock symbol format.
        
        Args:
            symbol: Stock symbol to validate
            
        Raises:
            ValueError: If symbol format is invalid
        """
        if self.market == 'CN':
            if not (symbol.endswith('.SH') or symbol.endswith('.SZ')):
                raise ValueError(f"Invalid A-share symbol format: {symbol}")
        elif self.market == 'HK':
            if not symbol.endswith('.HK'):
                raise ValueError(f"Invalid Hong Kong stock symbol format: {symbol}")
    
    def _validate_date_range(self, start_date: datetime, end_date: datetime) -> None:
        """Validate date range.
        
        Args:
            start_date: Start date
            end_date: End date
            
        Raises:
            ValueError: If date range is invalid
        """
        if start_date > end_date:
            raise ValueError("Start date must be earlier than end date")
        if end_date > datetime.now():
            raise ValueError("End date cannot be in the future")
    
    def _format_date(self, date: datetime) -> str:
        """Format datetime object to string.
        
        Args:
            date: Datetime object
            
        Returns:
            Formatted date string (YYYY-MM-DD)
        """
        return date.strftime('%Y-%m-%d')
    
    def _handle_error(self, error: Exception, context: str) -> None:
        """Handle and log errors.
        
        Args:
            error: Exception object
            context: Error context description
        """
        log.error(f"Error in {self.__class__.__name__} - {context}: {str(error)}")
        raise error
