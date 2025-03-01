"""
Tests for data models.
"""
import pytest
from datetime import datetime
import pandas as pd
import numpy as np
from src.data.models import MarketData, StockInfo, DataValidator

def test_market_data_creation():
    """Test MarketData instance creation."""
    data = {
        'symbol': '000001.SZ',
        'market': 'CN',
        'timestamp': datetime.now(),
        'open': 10.0,
        'high': 11.0,
        'low': 9.0,
        'close': 10.5,
        'volume': 1000000,
        'amount': 10500000.0
    }
    
    market_data = MarketData.from_dict(data)
    assert market_data.symbol == '000001.SZ'
    assert market_data.market == 'CN'
    assert isinstance(market_data.timestamp, datetime)
    assert market_data.open == 10.0

def test_stock_info_creation():
    """Test StockInfo instance creation."""
    data = {
        'symbol': '000001.SZ',
        'name': 'Test Stock',
        'market': 'CN',
        'industry': 'Technology',
        'list_date': datetime.now(),
        'is_active': True
    }
    
    stock_info = StockInfo.from_dict(data)
    assert stock_info.symbol == '000001.SZ'
    assert stock_info.name == 'Test Stock'
    assert stock_info.industry == 'Technology'

def test_data_validator():
    """Test DataValidator functionality."""
    # Create valid test data
    valid_data = pd.DataFrame({
        'symbol': ['000001.SZ'],
        'market': ['CN'],
        'timestamp': [datetime.now()],
        'open': [10.0],
        'high': [11.0],
        'low': [9.0],
        'close': [10.5],
        'volume': [1000000],
        'amount': [10500000.0]
    })
    
    assert DataValidator.validate_market_data(valid_data) == True
    
    # Test with invalid data (negative prices)
    invalid_data = valid_data.copy()
    invalid_data['close'] = -10.0
    assert DataValidator.validate_market_data(invalid_data) == False
    
    # Test with missing columns
    invalid_data = valid_data.drop('volume', axis=1)
    assert DataValidator.validate_market_data(invalid_data) == False
    
    # Test with null values
    invalid_data = valid_data.copy()
    invalid_data.loc[0, 'close'] = np.nan
    assert DataValidator.validate_market_data(invalid_data) == False
