"""
Tests for AKShare adapter.
"""
import pytest
from datetime import datetime, timedelta
import pandas as pd
from unittest.mock import patch, MagicMock
from src.data.adapters.akshare_adapter import AKShareAdapter
from src.data.models import StockInfo, MarketData


@pytest.fixture
def adapter():
    """Create an AKShare adapter instance."""
    return AKShareAdapter()


@pytest.fixture
def mock_stock_list():
    """Mock stock list data."""
    return pd.DataFrame({
        'code': ['000001', '600000'],
        'name': ['平安银行', '浦发银行']
    })


@pytest.fixture
def mock_stock_info():
    """Mock stock info data."""
    return pd.DataFrame({
        'item': ['名称', '行业', '上市日期'],
        'value': ['平安银行', '银行', '1991-04-03']
    })


@pytest.fixture
def mock_daily_data():
    """Mock daily historical data."""
    return pd.DataFrame({
        '日期': ['2024-02-28', '2024-02-29'],
        '开盘': [10.0, 10.1],
        '收盘': [10.2, 10.3],
        '最高': [10.4, 10.5],
        '最低': [9.8, 9.9],
        '成交量': [1000000, 1100000],
        '成交额': [10200000, 11330000]
    })


@pytest.fixture
def mock_real_time_data():
    """Mock real-time market data."""
    return pd.DataFrame({
        '代码': ['000001', '600000'],
        '名称': ['平安银行', '浦发银行'],
        '最新价': [10.2, 15.3],
        '开盘': [10.0, 15.0],
        '最高': [10.4, 15.5],
        '最低': [9.8, 14.8],
        '成交量': [1000000, 2000000],
        '成交额': [10200000, 30600000]
    })


@pytest.mark.asyncio
@patch('akshare.stock_info_a_code_name')
async def test_get_stock_list(mock_ak_stock_list, adapter, mock_stock_list):
    """Test getting stock list."""
    mock_ak_stock_list.return_value = mock_stock_list
    
    stocks = await adapter.get_stock_list()
    
    assert len(stocks) == 2
    assert all(isinstance(stock, StockInfo) for stock in stocks)
    assert all(stock.market == 'CN' for stock in stocks)
    
    # Test symbol format
    assert stocks[0].symbol == '000001.SZ'
    assert stocks[1].symbol == '600000.SH'


@pytest.mark.asyncio
@patch('akshare.stock_individual_info_em')
async def test_get_stock_info(mock_ak_stock_info, adapter, mock_stock_info):
    """Test getting stock information."""
    mock_ak_stock_info.return_value = mock_stock_info
    
    symbol = '000001.SZ'
    stock_info = await adapter.get_stock_info(symbol)
    
    assert isinstance(stock_info, StockInfo)
    assert stock_info.symbol == symbol
    assert stock_info.market == 'CN'
    assert stock_info.name == '平安银行'
    assert stock_info.industry == '银行'
    assert stock_info.list_date == datetime(1991, 4, 3)


@pytest.mark.asyncio
@patch('akshare.stock_zh_a_hist')
async def test_get_daily_data(mock_ak_daily_data, adapter, mock_daily_data):
    """Test getting daily historical data."""
    mock_ak_daily_data.return_value = mock_daily_data
    
    symbol = '000001.SZ'
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    
    df = await adapter.get_daily_data(symbol, start_date, end_date)
    
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert all(col in df.columns for col in ['open', 'high', 'low', 'close', 'volume', 'amount'])
    assert df.index.name == 'date'
    assert 'symbol' in df.columns
    assert all(df['symbol'] == symbol)


@pytest.mark.asyncio
@patch('akshare.stock_zh_a_spot_em')
async def test_get_real_time_quotes(mock_ak_real_time, adapter, mock_real_time_data):
    """Test getting real-time quotes."""
    mock_ak_real_time.return_value = mock_real_time_data
    
    symbols = ['000001.SZ', '600000.SH']
    quotes = await adapter.get_real_time_quotes(symbols)
    
    assert isinstance(quotes, dict)
    assert len(quotes) == len(symbols)
    assert all(symbol in quotes for symbol in symbols)
    assert all(isinstance(quote, MarketData) for quote in quotes.values())
    
    # Test quote data for first symbol
    quote = quotes['000001.SZ']
    assert quote.symbol == '000001.SZ'
    assert quote.market == 'CN'
    assert quote.timestamp is not None
    assert quote.open == 10.0
    assert quote.close == 10.2
    assert quote.volume == 1000000


def test_invalid_symbol(adapter):
    """Test handling of invalid symbols."""
    with pytest.raises(ValueError):
        adapter._validate_symbol('INVALID')
    
    with pytest.raises(ValueError):
        adapter._validate_symbol('000001.XX')


def test_invalid_date_range(adapter):
    """Test handling of invalid date ranges."""
    future_date = datetime.now() + timedelta(days=1)
    past_date = datetime.now() - timedelta(days=1)
    
    with pytest.raises(ValueError):
        adapter._validate_date_range(future_date, future_date)
    
    with pytest.raises(ValueError):
        adapter._validate_date_range(past_date, past_date - timedelta(days=1))
