"""
AKShare adapter for Chinese A-share market data.
"""
from datetime import datetime
from typing import List, Dict, Any, Optional
import pandas as pd
import akshare as ak
from functools import lru_cache

from .base_adapter import MarketDataAdapter
from ..models import MarketData, StockInfo
from ...utils.logger import log
from ...config.settings import DATA_SOURCE_CONFIG


class AKShareAdapter(MarketDataAdapter):
    """Adapter for retrieving A-share market data using AKShare."""
    
    # 列名映射
    COLUMN_MAP = {
        '日期': 'date',
        '开盘': 'open',
        '收盘': 'close',
        '最高': 'high',
        '最低': 'low',
        '成交量': 'volume',
        '成交额': 'amount',
        '振幅': 'amplitude',
        '涨跌幅': 'pct_change',
        '涨跌额': 'change',
        '换手率': 'turnover'
    }
    
    def __init__(self):
        """Initialize the AKShare adapter."""
        super().__init__(market='CN')
        self.config = DATA_SOURCE_CONFIG['akshare']
        
    @lru_cache(maxsize=1)
    async def get_stock_list(self) -> List[StockInfo]:
        """Get list of all A-share stocks.
        
        Returns:
            List of StockInfo objects
        """
        try:
            # 获取A股列表
            df = ak.stock_info_a_code_name()
            
            stocks = []
            for _, row in df.iterrows():
                stock_info = StockInfo(
                    symbol=f"{row['code']}.{self._get_exchange_suffix(row['code'])}",
                    name=row['name'],
                    market=self.market
                )
                stocks.append(stock_info)
            
            print(f"\n[DEBUG] 股票列表获取成功，共{len(stocks)}只股票")
            print("\n[DEBUG] 股票列表:")
            for stock in stocks:
                print(stock.symbol)
            
            return stocks
            
        except Exception as e:
            self._handle_error(e, "Failed to get stock list")
    
    async def get_stock_info(self, symbol: str) -> StockInfo:
        """Get detailed information for a specific A-share stock.
        
        Args:
            symbol: Stock symbol (e.g., '000001.SZ')
            
        Returns:
            StockInfo object with detailed information
        """
        try:
            self._validate_symbol(symbol)
            
            # 移除市场后缀
            code = symbol.split('.')[0]
            
            # 获取股票详细信息
            df = ak.stock_individual_info_em(code)
            
            # 转换为字典
            info_dict = df.set_index('item').to_dict()['value']
            
            print(f"\n[DEBUG] 股票{symbol}详细信息获取成功")
            print("\n[DEBUG] 股票详细信息:")
            for key, value in info_dict.items():
                print(f"{key}: {value}")
            
            return StockInfo(
                symbol=symbol,
                name=info_dict.get('名称', ''),
                market=self.market,
                industry=info_dict.get('行业', None),
                list_date=datetime.strptime(info_dict.get('上市日期', ''), '%Y-%m-%d')
                if info_dict.get('上市日期') else None
            )
            
        except Exception as e:
            self._handle_error(e, f"Failed to get stock info for {symbol}")
    
    async def get_daily_data(
        self,
        symbol: str,
        start_date: datetime,
        end_date: datetime,
        adjust_type: Optional[str] = 'qfq'
    ) -> pd.DataFrame:
        """Get daily historical data for an A-share stock.
        
        Args:
            symbol: Stock symbol (e.g., '000001.SZ')
            start_date: Start date
            end_date: End date
            adjust_type: Price adjustment type ('qfq' for forward adjustment,
                        'hfq' for backward adjustment, None for no adjustment)
            
        Returns:
            DataFrame with daily data
        """
        try:
            self._validate_symbol(symbol)
            self._validate_date_range(start_date, end_date)
            
            # 移除市场后缀
            code = symbol.split('.')[0]
            print(f"\n[DEBUG] 处理后的股票代码: {code}")
            
            # 获取历史数据
            print(f"\n[DEBUG] 开始获取数据...")
            print(f"参数: symbol={code}, period=daily, start_date={start_date.strftime('%Y%m%d')}, end_date={end_date.strftime('%Y%m%d')}, adjust={adjust_type}")
            
            df = ak.stock_zh_a_hist(
                symbol=code,
                period="daily",
                start_date=start_date.strftime('%Y%m%d'),
                end_date=end_date.strftime('%Y%m%d'),
                adjust=adjust_type
            )
            print("\n[DEBUG] 原始数据:")
            print(df)
            print("\n[DEBUG] 数据列名:")
            print(df.columns.tolist())
            
            # 检查数据是否为空
            if df.empty:
                print("\n[DEBUG] 获取到的数据为空!")
                raise ValueError(f"No data returned for symbol {symbol}")
                
            # 重命名列
            df = df.rename(columns=self.COLUMN_MAP)
            
            print("\n[DEBUG] 重命名后的列名:")
            print(df.columns.tolist())
            
            # 转换日期列
            df['date'] = pd.to_datetime(df['date'])
            
            # 只保留需要的列
            df = df[['date', 'open', 'high', 'low', 'close', 'volume', 'amount']]
            
            # 按日期排序
            df = df.sort_values('date')
            
            print("\n[DEBUG] 处理后的数据:")
            print(df[['open', 'high', 'low', 'close', 'volume', 'amount']].head())
            
            return df
            
        except Exception as e:
            self._handle_error(e, f"Failed to get daily data for {symbol}")
    
    async def get_real_time_quotes(self, symbols: List[str]) -> Dict[str, MarketData]:
        """Get real-time quotes for multiple A-share stocks.
        
        Args:
            symbols: List of stock symbols
            
        Returns:
            Dictionary mapping symbols to MarketData objects
        """
        try:
            for symbol in symbols:
                self._validate_symbol(symbol)
            
            # 获取实时行情
            codes = [s.split('.')[0] for s in symbols]
            df = ak.stock_zh_a_spot_em()  # 东方财富网接口
            
            # 过滤所需股票
            df = df[df['代码'].isin(codes)]
            
            result = {}
            for symbol in symbols:
                code = symbol.split('.')[0]
                stock_data = df[df['代码'] == code].iloc[0]
                
                result[symbol] = MarketData(
                    symbol=symbol,
                    market=self.market,
                    timestamp=datetime.now(),
                    open=float(stock_data['开盘']),
                    high=float(stock_data['最高']),
                    low=float(stock_data['最低']),
                    close=float(stock_data['最新价']),
                    volume=int(stock_data['成交量']),
                    amount=float(stock_data['成交额'])
                )
            
            print("\n[DEBUG] 实时行情获取成功")
            print("\n[DEBUG] 实时行情:")
            for symbol, data in result.items():
                print(f"{symbol}: {data}")
            
            return result
            
        except Exception as e:
            self._handle_error(e, f"Failed to get real-time quotes for {symbols}")
    
    def _get_exchange_suffix(self, code: str) -> str:
        """Get exchange suffix based on stock code.
        
        Args:
            code: Stock code
            
        Returns:
            Exchange suffix ('SH' or 'SZ')
        """
        if code.startswith(('600', '601', '603', '605', '688')):
            return 'SH'
        elif code.startswith(('000', '001', '002', '003', '300', '301')):
            return 'SZ'
        else:
            raise ValueError(f"Invalid stock code: {code}")
