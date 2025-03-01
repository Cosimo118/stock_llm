"""
AKShare adapter for Chinese A-share market data.
"""
from datetime import datetime
from enum import Enum
from typing import List, Dict, Any, Optional
import asyncio
import pandas as pd
import akshare as ak
from functools import lru_cache
from ..cache.cache_manager import CacheManager
import re
import logging

from .base_adapter import MarketDataAdapter
from ..models import MarketData, StockInfo
from ...utils.logger import log
from ...config.settings import DATA_SOURCE_CONFIG

logger = logging.getLogger(__name__)

class Period(Enum):
    """Data period types."""
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"


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
        self.cache = CacheManager()
        
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
            
            log.debug(f"\n[DEBUG] 股票列表获取成功，共{len(stocks)}只股票")
            log.debug("\n[DEBUG] 股票列表:")
            for stock in stocks:
                log.debug(stock.symbol)
            
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
            code = self._format_symbol(symbol)
            
            # 获取股票详细信息
            df = ak.stock_individual_info_em(code)
            
            # 转换为字典
            info_dict = df.set_index('item').to_dict()['value']
            
            log.debug(f"\n[DEBUG] 股票{symbol}详细信息获取成功")
            log.debug("\n[DEBUG] 股票详细信息:")
            for key, value in info_dict.items():
                log.debug(f"{key}: {value}")
            
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
    
    async def get_historical_data(
        self,
        symbol: str,
        period: Period,
        start_date: str,
        end_date: str,
        adjust_type: str = 'qfq'
    ) -> pd.DataFrame:
        """获取历史K线数据
        
        Args:
            symbol: 股票代码
            period: 数据周期
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)
            adjust_type: 复权类型, 默认前复权
            
        Returns:
            DataFrame包含历史数据
        """
        try:
            # 尝试从缓存获取数据
            cached_data = self.cache.get_data(
                symbol, period.value, start_date, end_date
            )
            if cached_data is not None:
                return cached_data

            self._validate_symbol(symbol)
            
            log.debug(f"开始获取数据...")
            log.debug(f"参数: symbol={symbol}, period={period.value}, "
                      f"start_date={start_date}, end_date={end_date}, adjust={adjust_type}")
            
            df = ak.stock_zh_a_hist(
                symbol=self._format_symbol(symbol),
                period=period.value,
                start_date=start_date,
                end_date=end_date,
                adjust=adjust_type
            )
            
            if df.empty:
                raise ValueError(f"获取数据为空: {symbol}")
                
            # 标准化数据格式
            df = df.rename(columns=self.COLUMN_MAP)
            df['date'] = pd.to_datetime(df['date']).apply(lambda x: x.strftime('%Y%m%d'))
            df = df[['date', 'open', 'high', 'low', 'close', 'volume', 'amount']]
            df = df.sort_values('date')
            df['symbol'] = symbol
            
            # 保存到缓存
            self.cache.save_data(
                symbol, period.value, start_date, end_date, df
            )
            
            return df
            
        except Exception as e:
            log.error(f"获取历史数据出错: {str(e)}")
            raise

    async def get_batch_historical_data(
        self,
        symbols: List[str],
        period: Period,
        start_date: str,
        end_date: str,
        max_workers: int = 5
    ) -> pd.DataFrame:
        """批量获取多只股票的历史数据
        
        Args:
            symbols: 股票代码列表
            period: 数据周期
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)
            max_workers: 最大并发数
            
        Returns:
            包含所有股票数据的DataFrame
        """
        try:
            log.debug(f"开始批量获取{len(symbols)}只股票的{period.value}数据")
            log.debug(f"股票列表: {symbols}")
            
            # 先从缓存获取数据
            missed_symbols, cached_data = self.cache.get_batch_data(
                symbols, period.value, start_date, end_date
            )
            
            if not missed_symbols:
                log.debug("所有数据均命中缓存")
                return pd.concat(cached_data, ignore_index=True)
                
            # 获取未缓存的数据
            log.debug(f"从API获取{len(missed_symbols)}只股票的数据")
            semaphore = asyncio.Semaphore(max_workers)
            tasks = []
            
            for symbol in missed_symbols:
                task = self._get_single_stock_data(
                    symbol=symbol,
                    period=period,
                    start_date=start_date,
                    end_date=end_date,
                    semaphore=semaphore
                )
                tasks.append(task)
                
            new_data = await asyncio.gather(*tasks)
            
            # 保存新数据到缓存
            self.cache.save_batch_data(
                missed_symbols, period.value,
                start_date, end_date, new_data
            )
            
            # 合并所有数据
            all_data = cached_data + new_data
            result = pd.concat(all_data, ignore_index=True)
            
            log.debug(f"成功获取{len(symbols)}/{len(symbols)}只股票的数据")
            log.debug(f"数据行数: {len(result)}")
            
            return result
            
        except Exception as e:
            log.error(f"批量获取数据出错: {str(e)}")
            raise

    async def _get_single_stock_data(
        self,
        symbol: str,
        period: Period,
        start_date: str,
        end_date: str,
        semaphore: asyncio.Semaphore,
        adjust_type: str = 'qfq'
    ) -> pd.DataFrame:
        """获取单只股票数据(内部方法)"""
        async with semaphore:
            return await self.get_historical_data(
                symbol=symbol,
                period=period,
                start_date=start_date,
                end_date=end_date,
                adjust_type=adjust_type
            )
    
    # 为了保持向后兼容性，保留get_daily_data方法
    async def get_daily_data(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        adjust_type: str = 'qfq'
    ) -> pd.DataFrame:
        """Get daily historical data for an A-share stock.
        
        This is a convenience method that calls get_historical_data with period=Period.DAILY
        """
        return await self.get_historical_data(
            symbol=symbol,
            period=Period.DAILY,
            start_date=start_date,
            end_date=end_date,
            adjust_type=adjust_type
        )
    
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
                    timestamp=datetime.now(),
                    open=float(stock_data['开盘']),
                    high=float(stock_data['最高']),
                    low=float(stock_data['最低']),
                    close=float(stock_data['最新价']),
                    volume=float(stock_data['成交量']),
                    amount=float(stock_data['成交额'])
                )
            
            return result
            
        except Exception as e:
            self._handle_error(e, "Failed to get real-time quotes")
            
    def _get_exchange_suffix(self, code: str) -> str:
        """Get exchange suffix for a stock code."""
        if code.startswith(('600', '601', '603', '605', '688')):
            return 'SH'  # 上海证券交易所
        elif code.startswith(('000', '001', '002', '003', '300', '301')):
            return 'SZ'  # 深圳证券交易所
        else:
            raise ValueError(f"Unknown exchange for stock code: {code}")
            
    def _format_symbol(self, symbol: str) -> str:
        """格式化股票代码（移除市场后缀）
        
        Args:
            symbol: 带市场后缀的股票代码 (e.g., '000001.SZ')
            
        Returns:
            不带后缀的股票代码 (e.g., '000001')
        """
        return symbol.split('.')[0]
        
    def _validate_symbol(self, symbol: str) -> None:
        """验证股票代码格式
        
        Args:
            symbol: 股票代码
            
        Raises:
            ValueError: 股票代码格式无效
        """
        if not isinstance(symbol, str):
            raise ValueError(f"股票代码必须是字符串类型: {symbol}")
            
        if not re.match(r'^\d{6}\.(SH|SZ)$', symbol):
            raise ValueError(f"股票代码格式无效: {symbol}")
