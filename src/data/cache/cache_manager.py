import os
import sqlite3
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, List, Tuple
import json
import logging

logger = logging.getLogger(__name__)

class CacheManager:
    """股票数据缓存管理器
    
    使用SQLite实现本地数据缓存，主要功能：
    1. 缓存历史行情数据
    2. 自动过期管理
    3. 批量数据存取
    """
    
    def __init__(self, cache_dir: str = "cache"):
        """初始化缓存管理器
        
        Args:
            cache_dir: 缓存目录路径
        """
        self.cache_dir = cache_dir
        os.makedirs(cache_dir, exist_ok=True)
        self.db_path = os.path.join(cache_dir, "stock_data.db")
        self._init_db()
        
    def _init_db(self):
        """初始化数据库表结构"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS stock_data (
                    symbol TEXT,
                    period TEXT,
                    start_date TEXT,
                    end_date TEXT,
                    data TEXT,
                    created_at TIMESTAMP,
                    expire_at TIMESTAMP,
                    PRIMARY KEY (symbol, period, start_date, end_date)
                )
            """)
            
    def _get_expire_time(self, period: str) -> datetime:
        """获取数据过期时间
        
        不同周期数据缓存时间不同：
        - 日K数据: 7天
        - 周K数据: 15天
        - 月K数据: 30天
        """
        now = datetime.now()
        expire_days = {
            "daily": 7,
            "weekly": 15,
            "monthly": 30
        }
        return now + timedelta(days=expire_days.get(period, 7))
        
    def get_data(self, symbol: str, period: str, 
                 start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """获取缓存的股票数据
        
        Args:
            symbol: 股票代码
            period: 数据周期(daily/weekly/monthly)
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            DataFrame或None(未命中缓存)
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute("""
                    SELECT data FROM stock_data 
                    WHERE symbol = ? AND period = ? 
                    AND start_date = ? AND end_date = ?
                    AND expire_at > datetime('now')
                """, (symbol, period, start_date, end_date))
                row = cursor.fetchone()
                
                if row:
                    logger.debug(f"缓存命中: {symbol} {period} {start_date}-{end_date}")
                    return pd.read_json(row[0])
                    
                logger.debug(f"缓存未命中: {symbol} {period} {start_date}-{end_date}")
                return None
                
        except Exception as e:
            logger.error(f"读取缓存出错: {str(e)}")
            return None
            
    def save_data(self, symbol: str, period: str,
                  start_date: str, end_date: str,
                  data: pd.DataFrame) -> bool:
        """保存股票数据到缓存
        
        Args:
            symbol: 股票代码
            period: 数据周期
            start_date: 开始日期
            end_date: 结束日期
            data: 股票数据DataFrame
            
        Returns:
            是否保存成功
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                expire_at = self._get_expire_time(period)
                conn.execute("""
                    INSERT OR REPLACE INTO stock_data
                    (symbol, period, start_date, end_date, data, created_at, expire_at)
                    VALUES (?, ?, ?, ?, ?, datetime('now'), ?)
                """, (
                    symbol, period, start_date, end_date,
                    data.to_json(), expire_at
                ))
                logger.debug(f"保存缓存成功: {symbol} {period} {start_date}-{end_date}")
                return True
                
        except Exception as e:
            logger.error(f"保存缓存出错: {str(e)}")
            return False
            
    def get_batch_data(self, symbols: List[str], period: str,
                      start_date: str, end_date: str) -> Tuple[List[str], List[pd.DataFrame]]:
        """批量获取缓存数据
        
        Args:
            symbols: 股票代码列表
            period: 数据周期
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            (未命中缓存的股票列表, 命中缓存的数据列表)
        """
        cached_data = []
        missed_symbols = []
        
        for symbol in symbols:
            data = self.get_data(symbol, period, start_date, end_date)
            if data is not None:
                cached_data.append(data)
            else:
                missed_symbols.append(symbol)
                
        return missed_symbols, cached_data
        
    def save_batch_data(self, symbols: List[str], period: str,
                       start_date: str, end_date: str,
                       data_list: List[pd.DataFrame]) -> None:
        """批量保存数据到缓存
        
        Args:
            symbols: 股票代码列表
            period: 数据周期
            start_date: 开始日期
            end_date: 结束日期
            data_list: 数据DataFrame列表
        """
        for symbol, data in zip(symbols, data_list):
            self.save_data(symbol, period, start_date, end_date, data)
            
    def clear_expired(self) -> int:
        """清理过期缓存数据
        
        Returns:
            清理的记录数
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute("""
                    DELETE FROM stock_data 
                    WHERE expire_at <= datetime('now')
                """)
                return cursor.rowcount
        except Exception as e:
            logger.error(f"清理过期缓存出错: {str(e)}")
            return 0
