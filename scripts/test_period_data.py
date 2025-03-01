"""
Test script for fetching stock data with different periods.
"""
import sys
import asyncio
from datetime import datetime, timedelta
from pathlib import Path

import akshare as ak
import pandas as pd

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from src.data.adapters.akshare_adapter import AKShareAdapter, Period
from src.utils.logger import log


async def test_period(adapter: AKShareAdapter, symbol: str, period: Period):
    """Test data retrieval for a specific period."""
    try:
        end_date = datetime(2024, 1, 10)
        # 根据周期设置不同的时间范围
        if period == Period.DAILY:
            start_date = end_date - timedelta(days=10)
        elif period == Period.WEEKLY:
            start_date = end_date - timedelta(days=30)
        else:  # MONTHLY
            start_date = end_date - timedelta(days=180)
        
        log.info(f"\n获取 {symbol} 的{period.value}数据")
        log.info(f"时间范围: {start_date.date()} 到 {end_date.date()}")
        
        df = await adapter.get_historical_data(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            period=period
        )
        
        print(f"\n{period.value}数据统计:")
        print(df.describe())
        print(f"\n总行数: {len(df)}")
        
    except Exception as e:
        log.error(f"获取{period.value}数据失败: {str(e)}")
        raise


async def main():
    """Main function to test different period data."""
    try:
        adapter = AKShareAdapter()
        symbol = "600887.SH"  # 伊利股份
        
        # 测试不同周期的数据
        for period in Period:
            await test_period(adapter, symbol, period)
            print("\n" + "="*50 + "\n")
            
    except Exception as e:
        log.error(f"测试失败: {str(e)}")
        raise


if __name__ == '__main__':
    # 运行主函数
    asyncio.run(main())
