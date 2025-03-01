"""
Test script for fetching stock data.
"""
import sys
import asyncio
from datetime import datetime, timedelta
from pathlib import Path

import akshare as ak

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from src.data.adapters.akshare_adapter import AKShareAdapter
from src.utils.logger import log


async def main():
    """Main function to test stock data fetching."""
    try:
        # 直接使用AKShare获取数据
        symbol = '600887'  # 伊利股份
        end_date = datetime(2024, 1, 10)  # 使用1月的历史日期
        start_date = datetime(2024, 1, 1)
        
        log.info(f"直接获取 {symbol} 从 {start_date.date()} 到 {end_date.date()} 的日K线数据")
        
        # 获取原始数据
        df_raw = ak.stock_zh_a_hist(
            symbol=symbol,
            period="daily",
            start_date=start_date.strftime('%Y%m%d'),
            end_date=end_date.strftime('%Y%m%d'),
            adjust="qfq"
        )
        
        print("\n原始数据列名:")
        print(df_raw.columns.tolist())
        print("\n原始数据预览:")
        print(df_raw.head())
        
        # 使用适配器获取数据
        log.info("\n使用适配器获取数据")
        adapter = AKShareAdapter()
        symbol_with_suffix = f"{symbol}.SH"
        df = await adapter.get_daily_data(symbol_with_suffix, start_date, end_date)
        
        print("\n适配器处理后的数据预览:")
        print(df.head())
        print("\n数据统计:")
        print(df.describe())
        print(f"\n总行数: {len(df)}")
        
    except Exception as e:
        log.error(f"获取数据失败: {str(e)}")
        raise


if __name__ == '__main__':
    # 运行主函数
    asyncio.run(main())
