"""
Test script for batch data retrieval functionality.
"""
import sys
import asyncio
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from src.data.adapters.akshare_adapter import AKShareAdapter, Period
from src.utils.logger import log


async def test_batch_data():
    """Test batch data retrieval for multiple stocks."""
    try:
        # 创建适配器实例
        adapter = AKShareAdapter()
        
        # 测试股票列表：白酒股
        symbols = [
            "600519.SH",  # 贵州茅台
            "000858.SZ",  # 五粮液
            "600887.SH",  # 伊利股份
            "002304.SZ",  # 洋河股份
            "000568.SZ"   # 泸州老窖
        ]
        
        # 设置时间范围
        end_date = datetime(2024, 1, 10)
        start_date = end_date - timedelta(days=30)
        
        log.info(f"\n开始批量获取{len(symbols)}只股票的数据")
        log.info(f"时间范围: {start_date.date()} 到 {end_date.date()}")
        log.info(f"股票列表: {symbols}")
        
        # 测试不同周期的批量数据获取
        for period in Period:
            log.info(f"\n获取{period.value}数据...")
            
            df = await adapter.get_batch_historical_data(
                symbols=symbols,
                start_date=start_date,
                end_date=end_date,
                period=period
            )
            
            # 分析数据
            print(f"\n{period.value}数据统计:")
            # 按股票分组统计
            stats = df.groupby('symbol').agg({
                'open': ['mean', 'min', 'max'],
                'volume': 'sum',
                'amount': 'sum'
            })
            print("\n每只股票的统计信息:")
            print(stats)
            
            # 计算每只股票的数据条数
            counts = df.groupby('symbol').size()
            print("\n每只股票的数据条数:")
            print(counts)
            
            print(f"\n总行数: {len(df)}")
            print("="*50)
            
    except Exception as e:
        log.error(f"测试失败: {str(e)}")
        raise


if __name__ == '__main__':
    # 运行测试
    asyncio.run(test_batch_data())
