import sys
import os
import asyncio
import logging
from datetime import datetime, timedelta

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.data.adapters.akshare_adapter import AKShareAdapter, Period

# 配置日志
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

async def test_cache():
    """测试数据缓存功能"""
    adapter = AKShareAdapter()
    
    # 测试数据参数
    symbols = ['600519.SH', '000858.SZ', '600887.SH']
    end_date = '20230220'
    start_date = '20230120'
    
    logging.info("\n测试单只股票数据缓存")
    logging.info(f"获取股票 {symbols[0]} 的数据")
    
    # 第一次获取数据(从API)
    data1 = await adapter.get_historical_data(
        symbol=symbols[0],
        period=Period.DAILY,
        start_date=start_date,
        end_date=end_date
    )
    logging.info(f"首次获取数据行数: {len(data1)}")
    
    # 第二次获取数据(应该从缓存获取)
    data2 = await adapter.get_historical_data(
        symbol=symbols[0],
        period=Period.DAILY,
        start_date=start_date,
        end_date=end_date
    )
    logging.info(f"第二次获取数据行数: {len(data2)}")
    
    logging.info("\n测试批量数据缓存")
    logging.info(f"获取{len(symbols)}只股票的数据")
    
    # 第一次批量获取
    batch_data1 = await adapter.get_batch_historical_data(
        symbols=symbols,
        period=Period.DAILY,
        start_date=start_date,
        end_date=end_date
    )
    logging.info(f"首次批量获取数据行数: {len(batch_data1)}")
    
    # 第二次批量获取(应该全部从缓存获取)
    batch_data2 = await adapter.get_batch_historical_data(
        symbols=symbols,
        period=Period.DAILY,
        start_date=start_date,
        end_date=end_date
    )
    logging.info(f"第二次批量获取数据行数: {len(batch_data2)}")
    
    # 测试部分缓存命中
    new_symbols = symbols + ['002304.SZ']
    logging.info(f"\n测试部分缓存命中")
    logging.info(f"获取{len(new_symbols)}只股票的数据(其中{len(symbols)}只已缓存)")
    
    batch_data3 = await adapter.get_batch_historical_data(
        symbols=new_symbols,
        period=Period.DAILY,
        start_date=start_date,
        end_date=end_date
    )
    logging.info(f"数据行数: {len(batch_data3)}")

if __name__ == "__main__":
    asyncio.run(test_cache())
