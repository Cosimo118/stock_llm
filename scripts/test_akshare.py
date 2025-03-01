"""
Simple test for AKShare functionality
"""
import akshare as ak

def main():
    """Test basic AKShare functionality"""
    try:
        # 获取股票列表
        print("获取股票列表...")
        stock_info = ak.stock_info_a_code_name()
        print(f"获取到 {len(stock_info)} 只股票")
        print("\n前5只股票:")
        print(stock_info.head())
        
        # 获取单只股票信息
        symbol = "600887"
        print(f"\n\n获取 {symbol} 的股票信息...")
        stock_info = ak.stock_individual_info_em(symbol=symbol)
        print(stock_info)
        
        # 获取日K线数据
        print(f"\n\n获取 {symbol} 的日K线数据...")
        df = ak.stock_zh_a_hist(symbol=symbol, period="daily", start_date="20240101", end_date="20240228", adjust="qfq")
        print("\n数据列名:")
        print(df.columns.tolist())
        print("\n数据预览:")
        print(df.head())
        
    except Exception as e:
        print(f"发生错误: {str(e)}")
        raise

if __name__ == "__main__":
    main()
