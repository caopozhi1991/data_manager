from tqsdk import TqApi, TqAuth
import pandas as pd
import os
from datetime import datetime, timedelta

def get_all_main_contracts():
    """获取当前市场所有主力合约列表"""
    with TqApi(auth=TqAuth("hxxx1991", "123456")) as api:
        # 方式1：通过 query_cont_quotes 直接获取全部主力合约（主力连续代码 KQ.m@...）[2†L24-L27]
        main_contracts = api.query_cont_quotes()
        # 返回格式示例：['KQ.m@SHFE.rb', 'KQ.m@DCE.m', 'KQ.m@CZCE.SR', ...]
        print(f"共获取 {len(main_contracts)} 个主力连续品种")
        main_continuous_list = []
        for contract in main_contracts:
            # 合约格式类似 "DCE.a2607" 或 "CZCE.SR501"
            parts = contract.split('.')
            if len(parts) == 2:
                exchange = parts[0]
                product = parts[1][:-4]  # 移除年份月份，提取品种代码
                # 构建主力连续合约代码
                main_continuous = f"KQ.m@{exchange}.{product}"
                main_continuous_list.append(main_continuous)

        # 去重
        main_continuous_list = list(set(main_continuous_list))
        print(main_continuous_list)
        return main_continuous_list

def download_daily_data(symbol, api, years=5):
    """下载日线数据（5年约1200根K线）"""
    klines = api.get_kline_serial(
        symbol=symbol,
        duration_seconds=86400,           # 日线
        data_length=2000                  # 取2000根，足够覆盖5年以上
    )
    df = pd.DataFrame(klines)
    df['datetime'] = pd.to_datetime(df['datetime'], unit='ns')
    # 只保留最近5年的数据
    cutoff_date = datetime.now() - timedelta(days=5*365)
    df = df[df['datetime'] >= cutoff_date]
    return df

def download_minute_data(symbol, api, minutes=5, days=365):
    """下载分钟线数据（365天约 365*6*24=52560 根5分钟线）"""
    duration = minutes * 60  # 5分钟 = 300秒
    # 设置足够大的 data_length 来覆盖一年数据
    # 期货一天交易约 7.5 小时 = 90 根5分钟线，一年约 23040 根
    klines = api.get_kline_serial(
        symbol=symbol,
        duration_seconds=duration,
        data_length=30000                 # 取30000根，足够覆盖一年以上
    )
    df = pd.DataFrame(klines)
    df['datetime'] = pd.to_datetime(df['datetime'], unit='ns')
    cutoff_date = datetime.now() - timedelta(days=days)
    df = df[df['datetime'] >= cutoff_date]
    return df

def save_to_csv(data, symbol, interval, output_dir="data"):
    """保存数据到CSV文件"""
    os.makedirs(output_dir, exist_ok=True)
    # 清理文件名中的特殊字符（KQ.m@XX.rb -> KQ_m@XX_rb）
    safe_symbol = symbol.replace('.', '_').replace('@', '_')
    filename = f"{output_dir}/{safe_symbol}_{interval}.csv"
    data.to_csv(filename, index=False)
    print(f"已保存: {filename}")

def main():
    api = TqApi(auth=TqAuth("hxxx1991", "123456"))
    
    try:
        # 1. 获取所有主力连续合约列表
        symbols = get_all_main_contracts()
        print(f"共有 {len(symbols)} 个主力连续品种需要下载")
        
        for i, symbol in enumerate(symbols):
            try:
                print(f"\n[{i+1}/{len(symbols)}] 正在处理: {symbol}")
                
                # 2. 下载日线数据（5年）
                print(f"  下载日线数据...")
                df_daily = download_daily_data(symbol, api, years=5)
                save_to_csv(df_daily, symbol, "daily", "daily_data")
                print(f"  日线下载完成: {len(df_daily)} 条")
                
                # 3. 下载分钟线数据（1年，默认为5分钟线）
                print(f"  下载分钟线数据...")
                df_minute = download_minute_data(symbol, api, minutes=5, days=365)
                save_to_csv(df_minute, symbol, "minute_5", "minute_data")
                print(f"  分钟线下载完成: {len(df_minute)} 条")
                
            except Exception as e:
                print(f"  下载失败: {e}")
                continue
                
    finally:
        api.close()

if __name__ == "__main__":
    main()