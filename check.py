import pandas as pd
from datetime import datetime
import pytz

filename = (f'backtest_subgraph_Ethereum_USDC_ETH.csv')
#filename = (f'backtest_subgraph_Ethereum_ETH_USDT.csv')
#filename = (f'backtest_subgraph_Ethereum_WBTC_USDC.csv')

df = pd.read_csv(f'.//unCheck//{filename}')

start_date = "2021-08-01"
end_date   = "2023-07-31"

start_datetime = datetime.strptime(start_date + ' 00:00:00', "%Y-%m-%d %H:%M:%S")
end_datetime = datetime.strptime(end_date + ' 23:00:00', "%Y-%m-%d %H:%M:%S")
hours_difference = int((end_datetime - start_datetime).total_seconds() / 3600)

print(f"start_date: {start_date} ")
print(f"end_date  : {end_date}")
print(f"總小時數  : {hours_difference} ")

df['date'] = pd.to_datetime(df['periodStartUnix'], unit='s')

df['note'] = None

while True:
    # 檢查 periodStartUnix 是否間隔一小時
    time_diff = df['periodStartUnix'].diff()

    # 找到不是一小時間隔的行
    invalid_intervals = time_diff[time_diff != 3600]

    # 輸出不是一小時間隔的行
    if not invalid_intervals.empty and not invalid_intervals.isna().all():
        #print(f"不是一小時間隔的行:{invalid_intervals} \n")

        for i in range(1, len(df)):
            time_diff = df['date'].iloc[i] - df['date'].iloc[i - 1]

            # 檢查時差是否大於一小時
            if time_diff.total_seconds() > 3600:
                # 插入新行
                new_row = df.iloc[i - 1].copy()
                new_row['Unnamed: 0'] += 1
                new_row['date'] += pd.Timedelta(hours=1)
                new_row['periodStartUnix'] += 3600
                new_row['open'] = new_row['high'] = new_row['low'] = new_row['close'] = df['close'].iloc[i - 1]
                new_row['note'] = 'Data Interpolation'

                # 插入新行
                df = pd.concat([df.iloc[:i], pd.DataFrame([new_row]), df.iloc[i:]]).reset_index(drop=True)
                #print(f"new_row = {new_row['Unnamed: 0']} \n")
    else:
        print("== 檢查完成所有行都是一小時間隔 == \n")
        break


df['Date'] = df['date']
df.set_index('Date', inplace=True)
df = df.loc[start_date:end_date]

# 删除 'date' 列
df.drop('date', axis=1, inplace=True)

# 重設 'Unnamed: 0' 列
df.reset_index(drop=True, inplace=True)
df['Unnamed: 0'] = df.index

row = df.shape[0] - 1
print(f"Total row: {row}  last = {df['Unnamed: 0'].iloc[-1]}")

# 將更新後的DataFrame保存到新的CSV文件
df.to_csv(f'.//inCheck//{filename}', index=False)
