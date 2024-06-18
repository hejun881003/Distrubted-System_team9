import pandas as pd
from pyignite import Client, AioClient

class MyData:
    def __init__(self, IA, IB, IC, ID):
        self.IA = IA
        self.IB = IB
        self.IC = IC
        self.ID = ID

# 读取CSV文件
csv_file_path = 'file.csv'  # 将 'file.csv' 替换为实际的CSV文件路径
data = pd.read_csv(csv_file_path)

client = Client()
client.connect('127.0.0.1', 10800)

# 创建或重置缓存
try:
    my_cache = client.get_cache('my_cache')
    my_cache.clear()
except Exception as e:
    print(f"Error accessing or creating cache: {e}")

# 将CSV文件中的数据插入缓存
for index, row in data.iterrows():
    if len(row) < 6:
        print(f"Skipping row {index}: not enough columns")
        continue

    key = row.iloc[0]
    value = f"{row.iloc[1]},{row.iloc[2]},{row.iloc[3]},{row.iloc[4]},{row.iloc[5]}"
    print(f"Inserting key: {key}, value: {value}")
    my_cache.put(key, value)

print("Insert successfully!")

# 获取并打印插入的数据以验证
for index, row in data.iterrows():
    if len(row) < 6:
        continue

    key = row.iloc[0]
    result = my_cache.get(key)
    split_result = result.split(',')
    if len(split_result) >= 4:
        mydata = MyData(split_result[0], split_result[1], split_result[2], split_result[3])
        print(f"Retrieved key: {key}, value: {mydata.IA}")

