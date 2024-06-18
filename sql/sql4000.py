from pyignite import Client
from pyignite.datatypes import *
from pyignite.datatypes.prop_codes import *
import pandas as pd
import time
# 连接到 Ignite 集群
client = Client()
client.connect('127.0.0.1', 10800)

drop_table_query = "DROP TABLE IF EXISTS Person4000"
client.sql(drop_table_query)


csv_file_path = '/Users/huanghejun/Downloads/資料/data_with_id_4000.csv'  # 替换为实际的 CSV 文件路径
df = pd.read_csv(csv_file_path)
# 创建表
create_table_query = """
CREATE TABLE Person4000 (
    ID INTEGER PRIMARY KEY,
    Name VARCHAR,
    Age INTEGER,
    Number1 INTEGER,
    Number2 INTEGER
)
"""
client.sql(create_table_query)
start = time.time()
# 插入数据
insert_query = "INSERT INTO Person4000 (ID, Name, Age, Number1, Number2) VALUES (?, ?, ?, ?, ?)"

# 批量插入数据
for row in df.itertuples(index=False):
    print(row.ID)
    print('\n')
    client.sql(insert_query, query_args=[row.ID, row.Name, row.Age, row.Number1, row.Number2])
end = time.time()
time_consume = end-start
print(f'INSERT總共花了{time_consume}秒')


# 关闭连接
client.close()