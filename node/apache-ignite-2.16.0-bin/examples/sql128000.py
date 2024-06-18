from pyignite import Client
from pyignite.datatypes import *
from pyignite.datatypes.prop_codes import *
import pandas as pd
import time

# 连接到 Ignite 集群
client = Client()
client.connect('127.0.0.1', 10800)
i = 0
sum1 = 0
for i in range(100):
 query = "SELECT * FROM Person128000 WHERE Name = 'Name1'"
 start = time.time()
 result = list(client.sql(query))
 end = time.time()
 time_consume = end - start
 sum1 = sum1 + time_consume
 i = i + 1 

print(f"SELECT平均花了: {sum1/100} 秒")
print('\n')
sum1 = 0
i = 0
for i in range(100):
 query = "SELECT AVG(Age) FROM Person128000"
 start = time.time()
 result = list(client.sql(query))
 end = time.time()
 time_consume = end - start
 sum1 = sum1 + time_consume
 i = i + 1 

print(f"AVG平均花了: {sum1/100} 秒")
print('\n')
sum1 = 0
i = 0
for i in range(100):
 query = "SELECT SUM(Age) FROM Person128000"
 start = time.time()
 result = list(client.sql(query))
 end = time.time()
 time_consume = end - start
 sum1 = sum1 + time_consume
 i = i + 1 

print(f"SUM平均花了: {sum1/100} 秒")
print('\n')

sum1 = 0
i = 0
for i in range(2):
 query = "SELECT * FROM Person128000 ORDER BY Age DESC"
 start = time.time()
 result = list(client.sql(query))
 end = time.time()
 time_consume = end - start
 sum1 = sum1 + time_consume
 i = i + 1 

print(f"SORT平均花了: {sum1/2} 秒")
print('\n')
sum1 = 0
i = 0
for i in range(2):
 query = "UPDATE Person128000 SET Number2=100 WHERE AGE=1"
 start = time.time()
 result = list(client.sql(query))
 end = time.time()
 time_consume = end - start
 sum1 = sum1 + time_consume
 i = i + 1 

print(f"Update平均花了: {sum1/2} 秒")
print('\n')

sum1 = 0
i = 0
for i in range(2):
 query = "SELECT * FROM Person128000 INNER JOIN Person4000 WHERE Person128000.Number1 = Person4000.Number1"
 start = time.time()
 result = list(client.sql(query))
 end = time.time()
 time_consume = end - start
 sum1 = sum1 + time_consume
 i = i + 1 

print(f"Join平均花了: {sum1/2} 秒")
print('\n')