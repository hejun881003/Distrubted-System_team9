import pandas as pd
from pyignite import Client

client = Client()
client.connect('127.0.0.1', 10800)


my_cache = client.get_cache('my_cache')
i = 5001
print(my_cache.get(i))


