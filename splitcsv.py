import os
import pandas as pd

files = os.listdir()
csv_files = [f for f in files if f[-3:] == 'csv']


for i,chunk in enumerate(pd.read_csv(csv_files, chunksize= 1346484)):
    chunk.to_csv('chunk{}.csv'.format(i), index=False)