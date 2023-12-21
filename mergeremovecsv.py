import os 
import pandas as pd

#list all the files from the directory

files = os.listdir()
#print(files)

csv_files = [f for f in files if f[-3:] == 'csv']    
#print(csv_files)
#csv_concat = pd.concat([pd.read_csv(f) for f in csv_files ], ignore_index=True)
#print(csv_concat)
#csv_concat.to_csv('6sense.csv')


for file in csv_files:
    os.remove(file)