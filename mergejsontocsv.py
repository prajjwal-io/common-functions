import pandas as pd
import os 
import json

files = os.listdir()
json_files = [f for f in files if f[-4:] == 'json']    
print(json_files)

def merge_JsonFiles(filename):
    result = []
    
    for f1 in filename:
        print(f1)
        try:
            with open(f1, 'r') as infile:
                data = json.load(infile)
                list_of_data = data["data"]
                result.extend(list_of_data)
                #print(result)
        except Exception as e:
            print(e)
    try:

        with open('6sense.json', 'w') as output_file:
            json.dump(result, output_file)

        with open('6sense.json', encoding='utf-8') as inputfile:
            df = pd.read_json(inputfile)

        df.to_csv('6sense.csv', encoding='utf-8', index=False)
    except Exception as e:
        print(e)

merge_JsonFiles(json_files)

