import os 
import json

files = os.listdir()
json_files = [f for f in files if f[-4:] == 'json']    
print(json_files)

def merge_JsonFiles(filename):
    result = list()
    for f1 in filename:
        with open(f1, 'r') as infile:
            result.extend(json.load(infile))

    with open('6senseCompanies.json', 'w') as output_file:
        json.dump(result, output_file)

merge_JsonFiles(json_files)