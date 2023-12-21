import json
import csv

# Opening JSON file
f = open('foodandbeverages.json')
data = json.load(f)

comp_data =[]

for comp in data:
    details={}
    #print(comp)
    details['ID'] = comp['_id']
    details['company_name']= comp['_source']['company_name']
    details['company_city']= comp['_source']['company_city']
    details['company_state']= comp['_source']['company_state']
    details['company_country']= comp['_source']['company_country']
    details['company_industry']= comp['_source']['company_industry']
    details['company_size']= comp['_source']['company_size']
    comp_data.append(details)
 
# now we will open a file for writing
with open('foodandbevergaesdetails.json', mode='w') as f:
      json.dump(comp_data, f)
 
# create the csv writer object
#csv_writer = csv.writer(data_file)
 
# Counter variable used for writing
# headers to the CSV file
#count = 0
 
# for comp in comp_data:
    # if count == 0:
#  
        #Writing headers of CSV file
        # header = comp.keys()
        # csv_writer.writerow(header)
        # count += 1
 
    # Writing data of CSV file
    #csv_writer.writerow(comp.values())
 
#data_file.close()
# 
# 
# 
# 
# 