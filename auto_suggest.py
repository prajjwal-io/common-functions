import requests
import json
import requests
import csv
import time
from concurrent.futures import ThreadPoolExecutor
import itertools

start = time.time()



def read_json(filepath):
    with open(filepath, "r") as f:
        data = json.load(f)
    return data

df = read_json("foodandbevergaesdetails.json")
names= []

for detail in df:
        comp_name = detail['company_name']
        names.append(comp_name)

#print(len(names))


def split(list_a, chunk_size):
    for i in range(0, len(list_a), chunk_size):
        yield list_a[i:i + chunk_size]

chunk_size = 100
chunked_list = list(split(names, chunk_size))


# names = names[:3]
# print(names)

def get_six_sense_details(name): 

    url = "https://6sense.com/api/technoogies/autosuggestions?searchTerm=" + name
    #print(url)
    company_data = {}

    headers={'User-Agent':'Mozilla/5.i (Windows NT 6.3; Win 64 ; x64) Apple WeKit /537.36(KHTML , like Gecko) Chrome/8i.i.3987.162 Safari/537.36'}
    
    response = requests.request("GET", url, headers=headers)
    
    data = response.json()
    print(len(data))
    
    for i in range(len(data) + 1):
        try:
            if (name.lower() == data['data'][i]['_source']['company_name'].lower()) :
                

        
            #print(data)
                company_data['company_website'] = data['data'][i]['_source']['company_website']
                company_name_seo = data['data'][i]['_source']['company_name_seo']
                company_data['company_name_seo '] = company_name_seo
                company_id = data['data'][i]['_id']
                company_data['company_id '] = company_id
                company_data['company_location_text'] = data['data'][i]['_source']['company_location_text']
                company_data['company_profile_image_url'] = data['data'][i]['_source']['company_profile_image_url']
                company_url = "https://6sense.com/company/" + company_name_seo + "/" + company_id
                company_data['company_url'] = company_url
                #lists.append(company_data)
                print(company_data)

            else :
                    pass
        except Exception as e:
            None

    return company_data

count = 0

#print(get_six_sense_details('0-60 Energy Cafe'))

# with ThreadPoolExecutor(max_workers=8) as executor:
#         results_two = executor.map(get_six_sense_details , names)
#         results_two = [r for r in results_two if r is not None ]


# print(results_two)
for chunk in chunked_list:
    try:
        count+=1
        with ThreadPoolExecutor(max_workers=8) as executor:
            results_two = executor.map(get_six_sense_details , chunk)
            results_two = [r for r in results_two if r is not None ]
        #with open("comp_detailsone.json", "w") as outfile:
        #json.dump(results_two, outfile) 
        filename = str(count)+'.csv'
        with open(filename, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames= results_two[0].keys())
            writer.writeheader()
            writer.writerows(results_two)
    except Exception as e:
         print(e)


end = time.time()
print(end - start)