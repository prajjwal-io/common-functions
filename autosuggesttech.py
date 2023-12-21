import requests
import json
import requests
import csv
import time
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import itertools

start = time.time()

df = pd.read_csv("SlintelTech.csv")

tech_name = df["TechnologyName"].to_list()

#print(len(tech_name))

#print(len(names))


def split(list_a, chunk_size):
    for i in range(0, len(list_a), chunk_size):
        yield list_a[i:i + chunk_size]

chunk_size = 100
chunked_list = list(split(tech_name, chunk_size))


# # names = names[:3]
# # print(names)
name = tech_name[0]
print(name)
def get_six_sense_details(name): 

    url = "https://6sense.com/api/technologies/autosuggestions?searchTerm=" + name
    #print(url)
    #tech_data = {}

    headers={'User-Agent':'Mozilla/5.i (Windows NT 6.3; Win 64 ; x64) Apple WeKit /537.36(KHTML , like Gecko) Chrome/8i.i.3987.162 Safari/537.36'}
    
    response = requests.get(url, headers=headers)
    data = response.json()
    tech_data = {}        

    #print(len(data))
    for i in range(len(data)):
        try:
            if (name.lower() == data[i]['_source']['company_name'].lower()) :
                print(data[i])
                company_name = data[i]['_source']['company_name']
                tech_data['Index'] = data[i]['_index']
                tech_data['ID'] = data[i]['_id']
                tech_data['Type'] = data[i]['_type']
                tech_data['Sort'] = data[i]['sort']
                tech_data['CompanyName '] = company_name
                tech_data['Category'] = data[i]['_source']['category']
                tech_data['MetaSeoURL'] = data[i]['_source']['meta_seo_url']
                tech_data['CatMetaSeoURL'] = data[i]['_source']['cat_meta_seo_url']
                tech_data['SubCatMetaSeoURL'] = data[i]['_source']['sub_cat_meta_seo_url']
                tech_data['Subcategory'] = data[i]['_source']['subcategory']
                tech_data['SubcategoryLabel'] = data[i]['_source']['subcategory_label']
                tech_data['Type'] = data[i]['_source']['type']
                tech_data['Domains'] = data[i]['_source']['domains']
                tech_data['AdeedThisMonth'] = data[i]['_source']['adds_this_month']
                tech_data['DeletedThisMonth'] = data[i]['_source']['deleted_this_month']
                tech_data['TechMetaSEOURL'] = data[i]['_source']['tech_meta_seo_url']
                tech_data['TechCleanedURL'] = data[i]['_source']['tech_cleaned_url']
                tech_data['Descripion'] = data[i]['_source']['description']
                tech_data['ComapanyNameLabel '] = data[i]['_source']['company_name_label']
                tech_data['SubCategoryLabelCompletion'] = data[i]['_source']['subcategory_label_completion']
                tech_data['CompanyProfileImageUrl'] = data[i]['_source']['company_profile_image_url']
                #lists.append(tech_data)
                #print(tech_data)             
        except Exception as e:
            print(e)
    return tech_data



get_six_sense_details(name)


#print(get_six_sense_details('0-60 Energy Cafe'))

# with ThreadPoolExecutor(max_workers=8) as executor:
#         results_two = executor.map(get_six_sense_details , names)
#         results_two = [r for r in results_two if r is not None ]


# print(results_two)
# count = 0

# for chunk in chunked_list:
#     try:
#         count+=1
#         with ThreadPoolExecutor(max_workers=8) as executor:
#             results_two = executor.map(get_six_sense_details , chunk)
#             results_two = [r for r in results_two if r is not None ]
#         #with open("comp_detailsone.json", "w") as outfile:
#         #json.dump(results_two, outfile) 
#         filename = str(count)+'.csv'
#         with open(filename, 'w', encoding='utf-8', newline='') as f:
#             writer = csv.DictWriter(f, fieldnames= results_two[0].keys())
#             writer.writeheader()
#             writer.writerows(results_two)
#     except Exception as e:
#          print(e)


end = time.time()
print(end - start)