import requests
import itertools
import time
import csv
from bs4 import BeautifulSoup
from datetime import datetime
from slugify import slugify
from concurrent.futures import ThreadPoolExecutor
import re
import urllib
import json
import sys 

url = "https://6sense.com/company/idkoda/5ba6107b7c86660d3c69f237"


start = time.time()

def getJson(url) :

    headers={'User-Agent':'Mozilla/5.0 (Windows NT 6.3; Win 64 ; x64) Apple WeKit /537.36(KHTML , like Gecko) Chrome/80.0.3987.162 Safari/537.36'}

    webpage = requests.get( url, headers = headers).text
    soup = BeautifulSoup(webpage,'lxml')
    script = soup.select('body script')[-2].text
    result = re.search('decodeURIComponent((.*));' , script)[1]
    result = result.strip("()").strip('""')
    data = urllib.parse.unquote_plus(result, encoding='utf-8', errors='replace')
    data = json.loads(data)
    return data

#print(getJson(url))

def get_comp_tech(url):
    try:
        base_url_tech = "https://6sense.com/tech/"

        data = getJson(url)
        techs= []
        categ = data["technologies_mapper_view"]["categories"].keys()
        #tech_cats = [data["technologies_mapper_view"]["categories"][cat] for cat in data["technologies_mapper_view"]["categories"].keys()]
        #print(tech_cats)
        for cat in categ:
            tech_cat = data["technologies_mapper_view"]["categories"][cat]
        #for tech_cat in tech_cats:
            for tech in tech_cat:
                tech_info ={} 
                tech_info["TechnologyCategory"] = cat
                tech_info['TechnologyName'] = tech
                tech_info['TechnologyId'] =  tech_cat[tech]["id"]
                tech_info['MetaSEOUrl'] = tech_cat[tech]["meta_seo_url"]
                tech_info['Subcategory'] =  tech_cat[tech]["subcategory"]
                tech_info['SubCatMetaSeoURL'] = tech_cat[tech]["sub_cat_meta_seo_url"]
                tech_info['TechnologyLogoURL'] = tech_cat[tech]["img"]
                tech_info['TechnologyURL'] = base_url_tech + tech_info['SubCatMetaSeoURL'] + "/" + tech_info['MetaSEOUrl']
                tech_info['TechnologyDescription'] = tech_cat[tech]["desc"]
                techs.append(tech_info)
                #print(tech_info)

    except Exception as e:
        print(e)

    return techs


techs = get_comp_tech(url)
print(techs)

# with open('data4.json', 'w', newline="") as f:
#     json.dump(data, f)
# s = 'asdf=5;iwantthis123jasd'
# result = re.search('asdf=5;(.*)123jasd', s)