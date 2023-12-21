import requests
import itertools
import time
import csv
from bs4 import BeautifulSoup
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import re
import urllib
import json


start = time.time()

def get_comp_info (url):

    headers={'User-Agent':'Mozilla/5.0 (Windows NT 6.3; Win 64 ; x64) Apple WeKit /537.36(KHTML , like Gecko) Chrome/80.0.3987.162 Safari/537.36'}

    webpage = requests.get( url, headers = headers).text
    soup = BeautifulSoup(webpage,'lxml')
    website_object = {}
    try: 

        website_object['company_name'] = soup.select_one('div.company_title.d-flex h1').text.split()[0]
        website_object['company_log_url'] = soup.select_one('div.company_title.d-flex img')['src']
       
        website_object['company_industry'] =  soup.select_one('div.company_desc_section ul li').text
        website_object['company_headquarters'] =  soup.select_one('div.media_body p').text.replace("\n", "").strip()
        website_object['company_linkedin_link'] =  soup.select_one('img.social_media_icons').get('onclick').strip("window.open()").split(",")[0].replace("'", "")
        website_object['company_size'] =  soup.select('div.d-flex.align-items-start p.sl_subHeader')[3].text.replace("\n", "").strip()
        website_object['company_stock_symbol'] =  soup.select('div.d-flex.align-items-start p.sl_subHeader')[4].text.replace("\n", "").strip()
        website_object['company_website'] =  soup.select('div.d-flex.align-items-start p.sl_subHeader')[5].text.replace("\n", "").strip()
        website_object['Type'] =  soup.select('div.d-flex.align-items-start p.sl_subHeader')[6].text.replace("\n", "").strip()
    except Exception as e :
        print(e)

    try:
        website_object['company_desc'] = soup.select_one('div.company_desc_section p').text.replace("\n", "") 
        part2 = soup.select_one('div.company_desc_section #desc-more').text.replace("\n", "")
        if part2 == None :
            pass
        else :
            website_object['company_desc'] += part2

    except Exception as e :
        print(e)

    return website_object


print(get_comp_info("https://6sense.com/company/idkoda/5ba6107b7c86660d3c69f237"))

print(time.time() - start)