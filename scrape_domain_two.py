import os
import logging
import sys
import datetime as dt
from logging import handlers
from bs4 import BeautifulSoup
#from dotenv import load_dotenv
import pandas as pd
#import pypyodbc
import tldextract
import json
import requests
from urllib.request import urlopen
#from fake_useragent import UserAgent
#from geopy import Nominatim
import urllib3
from concurrent.futures import ThreadPoolExecutor
import itertools
import time
import csv

#start = time.time()


#create a chunk of 1000 records with the split function
def split(list_a, chunk_size):
    for i in range(0, len(list_a), chunk_size):
        yield list_a[i:i + chunk_size]



def create_rotating_log():
    try:
        """
        Creates a rotating log as website.log
        """
        logger = logging.getLogger('website')
        logger.setLevel(logging.INFO)

        ## Here we define our formatter
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        logHandler = handlers.RotatingFileHandler('website_three.log', maxBytes=10000000, backupCount=4)
        logHandler.setLevel(logging.INFO)
        ## Here we set our logHandler's formatter
        logHandler.setFormatter(formatter)

        logger.addHandler(logHandler)
    except Exception as e:
            logger = logging.getLogger('website')
            logger.setLevel(logging.INFO)
            log_error(e)

    return logger

def log_error(e):
    exception_message = str(e)
    exception_type, exception_object, exception_traceback = sys.exc_info()
    filename = os.path.split(exception_traceback.tb_frame.f_code.co_filename)[1]
    logger.error(f"{exception_message} {exception_type} {filename}, Line {exception_traceback.tb_lineno} "+ str(dt.datetime.now()))


logger = create_rotating_log()


#taking inputs from data

# data = pd.read_csv('name_domain.csv', header= None)
#data.columns = ['comp_id', 'comp_name']

#for first 10 miliion data
# data_one = pd.read_csv('csv_one.csv')
# records = data_one['name'].tolist()


#for second 10 million data
data_two = pd.read_csv('csv_two.csv')
records = data_two['name'].tolist()
h = int(len(records)/2)
records = records[:h]

chunk_size = 1000
chunked_list = list(split(records, chunk_size))


def parse_url(url):  
    try:
        url=url.strip()
        if url[:5]=="//www":
            url= url[2:]
        if url[:5]=='/www.':
            url= url[1:]
        if url[:5] == "https":
            url = "http" + url[5:]
        elif url[:3] == "www":
            url = "http://" + url
        if url[:4] == 'http' and url[:10]!='http://www':
            url = "http://" + url[7:] 
        if url[:4]!="http":
            url= "http://"+url    
        url = url[:11] + url[11:].split('/')[0]
        url = url.split('?')[0]    
        return url
    except Exception as e:
        pass
        return url          

    
def parse_url(url):  
    try:
        url=url.strip()
        if url[:5]=="//www":
            url= url[2:]
        if url[:5]=='/www.':
            url= url[1:]
        if url[:5] == "https":
            url = "http" + url[5:]
        elif url[:3] == "www":
            url = "http://" + url
        if url[:4] == 'http' and url[:10]!='http://www':
            url = "http://" + url[7:] 
        if url[:4]!="http":
            url= "http://"+url    
        url = url[:11] + url[11:].split('/')[0]
        url = url.split('?')[0]    
        return url
    except Exception as e:
        pass
        return url          

    
def extract_url(url):
    input_url,input_domain,input_primary_domain,ispublic = None,None,None,True
    try:
        url = parse_url(url)

        #input url given by user with http://www. structure
        input_url = url
        
        ext = tldextract.extract(url)
        pvt_ext = tldextract.extract(url, include_psl_private_domains=True)

        #chech if private or public
        if (pvt_ext.suffix != ext.suffix):
            ispublic = False

        #input domain without https or www structure
        input_domain = ('.'.join(filter(None,ext[:3]))).replace('www.','')

        #primary input domain without subdomain
        input_primary_domain = ext.registered_domain    
    except Exception as e:
        pass   

    return input_url,input_domain,input_primary_domain,ispublic

def getGlassdoorWebsite(name):
    results={}
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; rv:91.0) Gecko/20100101 Firefox/91.0'}
    results['InputCompanyName'] = name
    #results['ID'] = name['comp_id']

    try:
        url = 'https://www.glassdoor.co.in/api-web/employer/find.htm?autocomplete=true&maxEmployersForAutocomplete=10&term=%s&gdToken=undefined'%(name)
        response = requests.get(url,headers=headers)
        status_code = response.status_code
        logger.info("status code - "+ str(status_code))
        #print(response)
        json_object = response.json()
        try:
            results['WebsiteDomain'] = json_object[0]['website']
            results['GlassDoorName'] = json_object[0]['label']
            results['Source'] = 'Glassdoor'
            results['InputPrimaryDomain'] = extract_url(results['WebsiteDomain'])[2]
            results['IsPublic'] = extract_url(results['WebsiteDomain'])[3]
        except:
            results['WebsiteDomain'] = None  
            results['GlassDoorName'] = None
            results['Source'] = None
            results['InputPrimaryDomain'] = None
            results['IsPublic'] = None
    except Exception as e:
        #print(e)   
        results['WebsiteDomain'] = None
        results['GlassDoorName'] = None
        results['Source'] = None
        results['InputPrimaryDomain'] = None
        results['IsPublic'] = None
        
    return results 

def getClearbitWebsite(name):
    results={}
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; rv:91.0) Gecko/20100101 Firefox/91.0'}
    results['InputCompanyName'] = name
    #results['ID'] = name['comp_id']
    try:
        url = 'https://autocomplete.clearbit.com/v1/companies/suggest?query=%s'%(name)
        response = requests.get(url,headers=headers)
        status_code = response.status_code
        logger.info("status code - "+ str(status_code))
        #print(response)
        json_object = response.json()
        try:
            results['WebsiteDomain'] = json_object[0]['domain']
            results['ClearbitName'] = json_object[0]['name']
            results['Source'] = 'Clearbit'
            results['InputPrimaryDomain'] = extract_url(results['WebsiteDomain'])[2]
            results['IsPublic'] = extract_url(results['WebsiteDomain'])[3]
        except:
            results['WebsiteDomain'] = None  
            results['ClearbitName'] = None
            results['Source'] = None
            results['InputPrimaryDomain'] = None
            results['IsPublic'] = None
    except Exception as e:   
        results['WebsiteDomain'] = None
        results['ClearbitName'] = None
        results['Source'] = None
        results['InputPrimaryDomain'] = None
        results['IsPublic'] = None
    return results        


def getWebsiteUrl(name):
    '''
    fetches website url from name
    
    '''
    result = {} 
    result['InputCompanyName'] = name
    #result['ID'] = name['comp_id']
    glassdoor = getGlassdoorWebsite(name=name)
    try:
        if(glassdoor['InputCompanyName'].lower()==glassdoor['GlassDoorName'].lower()):
            result['Website'] = parse_url(glassdoor['WebsiteDomain'])
            result['APICompanyName'] = glassdoor['GlassDoorName']
            result['Source'] = glassdoor['Source']
            result['Primary Domain'] = glassdoor['InputPrimaryDomain']
            result['IsPublic'] = glassdoor['IsPublic']
        else:
            clearbit = getClearbitWebsite(name=name)
            if(clearbit['InputCompanyName'].lower()==clearbit['ClearbitName'].lower()):
                result['Website'] = parse_url(clearbit['WebsiteDomain'])
                result['APICompanyName'] = clearbit['ClearbitName'] 
                result['Source'] = clearbit['Source']
                result['Primary Domain'] = clearbit['InputPrimaryDomain']
                result['IsPublic'] = clearbit['IsPublic']  
            else:
                result['InputCompanyName'] = name
                #result['ID'] = name['comp_id']
                result['APICompanyName'] = None
                result['Website'] = None 
                result['Source'] = None
                result['Primary Domain'] = None
                result['IsPublic'] = None
    except Exception as e: 
        result['InputCompanyName'] = name
        #result['ID'] = name['comp_id']
        result['APICompanyName'] = None
        result['Website'] = None
        result['Source'] = None
        result['Primary Domain'] = None
        result['IsPublic'] = None
    return result     

count = 0

for chunk in chunked_list:
    count+=1
    with ThreadPoolExecutor(max_workers=8) as executor:
        results_two = executor.map(getWebsiteUrl , chunk)
        results_two = [r for r in results_two if r is not None ]
    #with open("comp_detailsone.json", "w") as outfile:
    #json.dump(results_two, outfile) 
    filename = str(count)+'.csv'
    with open(filename, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames= results_two[0].keys())
        writer.writeheader()
        writer.writerows(results_two)

#o4 = time.time() - start
#print(o4)
