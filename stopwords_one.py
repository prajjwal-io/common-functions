import os
import logging
from logging import handlers
import sys
from bs4 import BeautifulSoup
#from dotenv import load_dotenv
import datetime as dt
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
stopwords = ['low-profit limited liability company',
'incorporated limited partnership',
 'limited liability partnership',
 'limited liability cooperative',
 'limited liability in shares',
 'limited liability company',
 'professional corporation',
 'professional association',
 'company private limited ',
 'private limited company',
 'ltd. liability company',
 'public limited company',
 'group holdings limited',
 'public company limited',
 'co-orporative limited',
 'limited liability co.',
 'incorporation pte ltd',
 'group company limited',
 'coorporative limited',
 'general partnership',
 'limited partnership',
 'company pvt limited',
 'group incorporation',
 'group, incorporated',
 'group, incorporated',
 'ltd. liability co.',
 'co. incorporation',
 'limited liability',
 'group holding ltd',
 'sendirian berhad',
 'company pvt. ltd',
 'company pte ltd ',
 'company pty ltd ',
 'private limited',
 'limited company',
 'company limited',
 'company pvt ltd',
 '& co-operation',
 'gmbh & co. ohg',
 'private limite',
 '& cooperation',
 'aps & co. k/s',
 'gmbh & co. kg',
 'incorporation',
 'pted. limited',
 'group company',
 'company, inc ',
 'group limited',
 'company, ltd ',
 'group sdn bhd',
 'group limited',
 'limited group',
 'co-operation',
 'co-operative',
 'pvt. limited',
 'pte. limited',
 'prp. limited',
 'pted limited',
 'incorporated',
 'gmbh & co kg',
 'company ltd.',
 'company inc ',
 'company, l.p',
 'company, l.c',
 'group berhad',
 'cooperation',
 'pvt limited',
 'spol s.r.o.',
 'pte limited',
 'prp limited',
 'pty limited',
 'corporation',
 'mbh & co kg',
 'corp., inc.',
 'ag & co. kg',
 'company ltd',
 'group, inc.  , , & co, ltd,   prof assn',
 's. de r.l.',
 's. en n.c.',
 's.a.i.c.a.',
 'sp. z.o.o.',
 'co limited',
 'sp. z o.o.',
 'co pte ltd',
 'bv & co kg',
 'ag & co kg',
 'se & co kg',
 'cia. ltda.',
 '& co group',
 'group, llc',
 'group inc.',
 'group, ltd',
 'group ltd.',
 'pvtlimited',
 'prp. ltd.',
 'pte. ltd.',
 'pvt. ltd.',
 '& co. ltd',
 'gmbh & co',
 'sdn. bhd.',
 'ltd./gte.',
 '& co. ag ',
 '& company',
 'group ltd',
 'group a/s',
 'group plc',
 'group inc',
 'group llc',
 'group ltd',
 'group inc',
 'ltd group',
 'a. en p.',
 'co., inc',
 'kol. srk',
 'kom. srk',
 'plc ltd.',
 'pte. ltd',
 'pted ltd',
 's. en c.',
 'sa de cv',
 'a.m.b.a.',
 's.m.b.a.',
 '& co. ag',
 's.l.n.e.',
 '& co ltd',
 's.c.e i.',
 'co., ltd',
 'ltée/ltd',
 'ptp ltd.',
 'ltd. co.',
 'co, ltd',
 'limited',
 'pte ltd',
 'pty ltd',
 'pvt ltd',
 'pvt ltd',
 'soparfi',
 'limitee',
 's.a.r.l',
 '& co kg',
 'limitée',
 'ptp ltd',
 'co. inc',
 'co ltd',
 'd.n.o.',
 'd.o.o.',
 'gesmbh',
 'j.t.d.',
 'k.d.d.',
 'pc ltd',
 's.c.s.',
 'v.o.s.',
 'l.l.c.',
 's.p.a.',
 'l.3.c.',
 'p.s.c.',
 's.r.o.',
 's.cra.',
 's.coop',
 'p. ltd',
 'co inc',
 'pvtltd  , , inc., ,inc.,   & co.',
 'corp.',
 'd/b/a',
 'ltée.',
 'ltd.,',
 'j.t.d',
 'sp.k.',
 'sp.p.',
 'pgmbh',
 's.c.a',
 's.c.s',
 'sp.j.',
 'd.o.o',
 'ltda.',
 'komag',
 'p.l.c',
 'p ltd',
 'l.l.c',
 'corp',
 'cvoa',
 'e.v.',
 'gmbh',
 'inc.',
 'ka/s',
 'kgaa',
 'ltd.',
 'ltda',
 'pmdn',
 'pty.',
 'ltee',
 'llc.',
 'pllc',
 'bhd.',
 'ent.',
 'lllp',
 'ltée',
 'l.p.',
 'a/s',
 'bhd',
 'i/s',
 'inc',
 'k/s',
 'llc',
 'llp',
 'ltd',
 'p/l',
 'plc',
 's/a',
 'pvt',
 '.co',
 'co.',
 'bhd',
 'p/s',
 'ptp',
 'l.p',
 'l.c',
 'oü',
 'pvt.',
 'private.',
 'limited.',
 'private.limited.',
 'private.limited',
 'private.ltd.',
 'private.ltd',
 'pvt.ltd',
 'pvt.ltd.',
 'private',
 'intl',
 'co',
 'mfg',
 'lab',
 'vfx',
 'ab',
 'cqb',
 'exton',
 'lab,',
 'inc.,',
 'dba:,',
 'dba:',
 'service']


#create a chunk of 1000 records with the split function
def split(list_a, chunk_size):
    for i in range(0, len(list_a), chunk_size):
        yield list_a[i:i + chunk_size]


def create_rotating_log():
    try:
        """
        Creates a rotating log as webfrogs.log
        """
        logger = logging.getLogger('website')
        logger.setLevel(logging.INFO)

        ## Here we define our formatter
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        logHandler = handlers.RotatingFileHandler('website.log', maxBytes=10000000, backupCount=4)
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
data_one = pd.read_csv('csv_one.csv')
records = data_one['name'].tolist()
h = int(len(records)/2)
records = records[:h]

#for second 10 million data
#data_two = pd.read_csv('csv_two.csv')
#records = data_two['name'].tolist()

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

def clear_stopwords(stopwords, name):
  try:
    stopwords = [x.lower() for x in stopwords]
    querywords = name.split()
    resultwords  = [word for word in querywords if word.lower() not in stopwords]
    result = ' '.join(resultwords)
  except Exception as e:
      return name        
  return result


def getGlassdoorWebsite(name):

  
    results={}

    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; rv:91.0) Gecko/20100101 Firefox/91.0'}
    results['InputCompanyName'] = name
    processed_name = clear_stopwords(stopwords, name)
    results['ProcessedCompanyName'] = processed_name
    #print(name)
    #results['ID'] = name['comp_id']

    try:
        url = 'https://www.glassdoor.co.in/api-web/employer/find.htm?autocomplete=true&maxEmployersForAutocomplete=10&term=%s&gdToken=undefined'%(processed_name)
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
            results['InputDomain'] = extract_url(results['WebsiteDomain'])[1]
            results['IsPublic'] = extract_url(results['WebsiteDomain'])[3]
        except:
            results['WebsiteDomain'] = None  
            results['GlassDoorName'] = None
            results['Source'] = None
            results['InputPrimaryDomain'] = None
            results['InputDomain'] = None
            results['IsPublic'] = None
    except Exception as e:
        #print(e)   
        results['WebsiteDomain'] = None
        results['GlassDoorName'] = None
        results['Source'] = None
        results['InputPrimaryDomain'] = None
        results['InputDomain'] = None
        results['IsPublic'] = None
        
    return results 

def getClearbitWebsite(name):

    
    results={}
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; rv:91.0) Gecko/20100101 Firefox/91.0'}
    results['InputCompanyName'] = name
    processed_name = clear_stopwords(stopwords, name)
    results['ProcessedCompanyName'] = processed_name
    #print(name)

    #results['ID'] = name['comp_id']
    try:
        url = 'https://autocomplete.clearbit.com/v1/companies/suggest?query=%s'%(processed_name)
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
            results['InputDomain'] = extract_url(results['WebsiteDomain'])[1]
            results['IsPublic'] = extract_url(results['WebsiteDomain'])[3]
        except:
            results['WebsiteDomain'] = None  
            results['ClearbitName'] = None
            results['Source'] = None
            results['InputPrimaryDomain'] = None
            results['InputDomain'] = None
            results['IsPublic'] = None
    except Exception as e:   
        results['WebsiteDomain'] = None
        results['ClearbitName'] = None
        results['Source'] = None
        results['InputPrimaryDomain'] = None
        results['InputDomain'] = None
        results['IsPublic'] = None
    return results        


def getWebsiteUrl(name):
    '''
    fetches website url from name
    
    '''
    result = {} 
    result['InputCompanyName'] = name
    #result['ID'] = name['comp_id']
    
    try:
        glassdoor = getGlassdoorWebsite(name=name)
        #print(glassdoor)
        if(glassdoor['GlassDoorName'] is not None and (glassdoor['ProcessedCompanyName'].lower()== clear_stopwords(stopwords, glassdoor['GlassDoorName'].lower()))):
            result['Website'] = parse_url(glassdoor['WebsiteDomain'])
            result['APICompanyName'] = glassdoor['GlassDoorName']
            result['Source'] = glassdoor['Source']
            result['Primary Domain'] = glassdoor['InputPrimaryDomain']
            result['Input Domain'] = glassdoor['InputDomain']
            result['IsPublic'] = glassdoor['IsPublic']
        else:
            clearbit = getClearbitWebsite(name=name)
            #rint(clearbit)
            if(clearbit['ClearbitName'] is not None and (clearbit['ProcessedCompanyName'].lower()== clear_stopwords(stopwords, clearbit['ClearbitName'].lower())) ):
                result['Website'] = parse_url(clearbit['WebsiteDomain'])
                result['APICompanyName'] = clearbit['ClearbitName'] 
                result['Source'] = clearbit['Source']
                result['Primary Domain'] = clearbit['InputPrimaryDomain']
                result['Input Domain'] = clearbit['InputDomain']
                result['IsPublic'] = clearbit['IsPublic']  
            else:
                result['InputCompanyName'] = name
                #result['ID'] = name['comp_id']
                result['APICompanyName'] = None
                result['Website'] = None 
                result['Source'] = None
                result['Primary Domain'] = None
                result['Input Domain'] = None
                result['IsPublic'] = None
    except Exception as e: 
        log_error(e)
        result['InputCompanyName'] = name
        #result['ID'] = name['comp_id']
        result['APICompanyName'] = None
        result['Website'] = None
        result['Source'] = None
        result['Primary Domain'] = None
        result['Input Domain'] = None
        result['IsPublic'] = None
    #print(result)    
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

# o4 = time.time() - start
# print(o4)
#print(getWebsiteUrl('SHARP COATING PVT LTD'))