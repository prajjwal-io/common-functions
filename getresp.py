import requests
import tldextract
import urllib3
from concurrent.futures import ThreadPoolExecutor
import itertools
import pandas as pd
import csv


def split(list_a, chunk_size):
    for i in range(0, len(list_a), chunk_size):
        yield list_a[i:i + chunk_size]

# read DataFrame
data = pd.read_csv("alldomain_1.csv")
records = data['domain'].tolist()

# h = int(len(records)/2)
# records = records[:h]

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

def get_response(domain):


    url = parse_url(domain)
    input_url,input_domain,input_primary_domain,inputispublic = extract_url(url)


    try: 
        result = {}
        response = requests.get(url,allow_redirects=True,timeout=10,verify=False,headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36"})
        history = response.history

        input_response = history[0].status_code
        input_url_1 = history[0].url
        input_url,input_domain,input_primary_domain,inputispublic = extract_url(input_url_1)

        #print(input_response,input_url_1)
        result["input_response"] = input_response
        result["input_url"] = input_url_1
        result["input_domain"] = input_domain
        result["input_primary_domain"] = input_primary_domain
        result["inputispublic"] = inputispublic

        final_response = response.status_code
        final_url_1 = response.url
        #print(final_response,final_url_1)
        final_url,final_domain,final_primary_domain,finalispublic = extract_url(final_url_1)
        result["final_response"] = final_response
        result["final_url"] = final_url_1
        result["final_domain"] = final_domain
        result["final_primary_domain"] = final_primary_domain
        result["finalispublic"] = finalispublic

    except Exception as e:         
        result["input_response"] = None
        result["input_url"] = input_url
        result["input_domain"] = input_domain
        result["input_primary_domain"] = input_primary_domain
        result["inputispublic"] = inputispublic

        result["final_response"] = None
        result["final_url"] = None
        result["final_domain"] = None
        result["final_primary_domain"] = None
        result["finalispublic"] = None
    return result


count = 0

for chunk in chunked_list:
    count+=1
    with ThreadPoolExecutor(max_workers=8) as executor:
        results_two = executor.map(get_response , records)
        results_two = [r for r in results_two if r is not None ]
    #with open("comp_detailsone.json", "w") as outfile:
    #json.dump(results_two, outfile) 
    filename = str(count)+'.csv'
    with open(filename, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames= results_two[0].keys())
        writer.writeheader()
        writer.writerows(results_two)


# with open('response.csv', 'w', encoding='utf-8', newline='') as f:
#     writer = csv.DictWriter(f, fieldnames= results_two[0].keys())
#     writer.writeheader()
#     writer.writerows(results_two)

#print(get_response("oceanfrogs.com"))
