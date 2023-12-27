import json 
import re
import requests 
import io
import pandas as pd
import xml.etree.ElementTree as ET 
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib.request import urlopen, urlparse

websites = pd.read_csv('furniture stores pages.csv').to_numpy()

websites_100 = websites[0:100]

products = []
title = []
# description = []
price = []
ws = []

def get_robots(url):
    parse = urlparse(url)
    domain = parse.netloc
    print(domain)
    scheme = parse.scheme
    url = scheme + '://' + domain + '/robots.txt'
    session = requests.Session()
    retry = Retry(connect=3, backoff_factor=0.5)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    sitemap_url = ''
    sitemap_ls = []
    try:
        with urlopen(url) as stream:
            for line in urlopen(url).read().decode("utf-8").split('\n'):
                if 'Sitemap'.lower() in line.lower():
                    sitemap_url = re.findall(r' (https.*xml)', line)[0]
                    sitemap_ls.append(sitemap_url)
        return list(set(sitemap_ls))
    except:
        return []

def get_html_from_subchild(subchild):
    loc_content = requests.get(subchild.text).text
    loc_root = re.findall("<loc>(.*?)</loc>", loc_content)

    for loc_child in loc_root:
        res = requests.get(loc_child + '.js')
        if res.status_code == 200:
            try:
                products.append(loc_child + '.js')
                ws.append(urlparse(loc_child).netloc)
                data = json.loads(res.text)
                title.append(data['title'])
                # description.append(data['description'])
                price.append(data['price'])
                # print(data['title'])
            except:
                print('No json')
                continue
        else:
            print('No website')

# Parse robots.txt for sitemaps and disallowed pages
for website in websites_100: 
    url = website[0]
    sitemaps = get_robots(url)
    for sitemap in sitemaps:
        try:
            sitemap_content = requests.get(sitemap).text
            f = io.StringIO(sitemap_content)
            tree = ET.parse(f)
            root = tree.getroot()
            for child in root:
                for subchild in child:
                    if 'loc' in subchild.tag:
                        if 'sitemap_products' in subchild.text:
                            # print(subchild.text)
                            data = get_html_from_subchild(subchild)            
        except:
            print('Sitemap does not exist')
            continue
    print('------------------')

df = pd.DataFrame(list(zip(ws, products, title, price)), columns =['Website', 'Product', 'Title', 'Price'])
df.to_csv('products.csv', index=False)
    