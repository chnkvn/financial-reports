import json
import re
from datetime import datetime
from itertools import chain
from typing import Iterable
import asyncio
import requests
import streamlit as st
from attrs import define, field
from bs4 import BeautifulSoup
from bs4.element import Tag
from icecream import ic

DATE_FORMAT = '%Y-%m-%d'


def date_to_str(date:datetime)-> str:
    if isinstance(date, datetime):
        return date.strftime(DATE_FORMAT)
    return date
def replace_stringify_date_objects_iterable(iterable: Iterable) -> Iterable:
    if isinstance(iterable,dict):
        return {key: (date_to_str(iterable[key]) if isinstance(iterable[key],datetime) else iterable[key]) for key in iterable}
    else:
        iterable = [date_to_str(x) if isinstance(x, datetime) else x for x in iter()]
        if isinstance(iterable, tuple):
            return tuple(iterable)
        elif isinstance(iterable, set):
            return set(iterable)
        return iterable
                
@define
class Asset:
    asset: str
    isin: str
    symbol: str
    currency: str
    name:str
    latest: float = field(converter=float)
    variation: float
    tradeDate: str = field(repr=date_to_str)
    url: str
    referenceIndex: str 
    morningstarCategory: str
    assetsComposition: dict
    #sectors: list
    lastDividende: dict = field(repr=replace_stringify_date_objects_iterable)
    
    def __hash__(self):
        return hash(self.isin)
    
    def __eq__(self, other):
        if not isinstance(other, Asset):
            # only equality tests to other `structure` instances are supported
            return NotImplemented
        return self.isin == other.isin

    @classmethod
    def from_boursorama(cls, data:dict):

        return cls(
            data['asset'],
            data['isin'],
            data['symbol'],
            data['currency'],
            data['name'],
            data['latest'],
            data['variation'],
            data['tradeDate'],
            data['url'],
            data['referenceIndex'],
            data['morningstarCategory'],
            data['assetsComposition'],
            #data['sectors'],
            data['lastDividende'])

def unicode_escape(s:str) -> str:
    """Remove unicode sequences from a string s"""
    return s.encode('utf8').decode('unicode_escape')

def extract_chart_data(soup:BeautifulSoup, id_:str ) -> dict:
    """Extract amChartData key from source code"""
    try:
        # Find the id in source code
        portfolio = soup.find_all('div', id=id_)
        # Extract the tags, remove NavigableString objects
        tags = list(chain.from_iterable(unicode_escape(x.get_text()).strip().split('\n')
                                        for x in portfolio[0].parent.contents if isinstance(x, Tag)))
        am_chart_data = [tag for tag in tags if '"amChartData"' in tag].pop()
        # Extract content
        return json.loads('{'+re.search(r'"amChartData":\[\{"name":.+\]\}',
                                        am_chart_data).group())['amChartData']
    except IndexError as e:
        return None

@st.cache_data
def get_current_asset_data(asset:str) -> dict:
    """From an ISIN or a asset name, returns a dictionary containing:
    - its symbol on boursorama.com
    - the latest price of the eassety
    - its daily variation
    - its financial exchange place code
    - its trade Date
    - store the url in a new key"""
    if asset.startswith('https://'):
        r = requests.get(asset)
    else:
        asset = asset.replace(' ', '%20')
        r = requests.get(f'https://www.boursorama.com/recherche/{asset}/')
    url_split = r.url.split('/')
    soup = BeautifulSoup(json.dumps(r.content.decode("utf-8")), "lxml").body
    data = {}
    try:
        symbol = url_split[-2]
        relevant_tag = next(iter(soup.select(f'div[data-faceplate-symbol*="{symbol}"]')))
        name = relevant_tag.select('a[title*="Cours"]')[0].get_text().replace('\\n', '').strip()
        # Remove irrelevent starting characters, and complete the string if data-ist-init is a class
        data = json.loads(relevant_tag['data-ist-init'].replace('\\"', '')+'"}') if relevant_tag.has_attr('data-ist-init') else {}
        if len(data) == 0:
            date_ = relevant_tag.find_all('div', class_='\\"c-faceplate__real-time\\"')[0]
            data['symbol'] = symbol
            data['tradeDate'] = datetime.strptime(re.search(r'[0-3][0-9]/[01][0-9]/[0-9]{4}', date_.get_text()).group(),'%Y-%m-%d')
        else:
            data['tradeDate'] = datetime.strptime(data['tradeDate'], '%Y-%m-%d')
        data['variation'] = relevant_tag.select('span[c-instrument--variation]')[0].get_text()
        data['latest'] = relevant_tag.select('span[c-instrument--last]')[0].get_text().replace(' ', '')
        data['isin'] = relevant_tag.find_all('h2', class_='\\"c-faceplate__isin\\"')[0].get_text().split(' ')[0]

        data['asset'] = url_split[url_split.index('cours')-1] if 'bourse' in r.url else 'Actions'
        data['name'] = unicode_escape(name)
        data['url'] = r.url
        data['currency'] = relevant_tag.find_all('span', class_ = '\\"c-faceplate__price-currency\\"').pop().get_text().strip()
        map_attributes = {
            'indice de référence':'referenceIndex',
            'catégorie morningstar': 'morningstarCategory',
            'amChartData': 'AssetsComposition'
        }
        # Init entries with null values
        for v in map_attributes.values():
            data[v] = None
        if 'bourse' in r.url:
            additional_info = soup.find_all('p', class_='\\"c-list-info__heading')
            for i in additional_info:
                attr = map_attributes.get(unicode_escape(i.get_text()).strip().lower(), unicode_escape(i.get_text()).strip())
                for v in i.next_siblings:
                    v= unicode_escape(v.get_text()).strip()
                    if '?' in v or len(v)<1:
                        continue
                    elif data.get(attr, None) is None :
                        data[attr]  =  v
                    elif isinstance(data[attr], str):
                        data[attr] = [data[attr]] + [v]
                    else:
                        data[attr].append(v)

        # Composition
        url_split.insert(-2, 'composition')
        composition_request = requests.get('/'.join(url_split))
        if composition_request.status_code == 200:
            soup = BeautifulSoup(json.dumps(composition_request.content.decode("utf-8")), "lxml").body
            data['assetsComposition'] = extract_chart_data(soup,'\\"portfolio\\"' )
            #data['sectors'] = extract_chart_data(soup,'\\"sector\\"' )
        else:
            data['assetsComposition'] = [{"name": data['asset'], 'value': 100  }]
            #data['sectors'] = [{'name': unicode_escape([link for link in soup.select('a[c-list-info__value]')][0].get_text()),
             #                  'value':100}]
        last_dividende = soup.find_all('p', string=re.compile('dernier dividende'))
        data['lastDividende'] = {}
        if len(last_dividende) >1:
            amount = False
            for p_tag in last_dividende:
                for sibling in p_tag.next_siblings:
                    if isinstance(sibling, Tag) and sibling.name=='p':
                        if sibling.attrs['class'] in [['\\"c-list-info__value'], '\\"c-list-info__value']:
                            if not amount:
                                data['lastDividende']['amount'] = unicode_escape(sibling.get_text()).strip()
                                amount = True
                                continue
                            try:
                                data['lastDividende']['date'] = datetime.strptime(unicode_escape(sibling.get_text()).strip(), '%d.%m.%y')
                                ic(data['lastDividende']['date'])
                            except ValueError as e:
                                data['lastDividende']['date'] = unicode_escape(sibling.get_text()).strip()
                            
        data = {k:(v.strip() if isinstance(v, str) else v) for k,v in data.items()}
        return data
    except StopIteration as e:
        raise ValueError('No asset found. Try with another name or the ISIN of your asset.')


def get_historical_data(bourso_ticker:str):
    req = requests.get(f'https://www.boursorama.com/bourse/action/graph/ws/GetTicksEOD?symbol={bourso_ticker}&length=7300&period=0')
    return req.json()['d']['QuoteTab']

if __name__ == '__main__':
    air_liquide = ['air liquide', 'FR0000120073']
    lvmh = ['mc', 'lvmh', 'FR0000121014']
    items = {Asset.from_boursorama(get_current_asset_data(asset)) for asset in air_liquide+lvmh}
    ic(items, len(items))
