#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Aug 26 12:18:36 2020

@author: anuj
"""

from multiprocessing import Pool
import re
import pandas as pd
import requests
from bs4 import BeautifulSoup
import urllib.request as urll
import matplotlib.pyplot as plt 
import numpy as np
import yfinance as yf

all_stocks = pd.read_csv('EQUITY_L.csv').set_index(('SYMBOL'))
all_stocks['COMPANY']=all_stocks['COMPANY'].str.lower()
def scrape_keywords(Base_url):
    page = requests.get(Base_url)
    soup=BeautifulSoup(page.content, 'html.parser')
    h=soup.find('head')
    meta=h.find('meta',attrs={"name":"Keywords"})
    return (meta['content'].split(', '))

def check_in_db(key):
    if(key in all_stocks.index):
        return key
    ret = (all_stocks.index[all_stocks['COMPANY'] == key])
    if len(ret)==0:
        if(key[-3:]=='ltd'):
            key = key[:-3]+'limited'
        ret = (all_stocks.index[all_stocks['COMPANY'] == key])
    return None if (len(ret)==0) else ret[0]
        
def check_if_stock(key):
    key = key.upper()
    ret = check_in_db(key)
    if ret:
        return ret
    default="https://economictimes.indiatimes.com/topic/"
    Base_url=default+key
    r=requests.get(Base_url, allow_redirects=True)
    url=str(r.url)
    ret=None
    if(url[-4:]=='.cms'):
        ret= url[len(default)-6:url.index('/',len(default)-6)]
    ret = None if (not ret) else check_in_db(ret.replace('-',' '))
    if ret:
        return ret
    Base_url=default+key+' company'
    r=requests.get(Base_url, allow_redirects=True)
    url=str(r.url)
    if(url[-4:]=='.cms'):
        ret= url[len(default)-6:url.index('/',len(default)-6)]
    if(ret and check_in_db(ret.replace('-',' '))):
        return check_in_db(ret.replace('-',' '))
    if(ret):
        ret=(ret[:-3]+'limited').replace('-',' ')
    return None if not ret else check_in_db(ret.replace('-',' '))
def get_stocks(keys):
    pool = Pool(4)
    ret = pool.map(check_if_stock, keys)
    pool.close()
    return [i+'.NS' for i in ret if i]
def scrape_research_page (pg='page-1'):
    Base_url = "https://www.moneycontrol.com/news/business/moneycontrol-research/"+pg#    Base_url = "https://www.moneycontrol.com/news/tags/recommendations.html"+pg
    page = requests.get(Base_url)
    soup=BeautifulSoup(page.content, 'html.parser')
    data1 = soup.find(id="cagetory")
    data2= data1.find_all('li')
    list_dates,list_recos,list_titles=[],[],[]
    print(len(data2))
    for i, article in enumerate(data2[:2]):
        a=article.find('a')
        if(a==None):
            continue
        dt=article.select_one("span").text
        title=a['title']
        link=a['href']
        print(dt,'\n',title)
        list_dates.append(dt)
#        list_dates.append(dt[:-4])
        list_titles.append(title)
        keywords=scrape_keywords(link)
        stock=[]
#        print(keywords)
        list_recos.append(get_stocks(keywords))
    dictionary={'dates':list_dates,'recos':list_recos,'titles':list_titles}
    df = ( pd.DataFrame(dictionary))
    df['dates'] = pd.to_datetime(df['dates'])
    return df
def plot_action(df, interval = '5m', period = '1w'):
    df.to_csv('recos.csv')
    for i in df.index:
        for j in df['recos'][i]:
            data = yf.download(j, start = df['dates'].dt.date[i], interval = interval, period = period)
            data['Close'] = 100*data['Close']/data['Close'][data.index>=df['dates'][i]].iloc[0]
            plt.plot(data['Close'])
            plt.legend([j])
            plt.axvline(x = df['dates'][i], color='black')
            plt.title(df['titles'][i])
            plt.show()
def main():
    df = scrape_research_page()
    plot_action(df,  interval = '5m', period = '1w')
main()
