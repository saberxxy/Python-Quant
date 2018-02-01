#-*- coding=utf-8 -*-
# 获取基金数据

from selenium import webdriver
import time
from bs4 import BeautifulSoup
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
import re
import urllib
import urllib.request
import os
import xml.sax
import pandas as pd

url = "http://data.funds.hexun.com/outxml/detail/openfundnetvalue.aspx?" \
      "fundcode=040035&startdate=2008-10-31&enddate=2018-01-31"

dcap = dict(DesiredCapabilities.PHANTOMJS)
dcap["phantomjs.page.settings.userAgent"] = (
    "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:50.0) Gecko/20100101 Firefox/50.0"
)
dcap["phantomjs.page.settings.resourceTimeout"] = 1000
dcap["phantomjs.page.settings.loadImages"] = False  # 不加载图片，加快速度
dcap["phantomjs.page.settings.disk-cache"] = True  # 启用缓存
dcap["phantomjs.page.settings.userAgent"] = "faking it"
dcap["phantomjs.page.settings.localToRemoteUrlAccessEnabled"] = False
dcap["phantomjs.page.settings.ignore-ssl-errors"] = True
# phantomjs.exe的路径G:\Anaconda3\phantomjs\bin
driver = webdriver.PhantomJS(executable_path='F:\\phantomjs-2.1.1-windows\\bin\\phantomjs.exe',
                             desired_capabilities=dcap)

# 获取网页数据
def getSoup(url):
    driver.get(url)
    content = driver.page_source  # 获取网页内容
    soup = BeautifulSoup(content, 'xml')
    return soup
    # getInfo(soup)

# 存入csv文件
def saveCsv(soup):
    # fld_enddate 日期，fld_unitnetvalue 单位净值，fld_netvalue 累计净值
    dateList = []
    unitnetvalueList = []
    netvalueList = []
    for x in soup.find_all('Data'):
        dateList.append(str(x.find_all('fld_enddate')[0].contents[0]))
        unitnetvalueList.append(float(x.find_all('fld_unitnetvalue')[0].contents[0]))
        netvalueList.append(float(x.find_all('fld_netvalue')[0].contents[0]))
    fundData = pd.DataFrame()
    fundData['dateList'] = dateList
    fundData['unitnetvalueList'] = unitnetvalueList
    fundData['netvalueList'] = netvalueList
    # print(fundData.head())
    fundData.to_csv('a.csv', mode='w')
    print('存储完毕')

if __name__ == '__main__':
    soup = getSoup(url)
    saveCsv(soup)




