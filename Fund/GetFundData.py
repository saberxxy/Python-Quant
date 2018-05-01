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
import uuid

# 导入连接文件
import sys
sys.path.append("..")
import common.GetOracleConn as conn

# 获取全局数据库连接
cursor = conn.getConfig()

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
    fundData['sdate'] = dateList
    fundData['unitnetvalue'] = unitnetvalueList
    fundData['netvalue'] = netvalueList
    # print(fundData.head())
    fundData.to_csv(code+'.csv', mode='w')
    print('存储CSV完毕')
    return fundData

# 存入数据库
def save_db(code, fund_data):
    # 建表
    if not is_table_exist(code):  # 如果表不存在，先创建表
        create_table(code)  # 如果表不存在，先建表
    else:  # 存在则截断
        cursor.execute("TRUNCATE TABLE FUND_" + code)

    fund_data['uuid'] = [uuid.uuid1() for l in range(0, len(fund_data))]  # 添加uuid
    fund_data['code'] = code
    print(fund_data.head())
    # 入库
    for k in range(0, len(fund_data)):
        df2 = fund_data[k:k + 1]
        sql = "insert into fund_" + str(code) + \
              "(uuid, sdate, code, unitnetvalue, netvalue) " \
              "values(:uuid, to_date(:sdate, 'yyyy-MM-dd'), :code, " \
              ":unitnetvalue, :netvalue)"

        cursor.execute(sql,
                       (
                           str(list(df2['uuid'])[0]),
                           str(list(df2['sdate'])[0]),
                           str(list(df2['code'])[0]),
                           round(float(df2['unitnetvalue']), 4),
                           round(float(df2['netvalue']), 4)
                       )
                      )
    cursor.execute("commit")

    print("存入数据库")

# 判断表是否已存在
def is_table_exist(code):
    table = "FUND_" + code
    sql = "SELECT TABLE_NAME FROM USER_TABLES"
    rs = cursor.execute(sql)
    result = rs.fetchall()
    tables = [i[0] for i in result]
    print(table)
    print(tables.__contains__(table))
    return tables.__contains__(table)

# 建表函数
def create_table(code):
    sql = "CREATE TABLE FUND_" + code + """
    (
            UUID VARCHAR2(80) PRIMARY KEY,
            SDATE DATE,
            CODE VARCHAR2(20),
            UNITNETVALUE NUMBER(10,2),
            NETVALUE NUMBER(10,2)
        )
    """
    cursor.execute(sql)
    # 添加注释
    comments = ["COMMENT ON TABLE FUND_" + code + " IS '" + code + "'",  # 表注释
                "COMMENT ON COLUMN FUND_" + code + ".UUID IS 'UUID'",
                "COMMENT ON COLUMN FUND_" + code + ".SDATE IS '日期'",
                "COMMENT ON COLUMN FUND_" + code + ".CODE IS '代码'",
                "COMMENT ON COLUMN FUND_" + code + ".UNITNETVALUE IS '单位净值'",
                "COMMENT ON COLUMN FUND_" + code + ".NETVALUE IS '累计净值'"]
    for i in comments:
        cursor.execute(i)


if __name__ == '__main__':
    # 基金代码
    # 110022，180012，090013，320010，002001，003188
    codes = ['110022', '180012', '090013', '320010', '002001', '003188']
    for code in codes:
    # code = '110022'
        url = "http://data.funds.hexun.com/outxml/detail/openfundnetvalue.aspx?" \
            "fundcode=" + code + "&startdate=2018-01-01&enddate=2018-03-31"
        soup = getSoup(url)
        fund_data = saveCsv(soup)
        save_db(code, fund_data)



