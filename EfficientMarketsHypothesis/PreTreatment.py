#-*- coding=utf-8 -*-
#数据预处理

import configparser
import pymysql
import re
import pandas as pd
import sqlalchemy

#读取配置文件
cf = configparser.ConfigParser()
cf.read("../config.conf")
mysqlHost = str(cf.get("mysql", "ip"))
mysqlPort = int(cf.get("mysql", "port"))
mysqlUser = str(cf.get("mysql", "username"))
mysqlPassword = str(cf.get("mysql", "password"))
mysqlDatabaseName = str(cf.get("mysql", "databasename"))
connStr = 'mysql://root:root@127.0.0.1:3306/test?charset=utf8'

#建立数据库连接
conn = pymysql.connect(host=mysqlHost, user=mysqlUser, passwd=mysqlPassword,
                        db=mysqlDatabaseName, port=mysqlPort, charset="utf8")
cur1 = conn.cursor()


def preTreatment():
    riskFree = 0.0096  # 无风险收益率，此处为2016年定期利率，日收益率0.0096
    cur1.execute("show tables")
    sharesList = []
    for i in cur1.fetchall():
        sharesList.append(i[0])
    for j in sharesList:
        pattern = re.match(r'^\d+_.*?_org$', j)
        if pattern != None:
            # print(j)
            if j == "000001_上证指数_org":
                # 获取时间
                cur1.execute("select b_date from %s" % (j))
                date = []
                for k in cur1.fetchall():
                    date.append(str(k[0]))

                index = [index for index in range(1, len(date) + 1)]  # 列表推倒式
                rateOfReturnDvalue = pd.DataFrame(index=index)  # 用于回归的DataFrame，减去无风险收益率的收益率
                rateOfReturnDvalue['date'] = list(date)

                # 获取指数收益率
                cur1.execute("select b_change from %s" % (j))
                number = "org_" + j.split("_")[0]
                locals()[number] = []
                for k in cur1.fetchall():
                    if float(k[0]) - riskFree == 0.00:
                        dValue = 0
                    else:
                        dValue = float(k[0]) - riskFree
                    locals()[number].append(dValue)
                rateOfReturnDvalue[number] = locals()[number]
                # print (locals()[number])

            else:
                # 获取各股收益率
                cur1.execute("select b_change from %s" % (j))
                number = "org_" + j.split("_")[0]
                locals()[number] = []
                for k in cur1.fetchall():
                    if float(k[0]) - riskFree == 0.00:
                        dValue = 0
                    else:
                        dValue = float(k[0]) - riskFree
                    locals()[number].append(dValue)
                rateOfReturnDvalue[number] = locals()[number]
                # print (locals()[number])

    print(rateOfReturnDvalue)
    tableName = 'rate_d_value'
    engine = sqlalchemy.create_engine(connStr)
    try:
        rateOfReturnDvalue.to_sql(tableName, engine, dtype=None)
    except Exception:
        pass

    print ("数据预处理完毕！")

def main():
    preTreatment()

if __name__ == '__main__':
    main()





