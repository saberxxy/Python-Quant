# -*- coding:utf-8 -*-
import cx_Oracle
conn = None
# 连接数数库
def conn(user='scott', password='tiger', host='localhost', sid='orcl'):
    conn = cx_Oracle.connect(user, password, host+':1521/'+sid)  # 连接数据库
    print(conn.version)  # 打印版本号
    cursor = conn.cursor()  # 创建cursor

# 创建游标
def createursor():
    conn()
    cursor = conn.cursor()
    return cursor

def main():
    conn()

if __name__=='__main__':
    main()
