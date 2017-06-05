# -*- coding:utf-8 -*-
import cx_Oracle
import gp

# ['uuid',--------varchar2 not null unique?
#  'date',--------date not null
#  'code',--------varchar not null
#  'name',--------varchar not null
#  'open',--------float
#  'close',-------float
#  'high',--------float
#  'low',---------float
#  'volume']------float



# print(data)


# def connect(user=None, password=None, host=None, sid=None):
def connect():
    conn = cx_Oracle.connect("ys/123456@localhost/orcl")
    return conn

# 创建表，插入数据
def create(conn, data):
    cursor = conn.cursor()
    # cursor.execute("CREATE TABLE + "+"600848"+"(uuid VARCHAR2(50), "+
    #                "\"date\" DATE NOT NULL, "+
    #                "code VARCHAR2(6) NOT NULL, "+
    #                "name VARCHAR2(20) NOT NULL, "+
    #                "open NUMBER(*,2), "+
    #                "close NUMBER(*,2), "+
    #                "high NUMBER(*,2), "+
    #                "low NUMBER(*,2), "+
    #                "volume NUMBER(*,2), "+
    #                "CONSTRAINT PK_GP_600848 PRIMARY KEY (uuid))"
    #                )
def insert(conn, data):
    cursor=conn.cursor()

    # params=[]
    for row in data.iterrows(): # 包括dataframe的index
        # row类型为tuple:key为int，value为Series
        values=row[1]
        uuid = values['uuid']
        date = values['date']
        code = values['code']
        name = values['name']
        open = values['open']
        close = values['close']
        high = values['high']
        low = values['low']
        volume = values['volume']
        # 绑定变量不要有保留字，如date,name等
        # params={'uuid':uuid,'datex':date,'code':code,'namex':name,'openx':open,'closex':close,'high':high,'low':low,'volume':volume}
        # sql = "INSERT INTO GP_"+data.code+" VALUES(:uuid,TO_DATE(:datex,'YYYY-MM-DD'),:code,:namex,:openx,:closex,:high,:low,:volume)"
        # cursor.execute(sql, params)
        params = [{'uuid':uuid,'datex':date,'code':code,'namex':name,'openx':open,'closex':close,'high':high,'low':low,'volume':volume}]
        sql = "INSERT INTO GP_"+data.code+" VALUES(:uuid,TO_DATE(:datex,'YYYY-MM-DD'),:code,:namex,:openx,:closex,:high,:low,:volume)"
        cursor.executemany(sql, params)
        # 提交事务
        # conn.commit()
        # cursor.close()
        # conn.close()

def main():
    data = gp.getData(code='600848', start='2017-02-03', end='2017-02-08')
    data = gp.addColumns(data)
    conn = connect()
    # create(conn,data)
    insert(conn, data)


if __name__=='__main__':
    main()
