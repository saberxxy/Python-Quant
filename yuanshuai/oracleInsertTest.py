import cx_Oracle
# import ConfigParser
import sys
import get_stock_data as gsd
import format_stock_data as fsd

sys.path.append('..')

conn = cx_Oracle.connect('stock/123456@localhost:1521/orcl')
# cp = ConfigParser.SafeConfigParser()
# cp.read('db.conf')


# 插入数据
def insert_data(stock_code, stock_data):
    cursor = conn.cursor()
    rows = []
    for i in stock_data.index:
        uuid = stock_data.loc[i, 'uuid']
        date = stock_data.loc[i, 'date']
        code = stock_data.loc[i, 'code']
        name = stock_data.loc[i, 'name']
        classify = stock_data.loc[i, 'classify']
        open = stock_data.loc[i, 'open']
        close = stock_data.loc[i, 'close']
        high = stock_data.loc[i, 'high']
        low = stock_data.loc[i, 'low']
        volume = stock_data.loc[i, 'volume']
        amount = stock_data.loc[i, 'amount']
        y_close = stock_data.loc[i, 'y_close']
        p_change = stock_data.loc[i, 'p_change']
        p_change_rate = stock_data.loc[i, 'p_change_rate']
        row = (uuid, date, code, name, classify, open, close, high, low, volume, amount, y_close, p_change,
               p_change_rate)
        rows.append(row)
        print(close, type(close))
    # print(rows, type(rows))

    sql = "insert into STOCK_" + stock_code + "(uuid, datetime, code, c_name, classify, open, close, high, low, " \
                                              "volume, amount, y_close, p_change, p_change_rate)values(:uuid, " \
                                              "to_date(:datex, 'yyyy-mm-dd'), :code, :namex, :classify, :openx, " \
                                              ":closex, :high, :low, :volume, :amount, :y_close, :p_change, " \
                                              ":p_change_rate) "


    print(sql)
    # cursor.prepare(sql)
    cursor.executemany(sql, rows)
    conn.commit()


def main():
    data1 = gsd.get_data('000001')
    data2 = fsd.format_data(data1)
    print(data2)
    insert_data('000001', data2)

if __name__ == '__main__':
    main()