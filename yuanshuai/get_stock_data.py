import tushare as ts
import time


def get_data(code, start=None, end=None, index=False):
    """
    获取原始数据
    :param code:
    :param start:
    :param end:
    :param index:
    :return:
    """
    end = time.strftime('%Y-%m-%d', time.localtime(time.time()))
    data = ts.get_h_data(code=code, start=start, end=end, index=index)
    return data, code


def get_name(code):
    """
    获取股票中文名称
    :param code:
    :return:
    """
    # TODO: 查表代替执行方法
    data = ts.get_stock_basics()
    name = data[data.index == code].name
    return name[0]


# def get_industry(code):
#     """
#     获取股票所属行业
#     :param code:
#     :return:
#     """
#     data = ts.get_stock_basics()
#     industry = data[data.index == code].industry
#     return industry[0]


def get_type(code):
    """
    获取板块信息
    :param code:
    :return:
    """
    code_pre = code[0:3]
    switcher={
        '300': "创业板",
        '600': "沪市A股",
        '601': "沪市A股",
        '602': "沪市A股",
        '900': "沪市B股",
        '000': "深市A股",
        '002': "中小板",
        '200': "深市B股",
    }
    return switcher.get(code_pre, '未知')


def get_all_company():
    df = ts.get_stock_basics()
    # print(pinyin(stockName))
    print("已获取数据")
    return df


def main():
    get_data(code='000001', start='2017-01-01')
    print(get_name('000001'))
    name = get_name('000001')
    print(name,type(name))
    # t = get_type('002001')
    # print(t)

if __name__ == '__main__':
    main()