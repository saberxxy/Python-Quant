import pandas as pd
from pandas import DataFrame, Series
import get_stock_data as gsd


def format_data(raw_data):
    data = raw_data[0]
    code = raw_data[1]
    data['code'] = code
    data['name'] = gsd.get_name(code)
    data['industry'] = gsd.get_industry(code)
    return data


def main():
    raw_data = gsd.get_data('000001')
    data = format_data(raw_data)
    print(data)


if __name__ == '__main__':
    main()

