import tushare as ts
import pandas as pd
from pandas import DataFrame, Series

def main():
    df = DataFrame(ts.get_h_data('000001'))
    print(df)

if __name__=="__main__":
    main()